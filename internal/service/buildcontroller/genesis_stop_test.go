package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func genesisRunningStopFixture(t *testing.T) (*KovaBuildReconciler, *controllerGenesisAPI, *kovav1.KovaBuild) {
	t.Helper()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(context.Background(), build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	return r, api, persistGenesisRunningCancellation(t, r, api, build)
}

func genesisRunningStatusKube(t *testing.T, api *controllerGenesisAPI, build *kovav1.KovaBuild) (*fakeKube, *int) {
	t.Helper()
	gets := new(int)
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			t.Fatal("Genesis forced stop sent a runner POST")
		}
		*gets++
		_, _ = fmt.Fprintf(opts.Stdout, `{"status":"running","requestId":%q}`, string(build.UID))
		return nil
	}}
	return kubeClient, gets
}

func requireGenesisActiveCharge(t *testing.T, r *KovaBuildReconciler, build *kovav1.KovaBuild, closing bool) {
	t.Helper()
	_, state, err := r.readReservations(context.Background(), build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	entry, found := state.Active[reservationKey(build)]
	if !found || entry.BuildName != build.Name || entry.Closing != closing {
		t.Fatalf("active charge = %+v, found=%t, want closing=%t", entry, found, closing)
	}
}

func TestGenesisHealthyRunningCancellationPersistsIntentBeforeUIDStop(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient, gets := genesisRunningStatusKube(t, api, build)
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatal(err)
	}
	var stopped kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &stopped); err != nil {
		t.Fatal(err)
	}
	if stopped.Status.Phase != kovav1.PhaseCancelled || !validGenesisStopIntent(&stopped) ||
		stopped.Status.AdmissionGenesisStopIntent.Reason != "Cancelled" || *gets != 1 ||
		len(kubeClient.deletedUIDs) != 1 || kubeClient.deletedUIDs[0] != types.UID(stopped.Status.AdmissionGenesisWitness.PodUID) {
		t.Fatalf("forced stop lost durable intent, outcome, or UID: phase=%s intent=%+v gets=%d deleted=%v",
			stopped.Status.Phase, stopped.Status.AdmissionGenesisStopIntent, *gets, kubeClient.deletedUIDs)
	}
	requireGenesisActiveCharge(t, r, build, true)
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("terminal cleanup did not converge: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("terminal cleanup retained released active charge: active=%+v err=%v", state.Active, err)
	}
}

func TestGenesisHealthyStartingCancellationStopsExactIdlePod(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	current, _ := persistGenesisStartingWitness(t, r, api, build)
	current.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "true"}
	if err := api.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	gets := 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			t.Fatal("Starting cancellation sent a runner POST")
		}
		gets++
		_, _ = fmt.Fprint(opts.Stdout, `{"status":"idle"}`)
		return nil
	}}
	r.Kube = kubeClient
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err != nil {
		t.Fatalf("exact idle Starting runner could not be cancelled: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseCancelled || !validGenesisStopIntent(current) ||
		gets != 1 || len(kubeClient.deletedUIDs) != 1 {
		t.Fatalf("Starting cancellation lost exact stop: phase=%s intent=%+v gets=%d deleted=%v",
			current.Status.Phase, current.Status.AdmissionGenesisStopIntent, gets, kubeClient.deletedUIDs)
	}
}

func TestGenesisCancellationRefusesPhaseAdvanceDuringRunnerObservation(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			t.Fatal("phase-advanced cancellation sent a runner POST")
		}
		var current kovav1.KovaBuild
		if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
			return err
		}
		current.Status.Phase = kovav1.PhaseVerifying
		if err := api.Status().Update(ctx, &current); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(opts.Stdout, `{"status":"running","requestId":%q}`, string(build.UID))
		return nil
	}}
	r.Kube = kubeClient
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
		t.Fatal("phase advance after GET was treated as permission to cancel")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseVerifying || current.Status.AdmissionGenesisStopIntent != nil ||
		len(kubeClient.deletedUIDs) != 0 {
		t.Fatalf("phase advance lost completed-result path: phase=%s intent=%+v deleted=%v",
			current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
	}
	requireGenesisActiveCharge(t, r, build, false)
}

type genesisIntentFaultClient struct {
	client.Client
	failOnce bool
	persist  bool
}

func (c *genesisIntentFaultClient) Status() client.SubResourceWriter {
	return &genesisIntentFaultWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type genesisIntentFaultWriter struct {
	client.SubResourceWriter
	parent *genesisIntentFaultClient
}

func (w *genesisIntentFaultWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.AdmissionGenesisStopIntent != nil &&
		!isTerminalPhase(build.Status.Phase) && w.parent.failOnce {
		w.parent.failOnce = false
		if w.parent.persist {
			if err := w.SubResourceWriter.Update(ctx, obj, opts...); err != nil {
				return err
			}
		}
		return errors.New("injected unknown stop-intent status response")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestGenesisStopIntentUnknownWriteNeverDeletesWithoutReadback(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprintf("persisted=%t", persisted), func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisRunningStopFixture(t)
			kubeClient, gets := genesisRunningStatusKube(t, api, build)
			r.Kube = kubeClient
			r.Client = &genesisIntentFaultClient{Client: api.Client, failOnce: true, persist: persisted}
			key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
			if _, err := r.Reconcile(ctx, key); err == nil {
				t.Fatal("unknown stop-intent status write was treated as stop authority")
			}
			if len(kubeClient.deletedUIDs) != 0 {
				t.Fatalf("Pod was deleted after unknown intent write: %v", kubeClient.deletedUIDs)
			}
			var current kovav1.KovaBuild
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			if (current.Status.AdmissionGenesisStopIntent != nil) != persisted || current.Status.Phase != kovav1.PhaseRunning {
				t.Fatalf("intent persistence mismatch: phase=%s intent=%+v", current.Status.Phase, current.Status.AdmissionGenesisStopIntent)
			}
			if _, err := r.Reconcile(ctx, key); err != nil {
				t.Fatalf("restart did not recover bounded stop: %v", err)
			}
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			wantGets := 2
			if persisted {
				wantGets = 1
			}
			if current.Status.Phase != kovav1.PhaseCancelled || len(kubeClient.deletedUIDs) != 1 || *gets != wantGets {
				t.Fatalf("restart outcome phase=%s deletes=%v GETs=%d, want %d", current.Status.Phase, kubeClient.deletedUIDs, *gets, wantGets)
			}
		})
	}
}

type genesisLostDeleteKube struct {
	*fakeKube
	loseOnce bool
}

type genesisAfterDeleteReplacementKube struct {
	*fakeKube
	store          client.Client
	replacementUID types.UID
}

func (k *genesisAfterDeleteReplacementKube) DeletePodWithUID(ctx context.Context, namespace, name string, uid types.UID) error {
	var original corev1.Pod
	if err := k.store.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &original); err != nil {
		return err
	}
	if err := k.fakeKube.DeletePodWithUID(ctx, namespace, name, uid); err != nil {
		return err
	}
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = k.replacementUID
	return k.store.Create(ctx, replacement)
}

func (k *genesisLostDeleteKube) DeletePodWithUID(ctx context.Context, namespace, name string, uid types.UID) error {
	if err := k.fakeKube.DeletePodWithUID(ctx, namespace, name, uid); err != nil {
		return err
	}
	if k.loseOnce {
		k.loseOnce = false
		return errors.New("injected lost UID delete response")
	}
	return nil
}

func TestGenesisStopRecoversAfterUIDDeleteResponseLost(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient, gets := genesisRunningStatusKube(t, api, build)
	r.Kube = &genesisLostDeleteKube{fakeKube: kubeClient, loseOnce: true}
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("lost UID delete response was silently accepted")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) || len(kubeClient.deletedUIDs) != 1 {
		t.Fatalf("lost delete did not retain recoverable intent: phase=%s intent=%+v deleted=%v",
			current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("direct absence did not recover lost delete: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseCancelled || *gets != 1 || len(kubeClient.deletedUIDs) != 1 {
		t.Fatalf("retry lost exact stop: phase=%s GETs=%d deletes=%v", current.Status.Phase, *gets, kubeClient.deletedUIDs)
	}
}

type genesisTerminalFaultClient struct {
	client.Client
	failOnce bool
}

func (c *genesisTerminalFaultClient) Status() client.SubResourceWriter {
	return &genesisTerminalFaultWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type genesisTerminalFaultWriter struct {
	client.SubResourceWriter
	parent *genesisTerminalFaultClient
}

func (w *genesisTerminalFaultWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseCancelled &&
		build.Status.AdmissionGenesisStopIntent != nil && w.parent.failOnce {
		w.parent.failOnce = false
		return errors.New("injected lost terminal status write")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestGenesisStopRecoversLostTerminalWriteOnlyWhileOriginalPairHealthy(t *testing.T) {
	for _, pairLoss := range []bool{false, true} {
		t.Run(fmt.Sprintf("pairLoss=%t", pairLoss), func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisRunningStopFixture(t)
			kubeClient, gets := genesisRunningStatusKube(t, api, build)
			r.Kube = kubeClient
			r.Client = &genesisTerminalFaultClient{Client: api.Client, failOnce: true}
			key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
			if _, err := r.Reconcile(ctx, key); err == nil {
				t.Fatal("lost terminal write was treated as completion")
			}
			var current kovav1.KovaBuild
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) || len(kubeClient.deletedUIDs) != 1 {
				t.Fatalf("lost terminal write lacked recoverable evidence: phase=%s intent=%+v deleted=%v",
					current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
			}
			if pairLoss {
				var queue corev1.ConfigMap
				if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
					t.Fatal(err)
				}
				if err := api.Delete(ctx, &queue); err != nil {
					t.Fatal(err)
				}
			}
			_, err := r.Reconcile(ctx, key)
			if pairLoss != (err != nil) {
				t.Fatalf("retry error=%v, pairLoss=%t", err, pairLoss)
			}
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			wantPhase := kovav1.PhaseCancelled
			if pairLoss {
				wantPhase = kovav1.PhaseRunning
			}
			if current.Status.Phase != wantPhase || *gets != 1 || len(kubeClient.deletedUIDs) != 1 {
				t.Fatalf("retry changed stopped evidence: phase=%s want=%s GETs=%d deletes=%v",
					current.Status.Phase, wantPhase, *gets, kubeClient.deletedUIDs)
			}
		})
	}
}

func TestGenesisStopRefusesSameOwnerPodReplacement(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient, _ := genesisRunningStatusKube(t, api, build)
	replacementUID := types.UID("replacement-runner")
	kubeClient.beforeUIDDelete = replaceRunnerBeforeUIDDelete(t, api.Client, replacementUID)
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); !apierrors.IsConflict(err) {
		t.Fatalf("replacement stop error=%v, want UID conflict", err)
	}
	assertReplacementRunner(t, api.Client, build, replacementUID)
	if len(kubeClient.deletedUIDs) != 0 {
		t.Fatalf("replacement UID was deleted: %v", kubeClient.deletedUIDs)
	}
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("replacement Pod passed durable witness on retry")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) {
		t.Fatalf("replacement erased original accepted evidence: phase=%s intent=%+v", current.Status.Phase, current.Status.AdmissionGenesisStopIntent)
	}
	requireGenesisActiveCharge(t, r, build, true)
}

func TestGenesisStopRefusesSameOwnerReplacementAfterUIDDelete(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient, _ := genesisRunningStatusKube(t, api, build)
	replacementUID := types.UID("replacement-after-delete")
	r.Kube = &genesisAfterDeleteReplacementKube{fakeKube: kubeClient, store: api.Client, replacementUID: replacementUID}
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); !apierrors.IsConflict(err) {
		t.Fatalf("post-delete replacement error=%v, want conflict", err)
	}
	assertReplacementRunner(t, api.Client, build, replacementUID)
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("post-delete replacement passed original Pod witness")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) {
		t.Fatalf("post-delete replacement fabricated stop: phase=%s intent=%+v", current.Status.Phase, current.Status.AdmissionGenesisStopIntent)
	}
	requireGenesisActiveCharge(t, r, build, true)
}

func TestGenesisStopRetriesOriginalTerminatingPodWithoutNewRunnerWork(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	var pod corev1.Pod
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = []string{"test.example/hold"}
	if err := api.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	kubeClient, gets := genesisRunningStatusKube(t, api, build)
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("Terminating Pod was treated as absent before UID deletion completed")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) {
		t.Fatalf("Terminating retry lost intent: phase=%s intent=%+v", current.Status.Phase, current.Status.AdmissionGenesisStopIntent)
	}
	if _, _, err := r.directGenesisWitness(ctx, &current); err == nil {
		t.Fatal("Terminating Pod granted normal runner authority")
	}
	if _, _, err := r.directGenesisCleanupWitness(ctx, &current); err != nil {
		t.Fatalf("original Terminating Pod lost cleanup witness: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := api.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("original Terminating Pod could not complete stop: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseCancelled || *gets != 1 {
		t.Fatalf("Terminating retry reran work or missed stop: phase=%s GETs=%d", current.Status.Phase, *gets)
	}
}

func TestGenesisStopIntentCannotReleaseAfterPairLoss(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	kubeClient, gets := genesisRunningStatusKube(t, api, build)
	kubeClient.deleteErr = errors.New("injected UID delete uncertainty")
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("uncertain deletion was treated as completed")
	}
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	kubeClient.deleteErr = nil
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("pair loss permitted intent completion or ledger release")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || !validGenesisStopIntent(&current) ||
		len(current.Finalizers) == 0 || len(kubeClient.deletedUIDs) != 0 || *gets != 1 {
		t.Fatalf("pair loss changed stop evidence: phase=%s intent=%+v finalizers=%v deleted=%v GETs=%d",
			current.Status.Phase, current.Status.AdmissionGenesisStopIntent, current.Finalizers, kubeClient.deletedUIDs, *gets)
	}
}

func TestGenesisDeletingCRUsesDurableUIDStopBeforeFinalizerRelease(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	delete(current.Annotations, kovav1.CancellationRequestedAnnotation)
	if err := api.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &current); err != nil {
		t.Fatal(err)
	}
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(kube.ExecOptions) error {
		t.Fatal("explicit CR deletion contacted the runner")
		return nil
	}}
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("deleting CR could not durably stop its original Pod: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.DeletionTimestamp == nil || current.Status.Phase != kovav1.PhaseCancelled ||
		!validGenesisStopIntent(&current) || current.Status.AdmissionGenesisStopIntent.Reason != "Deleted" ||
		len(kubeClient.deletedUIDs) != 1 || kubeClient.deletedUIDs[0] != types.UID(current.Status.AdmissionGenesisWitness.PodUID) {
		t.Fatalf("deletion lost exact stop evidence: deleting=%t phase=%s intent=%+v deleted=%v",
			current.DeletionTimestamp != nil, current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("deletion finalizer did not finish under healthy original pair: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("deletion released finalizer without active cleanup: active=%+v err=%v", state.Active, err)
	}
}

func TestGenesisExpiryNeedsNonterminalRunnerObservation(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprintf("uncertain=%t", uncertain), func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisRunningStopFixture(t)
			var current kovav1.KovaBuild
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			delete(current.Annotations, kovav1.CancellationRequestedAnnotation)
			if err := api.Update(ctx, &current); err != nil {
				t.Fatal(err)
			}
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
			current.Status.StartedAt = &started
			if err := api.Status().Update(ctx, &current); err != nil {
				t.Fatal(err)
			}
			r.Cfg.MaxBuildDuration = time.Minute
			kubeClient, gets := genesisRunningStatusKube(t, api, build)
			if uncertain {
				kubeClient.execFn = func(kube.ExecOptions) error { *gets++; return errors.New("status unavailable") }
			}
			r.Kube = kubeClient
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)})
			if uncertain != (err != nil) {
				t.Fatalf("expiry error=%v, uncertain=%t", err, uncertain)
			}
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			if uncertain {
				if current.Status.Phase != kovav1.PhaseRunning || current.Status.AdmissionGenesisStopIntent != nil ||
					len(kubeClient.deletedUIDs) != 0 {
					t.Fatalf("uncertain expiry stopped an accepted runner: phase=%s intent=%+v deleted=%v",
						current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
				}
				requireGenesisActiveCharge(t, r, build, false)
			} else if current.Status.Phase != kovav1.PhaseFailed || current.Status.Reason != "BuildTimedOut" ||
				!validGenesisStopIntent(&current) || len(kubeClient.deletedUIDs) != 1 {
				t.Fatalf("witnessed expiry lacked exact stop: phase=%s reason=%s intent=%+v deleted=%v",
					current.Status.Phase, current.Status.Reason, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
			}
			if *gets != 1 {
				t.Fatalf("expiry observed status %d times, want one", *gets)
			}
		})
	}
}

func TestGenesisRunningIdleCannotAuthorizeCancellationOrExpiryStop(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(fmt.Sprintf("expiry=%t", expiry), func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisRunningStopFixture(t)
			if expiry {
				var current kovav1.KovaBuild
				if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
					t.Fatal(err)
				}
				delete(current.Annotations, kovav1.CancellationRequestedAnnotation)
				if err := api.Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
				if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
					t.Fatal(err)
				}
				started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
				current.Status.StartedAt = &started
				if err := api.Status().Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
				r.Cfg.MaxBuildDuration = time.Minute
			}
			kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
				if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
					t.Fatal("inconsistent idle Running state sent a runner POST")
				}
				_, _ = fmt.Fprint(opts.Stdout, `{"status":"idle"}`)
				return nil
			}}
			r.Kube = kubeClient
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
				t.Fatal("idle Running runner was treated as a known nonterminal result")
			}
			var current kovav1.KovaBuild
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase != kovav1.PhaseRunning || current.Status.AdmissionGenesisStopIntent != nil ||
				len(kubeClient.deletedUIDs) != 0 {
				t.Fatalf("idle Running state erased accepted evidence: phase=%s intent=%+v deleted=%v",
					current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
			}
			requireGenesisActiveCharge(t, r, build, false)
		})
	}
}

func TestGenesisPollWindowCannotStopOnUnknownRunnerStatus(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisRunningStopFixture(t)
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	delete(current.Annotations, kovav1.CancellationRequestedAnnotation)
	if err := api.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	since := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	current.Status.PollFailureSince = &since
	if err := api.Status().Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	r.Cfg.PollRetryWindow = time.Minute
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(kube.ExecOptions) error { return errors.New("status unavailable") }}
	r.Kube = kubeClient
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
		t.Fatal("poll window treated unknown status as stop permission")
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || current.Status.AdmissionGenesisStopIntent != nil ||
		len(kubeClient.deletedUIDs) != 0 {
		t.Fatalf("poll window erased accepted evidence: phase=%s intent=%+v deleted=%v",
			current.Status.Phase, current.Status.AdmissionGenesisStopIntent, kubeClient.deletedUIDs)
	}
	requireGenesisActiveCharge(t, r, build, false)
}

func TestGenesisTerminalReceiptCanCleanupDirectlyAbsentPodWithoutStopIntent(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	current, pod := persistGenesisStartingWitness(t, r, api, build)
	if err := r.finish(ctx, current, kovav1.PhaseFailed, "RunnerProtocolError", "observed failure"); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	r.Kube = &fakeKube{podClient: api.Client}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err != nil {
		t.Fatalf("durable terminal receipt could not cleanup direct Pod absence: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("terminal receipt cleanup retained active charge: active=%+v err=%v", state.Active, err)
	}
}
