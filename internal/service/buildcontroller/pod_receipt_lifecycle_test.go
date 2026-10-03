package buildcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestPrepareGenesisPodDefaultsCopiesMissingRequestsFromLimits(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "runner", Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("3Gi"),
			},
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		}}},
		InitContainers: []corev1.Container{{Name: "source-fetch", Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")},
		}}},
	}}
	prepareGenesisPodDefaults(pod)
	runnerResources := pod.Spec.Containers[0].Resources
	if got, want := runnerResources.Requests[corev1.ResourceCPU], resource.MustParse("100m"); got.Cmp(want) != 0 {
		t.Fatalf("explicit CPU request overwritten: %s", got.String())
	}
	if got, want := runnerResources.Requests[corev1.ResourceMemory], runnerResources.Limits[corev1.ResourceMemory]; got.Cmp(want) != 0 {
		t.Fatalf("runner memory request was not defaulted from its limit: %s", got.String())
	}
	initResources := pod.Spec.InitContainers[0].Resources
	if got, want := initResources.Requests[corev1.ResourceEphemeralStorage], initResources.Limits[corev1.ResourceEphemeralStorage]; got.Cmp(want) != 0 {
		t.Fatalf("init ephemeral-storage request was not defaulted from its limit: %s", got.String())
	}
}

func receiptedGenesisPodTemplate(t *testing.T, r *KovaBuildReconciler, build *kovav1.KovaBuild) *corev1.Pod {
	t.Helper()
	pod := runner.PreparePod(runner.ManifestOptions{
		PodName: buildPodName(build.Name), Namespace: build.Namespace,
		Image: r.Cfg.RunnerImage, ImagePullPolicy: "IfNotPresent",
		SourceURI: build.Spec.Source.URI, SourceDigest: build.Spec.Source.Digest,
		RegistryPlainHTTP: r.Cfg.RegistryPlainHTTP,
		Labels:            map[string]string{"kova.cofy.dev/build-id": build.Name},
	})
	prepareGenesisPodDefaults(&pod)
	if err := controllerutil.SetControllerReference(build, &pod, r.Scheme); err != nil {
		t.Fatal(err)
	}
	return &pod
}

func terminalGenesisBuild(t *testing.T, api *controllerGenesisAPI, build *kovav1.KovaBuild) *kovav1.KovaBuild {
	t.Helper()
	ctx := context.Background()
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = kovav1.PhaseFailed
	if err := api.Status().Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	return &current
}

func TestGenesisPodReceiptBeforeSoleCreateAndOrderedCleanup(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	pod := receiptedGenesisPodTemplate(t, r, build)
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	if counted.creates != 1 {
		t.Fatalf("Pod receipt Create count=%d, want one", counted.creates)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	entry := state.Active[reservationKey(build)]
	if entry.PodAttemptNonce != attempt || entry.PodTemplateDigest != pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] ||
		entry.PodReceiptUID == "" || entry.PodReceiptDigest == "" || len(entry.InFlight) != 1 || entry.InFlight[0] != attempt {
		t.Fatalf("Pod Create was issued before durable receipt and in-flight fence: %+v", entry)
	}
	if _, err := r.beginPodCreate(ctx, build); err == nil || counted.creates != 1 {
		t.Fatalf("spent nonce was replayed: err=%v creates=%d", err, counted.creates)
	}
	pod.UID = "original-pod-uid"
	if err := api.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := r.qualifyGenesisPod(ctx, build, pod, false); err != nil {
		t.Fatalf("direct Pod readback did not match its pre-Create receipt: %v", err)
	}
	if err := r.completePodCreate(ctx, build, attempt); err != nil {
		t.Fatal(err)
	}
	_, state, err = r.readReservations(ctx, build.Namespace)
	if err != nil || state.Active[reservationKey(build)].PodAttemptNonce != attempt || len(state.Active[reservationKey(build)].InFlight) != 0 {
		t.Fatalf("completion forgot permanent spent nonce or retained resolved issue: entry=%+v err=%v", state.Active[reservationKey(build)], err)
	}
	intent, err := r.podIntent(ctx, build, state.Active[reservationKey(build)])
	if err != nil {
		t.Fatal(err)
	}
	podReceipt, err := recoveryreceipt.NewPodCreateConfigMap(intent)
	if err != nil {
		t.Fatal(err)
	}
	terminal := terminalGenesisBuild(t, api, build)
	if err := r.fenceReservation(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, terminal); err != nil {
		t.Fatalf("terminal cleanup did not settle Pod then grant receipt: %v", err)
	}
	_, state, err = r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("active charge survived settled receipts: active=%+v err=%v", state.Active, err)
	}
	if _, err := r.RecoveryReceipts.Get(ctx, podReceipt.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod receipt remained after terminal cleanup: %v", err)
	}
}

func TestGenesisLostPodArmResponseNeverCreatesReceiptOrPod(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	r.Genesis.Bootstrap.API = &loseActivePatchResponse{CoreAPI: api, at: 2}
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	pod := receiptedGenesisPodTemplate(t, r, build)
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); !errors.Is(err, context.DeadlineExceeded) || counted.creates != 0 {
		t.Fatalf("lost arm response authorized receipt Create: err=%v creates=%d", err, counted.creates)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	entry := state.Active[reservationKey(build)]
	if entry.PodAttemptNonce != attempt || entry.PodTemplateDigest == "" || entry.PodReceiptUID != "" || len(entry.InFlight) != 0 {
		t.Fatalf("lost arm response did not retain observable spent state: %+v", entry)
	}
	r.Genesis.Bootstrap.API = api
	if _, err := r.beginPodCreate(ctx, build); err == nil || counted.creates != 0 {
		t.Fatalf("restart replayed spent Pod attempt: err=%v creates=%d", err, counted.creates)
	}
	terminal := terminalGenesisBuild(t, api, build)
	if err := r.fenceReservation(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, terminal); err == nil {
		t.Fatal("armed missing receipt was treated as no-effect evidence")
	}
}

func TestGenesisPodReceiptLostCreateResponseUsesExactReadback(t *testing.T) {
	ctx := context.Background()
	r, _, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts, writeThenLose: true}
	r.RecoveryReceipts = counted
	pod := receiptedGenesisPodTemplate(t, r, build)
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); err != nil || counted.creates != 1 {
		t.Fatalf("committed receipt with lost response was not read back once: err=%v creates=%d", err, counted.creates)
	}
}

type pauseNthActivePatch struct {
	admissiongenesis.CoreAPI
	mu      sync.Mutex
	at      int
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (a *pauseNthActivePatch) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.ActiveLedgerName {
		a.mu.Lock()
		a.calls++
		paused := a.calls == a.at
		a.mu.Unlock()
		if paused {
			close(a.entered)
			select {
			case <-a.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return a.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
}

func TestGenesisPausedPodIssueCannotOutrunTerminalCleanup(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	pod := receiptedGenesisPodTemplate(t, r, build)
	attempt, err := r.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
		t.Fatal(err)
	}
	digest, err := recoveryreceipt.CanonicalPodTemplateDigest(pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[recoveryreceipt.PodTemplateDigestAnnotation] = digest
	armed, err := r.armFreshPodAttempt(ctx, build, attempt, digest)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := r.podIntent(ctx, build, armed)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := recoveryreceipt.RecordPodCreateOnce(ctx, r.RecoveryReceipts, intent)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := r.pinPodReceipt(ctx, build, armed, witness)
	if err != nil {
		t.Fatal(err)
	}
	paused := &pauseNthActivePatch{CoreAPI: api, at: 1, entered: make(chan struct{}), release: make(chan struct{})}
	r.Genesis.Bootstrap.API = paused
	result := make(chan error, 1)
	go func() { result <- r.authorizeFreshPodCreate(ctx, build, pinned) }()
	select {
	case <-paused.entered:
	case err := <-result:
		t.Fatalf("issue CAS exited before pause: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Pod issue CAS did not pause")
	}
	terminal := terminalGenesisBuild(t, api, build)
	if err := r.fenceReservation(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, terminal); err != nil {
		t.Fatalf("unissued pinned attempt did not clean up: %v", err)
	}
	close(paused.release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("paused issue CAS obtained Pod Create authority after cleanup")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paused issue CAS did not finish")
	}
}

type createThenRejectReceipt struct {
	ReceiptConfigMaps
	creates int
}

func (a *createThenRejectReceipt) Create(ctx context.Context, cm *corev1.ConfigMap, opts metav1.CreateOptions) (*corev1.ConfigMap, error) {
	a.creates++
	if _, err := a.ReceiptConfigMaps.Create(ctx, cm, opts); err != nil {
		return nil, err
	}
	// A matching direct GET following a typed AlreadyExists rejection is
	// observation only. It cannot confer this caller's fresh Create right.
	return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, cm.Name)
}

func TestGenesisPodReceiptUnknownOrAlreadyExistsNeverIssuesPodCreate(t *testing.T) {
	for _, mode := range []string{"lost-no-write", "matching-already-exists"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, _, build := genesisAdmissionFixture(t)
			if _, err := r.admission(ctx, build); err != nil {
				t.Fatal(err)
			}
			pod := receiptedGenesisPodTemplate(t, r, build)
			attempt, err := r.beginPodCreate(ctx, build)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
				t.Fatal(err)
			}
			var creates func() int
			switch mode {
			case "lost-no-write":
				api := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts, lose: true}
				r.RecoveryReceipts = api
				creates = func() int { return api.creates }
			case "matching-already-exists":
				api := &createThenRejectReceipt{ReceiptConfigMaps: r.RecoveryReceipts}
				r.RecoveryReceipts = api
				creates = func() int { return api.creates }
			}
			if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); !errors.Is(err, recoveryreceipt.ErrUnconfirmed) {
				t.Fatalf("uncertain/typed rejected Pod receipt armed Create: %v", err)
			}
			_, state, err := r.readReservations(ctx, build.Namespace)
			if err != nil {
				t.Fatal(err)
			}
			entry := state.Active[reservationKey(build)]
			if entry.PodAttemptNonce != attempt || entry.PodTemplateDigest == "" || entry.PodReceiptUID != "" || len(entry.InFlight) != 0 {
				t.Fatalf("uncertain Pod receipt issued Create or lost liability: %+v", entry)
			}
			if _, err := r.beginPodCreate(ctx, build); err == nil || creates() != 1 {
				t.Fatalf("restart repeated spent receipt/Pod attempt: err=%v creates=%d", err, creates())
			}
		})
	}
}

func TestGenesisPodDirectReadRejectsTamperedTemplateOrReceiptReplacement(t *testing.T) {
	for _, mode := range []string{"template", "receipt-uid"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			if _, err := r.admission(ctx, build); err != nil {
				t.Fatal(err)
			}
			pod := receiptedGenesisPodTemplate(t, r, build)
			attempt, err := r.beginPodCreate(ctx, build)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.stampGenesisPod(ctx, build, pod, attempt); err != nil {
				t.Fatal(err)
			}
			if err := r.finishFreshPodAttempt(ctx, build, pod, attempt); err != nil {
				t.Fatal(err)
			}
			pod.UID = "original-pod-uid"
			if err := api.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}
			if _, err := r.pinnedGenesisPodForBuild(ctx, build, false); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "template":
				var changed corev1.Pod
				if err := api.Get(ctx, client.ObjectKeyFromObject(pod), &changed); err != nil {
					t.Fatal(err)
				}
				changed.Spec.Containers[0].Env = append(changed.Spec.Containers[0].Env, corev1.EnvVar{Name: "INJECTED", Value: "yes"})
				// The digest annotation is left untouched on purpose.
				if err := api.Update(ctx, &changed); err != nil {
					t.Fatal(err)
				}
			case "receipt-uid":
				_, state, err := r.readReservations(ctx, build.Namespace)
				if err != nil {
					t.Fatal(err)
				}
				intent, err := r.podIntent(ctx, build, state.Active[reservationKey(build)])
				if err != nil {
					t.Fatal(err)
				}
				old, err := r.RecoveryReceipts.Get(ctx, "kova-pod-create-intent-"+attempt, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := api.Delete(ctx, old); err != nil {
					t.Fatal(err)
				}
				replacement, err := recoveryreceipt.NewPodCreateConfigMap(intent)
				if err != nil {
					t.Fatal(err)
				}
				replacement.UID = "replacement-receipt-uid"
				if err := api.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.pinnedGenesisPodForBuild(ctx, build, false); err == nil {
				t.Fatal("mutable Pod or replacement receipt qualified as original")
			}
		})
	}
}
