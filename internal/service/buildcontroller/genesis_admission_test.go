package buildcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/daemonclient"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type controllerGenesisAPI struct {
	client.Client
	patches int
}

type failingGenesisPodReader struct{ client.Reader }

func (r failingGenesisPodReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		return fmt.Errorf("transient direct Pod read failure")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (a *controllerGenesisAPI) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var ns corev1.Namespace
	if err := a.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

func (a *controllerGenesisAPI) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := a.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func (a *controllerGenesisAPI) CreateConfigMap(ctx context.Context, _ string, cm *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	if err := a.Create(ctx, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

func (a *controllerGenesisAPI) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	a.patches++
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if err := a.Patch(ctx, cm, client.RawPatch(types.JSONPatchType, body)); err != nil {
		return nil, err
	}
	return a.GetConfigMap(ctx, namespace, name)
}

func genesisAdmissionFixture(t *testing.T) (*KovaBuildReconciler, *controllerGenesisAPI, *kovav1.KovaBuild) {
	t.Helper()
	cfg := admissionConfig()
	cfg.Namespace = "jobs"
	build := queuedBuild("direct", "alice", 1, 1)
	receipt := admissioncontract.Receipt{Namespace: cfg.Namespace, GenesisName: admissioncontract.GenesisName,
		GenesisUID: "genesis-original", Contract: admissioncontract.Contract{
			Version: 2, NamespaceUID: "namespace-original", Generation: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ReceiptNamespace: "receipts-57", ReceiptNamespaceUID: "receipts-original",
			ActiveLedgerName: admissioncontract.ActiveLedgerName, ActiveLedgerSchema: 1,
			QueueLedgerName: admissioncontract.QueueLedgerName, QueueLedgerSchema: 2,
			Limits: admissioncontract.Limits{MaxActiveJobs: cfg.MaxActiveJobs, MaxActiveJobsPerRequester: cfg.MaxActiveJobsPerRequester,
				WorkerSlots: cfg.WorkerSlots, MaxQueuedJobs: cfg.MaxQueuedJobs, MaxQueuedJobsPerRequester: cfg.MaxQueuedJobsPerRequester},
		}}
	activeData, err := GenesisEmptyAdmissionData(cfg)
	if err != nil {
		t.Fatal(err)
	}
	queueData, err := queueadmission.GenesisEmptyQueueData(cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
	if err != nil {
		t.Fatal(err)
	}
	active, err := receipt.NewLedgerObject(admissioncontract.Active, admissioncontract.ActiveLedgerDataKey, activeData,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := receipt.NewLedgerObject(admissioncontract.Queue, admissioncontract.QueueLedgerDataKey, queueData,
		"cccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	active.UID, active.ResourceVersion = "active-original", "11"
	queue.UID, queue.ResourceVersion = "queue-original", "12"
	data, err := json.Marshal(admissioncontract.GenesisData{Contract: receipt.Contract, Phase: admissioncontract.PhaseCommitted,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	genesis := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace, Name: receipt.GenesisName,
		UID: types.UID(receipt.GenesisUID), ResourceVersion: "13"}, Immutable: &immutable,
		Data: map[string]string{admissioncontract.GenesisDataKey: string(data)}}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, UID: types.UID(receipt.Contract.NamespaceUID)},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	receiptNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: receipt.Contract.ReceiptNamespace,
		UID: types.UID(receipt.Contract.ReceiptNamespaceUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}}
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(namespace, receiptNamespace, genesis, active, queue, build).Build()
	api := &controllerGenesisAPI{Client: base}
	bootstrap := admissiongenesis.Bootstrapper{API: api, Receipt: receipt,
		Active: admissiongenesis.LedgerTemplate{Role: admissioncontract.Active, DataKey: admissioncontract.ActiveLedgerDataKey,
			EmptyData: activeData, Validate: func(cm *corev1.ConfigMap) error { return ValidateAdmissionLedgerForGenesis(cm, cfg) }},
		Queue: admissiongenesis.LedgerTemplate{Role: admissioncontract.Queue, DataKey: admissioncontract.QueueLedgerDataKey,
			EmptyData: queueData, Validate: func(cm *corev1.ConfigMap) error {
				return queueadmission.ValidateQueueLedgerForGenesis(cm, cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester)
			}},
		Preflight: func(context.Context) error { return nil },
	}
	binding := admissiongenesis.Binding{NamespaceUID: receipt.Contract.NamespaceUID, GenesisUID: receipt.GenesisUID,
		ActiveLedgerUID: string(active.UID), QueueLedgerUID: string(queue.UID)}
	guard, err := admissiongenesis.NewGuard(context.Background(), bootstrap, binding)
	if err != nil {
		t.Fatal(err)
	}
	r := &KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Cfg: cfg, Genesis: guard}
	return r, api, build
}

func persistGenesisStartingWitness(t *testing.T, r *KovaBuildReconciler, api *controllerGenesisAPI, build *kovav1.KovaBuild) (*kovav1.KovaBuild, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: build.Namespace, Name: buildPodName(build.Name),
		UID: "pod-original", Labels: map[string]string{"kova.cofy.dev/build-id": build.Name}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner"}}}}
	if err := controllerutil.SetControllerReference(build, pod, r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(ctx, build, pod, "dddddddddddddddddddddddddddddddd"); err != nil {
		t.Fatal(err)
	}
	if err := api.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	var observed corev1.Pod
	if err := api.Get(ctx, client.ObjectKeyFromObject(pod), &observed); err != nil {
		t.Fatal(err)
	}
	witness, err := r.witnessFromPod(build, &observed)
	if err != nil {
		t.Fatal(err)
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = kovav1.PhaseStarting
	current.Status.RunnerPodName = observed.Name
	current.Status.AllocatedConcurrency = 1
	current.Status.AdmissionGenesisWitness = witness
	if err := api.Status().Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	return &current, &observed
}

func persistGenesisRunningCancellation(t *testing.T, r *KovaBuildReconciler, api *controllerGenesisAPI, build *kovav1.KovaBuild) *kovav1.KovaBuild {
	t.Helper()
	ctx := context.Background()
	current, _ := persistGenesisStartingWitness(t, r, api, build)
	current.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "true"}
	if err := api.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = kovav1.PhaseRunning
	if err := api.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), current); err != nil {
		t.Fatal(err)
	}
	return current
}

func TestGenesisStartingWitnessRejectsForgedDirectCRWithoutGrant(t *testing.T) {
	r, api, build := genesisAdmissionFixture(t)
	starting, _ := persistGenesisStartingWitness(t, r, api, build)
	if _, _, err := r.directGenesisWitness(context.Background(), starting); err != nil {
		t.Fatalf("test did not persist independent Pod/CR witness: %v", err)
	}
	if _, _, err := r.directGenesisGrant(context.Background(), starting); err == nil {
		t.Fatal("forged direct Starting CR obtained runner submission without an active grant")
	}
	r.Kube = &fakeKube{execFn: func(kube.ExecOptions) error {
		t.Fatal("forged direct Starting CR contacted runner")
		return nil
	}}
	if _, err := r.submitWhenReady(context.Background(), starting); err == nil {
		t.Fatal("forged direct Starting CR unexpectedly reconciled")
	}
}

func TestGenesisAcceptedBuildPreservesExactResultAfterLedgerLoss(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	build.Spec.Targets = buildTargets(ref)
	if err := api.Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	r.Cfg.RegistryPlainHTTP = []string{host}
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("pre-loss grant was not durable: decision=%+v err=%v", decision, err)
	}
	starting, _ := persistGenesisStartingWitness(t, r, api, build)
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	buildPosts, exports := 0, 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = fmt.Fprintf(opts.Stdout, `{"status":"completed","requestId":%q}`, string(build.UID))
		case strings.Contains(command, "/api/v1/export"):
			exports++
			_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", ref, digest)
		case strings.Contains(command, "--method POST"):
			buildPosts++
		default:
			_, _ = io.WriteString(opts.Stdout, "")
		}
		return nil
	}}
	r.Kube = kubeClient
	if _, err := r.submitWhenReady(ctx, starting); err != nil {
		t.Fatalf("accepted pre-loss work was not observed: %v", err)
	}
	var verifying kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &verifying); err != nil {
		t.Fatal(err)
	}
	if verifying.Status.Phase != kovav1.PhaseVerifying {
		t.Fatalf("pre-loss accepted runner was not preserved as Verifying: %s", verifying.Status.Phase)
	}
	if _, err := r.reconcileVerifying(ctx, &verifying); err != nil {
		t.Fatalf("witness-bound receipt verification failed: %v", err)
	}
	var finished kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &finished); err != nil {
		t.Fatal(err)
	}
	if finished.Status.Phase != kovav1.PhaseSucceeded || len(finished.Status.Outputs) != 1 ||
		finished.Status.Outputs[0].ManifestDigest != digest || buildPosts != 0 || exports != 1 {
		t.Fatalf("result not exact or new work escaped loss gate: phase=%s outputs=%+v buildPosts=%d exports=%d",
			finished.Status.Phase, finished.Status.Outputs, buildPosts, exports)
	}
	if _, err := r.reconcileTerminal(ctx, &finished); err == nil {
		t.Fatal("terminal cleanup released capacity after committed queue loss")
	}
	if len(kubeClient.deleted) != 0 || len(finished.Finalizers) == 0 {
		t.Fatalf("finalizer or runner Pod was removed after loss: deleted=%v finalizers=%v", kubeClient.deleted, finished.Finalizers)
	}
	var active corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &active); err != nil {
		t.Fatal(err)
	}
	state, err := decodeReservations(&active)
	if err != nil || len(state.Active) != 1 {
		t.Fatalf("accepted charge was lost: active=%+v err=%v", state.Active, err)
	}
}

func TestGenesisCancellationAfterPairLossPreservesAcceptedExactResult(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	build.Spec.Targets = buildTargets(ref)
	if err := api.Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	r.Cfg.RegistryPlainHTTP = []string{host}
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("pre-loss grant was not durable: decision=%+v err=%v", decision, err)
	}
	starting, _ := persistGenesisStartingWitness(t, r, api, build)
	starting.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "true"}
	if err := api.Update(ctx, starting); err != nil {
		t.Fatal(err)
	}
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	buildPosts, exports := 0, 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = fmt.Fprintf(opts.Stdout, `{"status":"completed","requestId":%q}`, string(build.UID))
		case strings.Contains(command, "/api/v1/export"):
			exports++
			_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", ref, digest)
		case strings.Contains(command, "--method POST"):
			buildPosts++
		default:
			_, _ = io.WriteString(opts.Stdout, "")
		}
		return nil
	}}
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("cancel intent hid a completed accepted runner: %v", err)
	}
	var verifying kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &verifying); err != nil {
		t.Fatal(err)
	}
	if verifying.Status.Phase != kovav1.PhaseVerifying || !cancellationRequested(&verifying) {
		t.Fatalf("cancel intent or verified evidence transition was lost: phase=%s annotations=%v", verifying.Status.Phase, verifying.Annotations)
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("cancel intent blocked exact receipt verification: %v", err)
	}
	var finished kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &finished); err != nil {
		t.Fatal(err)
	}
	if finished.Status.Phase != kovav1.PhaseSucceeded || len(finished.Status.Outputs) != 1 ||
		finished.Status.Outputs[0].ManifestDigest != digest || !cancellationRequested(&finished) ||
		buildPosts != 0 || exports != 1 {
		t.Fatalf("cancel/loss result not exact: phase=%s outputs=%+v cancel=%t buildPosts=%d exports=%d",
			finished.Status.Phase, finished.Status.Outputs, cancellationRequested(&finished), buildPosts, exports)
	}
	if _, err := r.Reconcile(ctx, key); err == nil {
		t.Fatal("terminal cleanup released charge after committed pair loss")
	}
	if len(kubeClient.deleted) != 0 || len(finished.Finalizers) == 0 {
		t.Fatalf("cancel/loss removed runner or finalizer: deleted=%v finalizers=%v", kubeClient.deleted, finished.Finalizers)
	}
	var active corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &active); err != nil {
		t.Fatal(err)
	}
	state, err := decodeReservations(&active)
	if err != nil || len(state.Active) != 1 {
		t.Fatalf("accepted charge was lost under cancellation: active=%+v err=%v", state.Active, err)
	}
}

func TestGenesisHealthyCancellationObservesCompletedRunnerBeforeStop(t *testing.T) {
	host, ref, digest := testVerifiedImage(t, "amd64")
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	build.Spec.Targets = buildTargets(ref)
	if err := api.Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	r.Cfg.RegistryPlainHTTP = []string{host}
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	persistGenesisRunningCancellation(t, r, api, build)
	buildPosts, cancelPosts, exports := 0, 0, 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		command := strings.Join(opts.Command, " ")
		switch {
		case strings.Contains(command, "--method GET"):
			_, _ = fmt.Fprintf(opts.Stdout, `{"status":"completed","requestId":%q}`, string(build.UID))
		case strings.Contains(command, "/api/v1/export"):
			exports++
			_, _ = fmt.Fprintf(opts.Stdout, "{\"target\":%q,\"success\":true,\"manifest_digest\":%q}\n", ref, digest)
		case strings.Contains(command, "/api/v1/build/cancel"):
			cancelPosts++
		case strings.Contains(command, "--method POST"):
			buildPosts++
		}
		return nil
	}}
	r.Kube = kubeClient
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("completed runner was stopped before observation: %v", err)
	}
	var verifying kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &verifying); err != nil {
		t.Fatal(err)
	}
	if verifying.Status.Phase != kovav1.PhaseVerifying || !cancellationRequested(&verifying) {
		t.Fatalf("completed runner was not preserved for verification: phase=%s cancel=%t", verifying.Status.Phase, cancellationRequested(&verifying))
	}
	if _, err := r.Reconcile(ctx, key); err != nil {
		t.Fatalf("completed runner receipts were lost to cancel: %v", err)
	}
	var finished kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &finished); err != nil {
		t.Fatal(err)
	}
	if finished.Status.Phase != kovav1.PhaseSucceeded || len(finished.Status.Outputs) != 1 ||
		finished.Status.Outputs[0].ManifestDigest != digest || !cancellationRequested(&finished) ||
		buildPosts != 0 || cancelPosts != 0 || exports != 1 || len(kubeClient.deleted) != 0 {
		t.Fatalf("completed runner result or stop boundary was wrong: phase=%s outputs=%+v posts=%d cancels=%d exports=%d deleted=%v",
			finished.Status.Phase, finished.Status.Outputs, buildPosts, cancelPosts, exports, kubeClient.deleted)
	}
}

func TestGenesisHealthyCancellationKeepsUncertainRunnerAndCharge(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	persistGenesisRunningCancellation(t, r, api, build)
	execCalls := 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		execCalls++
		if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			t.Fatal("uncertain runner status triggered a new POST")
		}
		return fmt.Errorf("runner status transport unavailable")
	}}
	r.Kube = kubeClient
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
		t.Fatal("uncertain status was treated as permission to stop or terminalize")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || current.Status.AdmissionGenesisWitness == nil ||
		len(kubeClient.deleted) != 0 || execCalls != 1 {
		t.Fatalf("uncertain runner changed state or was stopped: phase=%s witness=%+v deleted=%v exec=%d",
			current.Status.Phase, current.Status.AdmissionGenesisWitness, kubeClient.deleted, execCalls)
	}
	var active corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &active); err != nil {
		t.Fatal(err)
	}
	state, err := decodeReservations(&active)
	if err != nil || len(state.Active) != 1 || state.Active[reservationKey(build)].Closing {
		t.Fatalf("uncertain status released or fenced charge: state=%+v err=%v", state.Active, err)
	}
}

func TestGenesisHealthyCancellationCannotObserveForgedWitnessWithoutGrant(t *testing.T) {
	r, api, build := genesisAdmissionFixture(t)
	persistGenesisRunningCancellation(t, r, api, build)
	r.Kube = &fakeKube{podClient: api.Client, execFn: func(kube.ExecOptions) error {
		t.Fatal("forged cancellation witness contacted the runner without an active grant")
		return nil
	}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
		t.Fatal("forged cancellation witness bypassed the original active grant")
	}
}

func TestGenesisCancellationAfterPairLossObservesRunningButCannotStop(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("original grant was not durable: decision=%+v err=%v", decision, err)
	}
	persistGenesisRunningCancellation(t, r, api, build)
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	gets := 0
	kubeClient := &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
		if !strings.Contains(strings.Join(opts.Command, " "), "--method GET") {
			t.Fatal("committed pair loss triggered a new runner POST")
		}
		gets++
		_, _ = fmt.Fprintf(opts.Stdout, `{"status":"running","requestId":%q}`, string(build.UID))
		return nil
	}}
	r.Kube = kubeClient
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(build)}); err == nil {
		t.Fatal("still-running accepted job was stopped after committed pair loss")
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseRunning || current.Status.AdmissionGenesisWitness == nil ||
		len(kubeClient.deleted) != 0 || gets != 1 || len(current.Finalizers) == 0 {
		t.Fatalf("pair loss changed accepted running evidence: phase=%s witness=%+v deleted=%v gets=%d finalizers=%v",
			current.Status.Phase, current.Status.AdmissionGenesisWitness, kubeClient.deleted, gets, current.Finalizers)
	}
	var active corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &active); err != nil {
		t.Fatal(err)
	}
	state, err := decodeReservations(&active)
	if err != nil || len(state.Active) != 1 || state.Active[reservationKey(build)].Closing {
		t.Fatalf("pair loss released or fenced charge: active=%+v err=%v", state.Active, err)
	}
}

func TestGenesisStartingSubmissionRequiresDirectStatusReadback(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("failed to persist test grant: decision=%+v err=%v", decision, err)
	}
	starting, _ := persistGenesisStartingWitness(t, r, api, build)
	if _, _, err := r.directGenesisGrant(ctx, starting); err != nil {
		t.Fatalf("legitimate direct status/Pod/grant proof was refused: %v", err)
	}
	stale := starting.DeepCopy()
	stale.Status.AdmissionGenesisWitness = nil
	if _, _, err := r.directGenesisGrant(ctx, stale); err == nil {
		t.Fatal("runner submission ignored unknown Starting witness persistence")
	}
}

func TestGenesisStartingWitnessRejectsMutablePodOrSameNameReplacement(t *testing.T) {
	for _, mode := range []string{"annotation mutation", "UID fence removed", "UID fence forged", "same-name replacement"} {
		t.Run(mode, func(t *testing.T) {
			r, api, build := genesisAdmissionFixture(t)
			starting, pod := persistGenesisStartingWitness(t, r, api, build)
			ctx := context.Background()
			switch mode {
			case "annotation mutation":
				pod.Annotations[genesisActiveUIDKey] = "forged-ledger"
				if err := api.Update(ctx, pod); err != nil {
					t.Fatal(err)
				}
			case "UID fence removed", "UID fence forged":
				runner := &pod.Spec.Containers[0]
				if mode == "UID fence removed" {
					runner.Env = nil
				} else {
					runner.Env[0].ValueFrom.FieldRef.FieldPath = "metadata.name"
				}
				if err := api.Update(ctx, pod); err != nil {
					t.Fatal(err)
				}
			case "same-name replacement":
				if err := api.Delete(ctx, pod); err != nil {
					t.Fatal(err)
				}
				pod.ResourceVersion = ""
				pod.UID = "replacement-pod"
				if err := api.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := r.directGenesisWitness(ctx, starting); err == nil {
				t.Fatal("mutated or replaced Pod passed independent witness check")
			}
		})
	}
}

func TestGenesisPodStampRefusesReservedRunnerEnvCollision(t *testing.T) {
	r, _, build := genesisAdmissionFixture(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: build.Namespace, Name: buildPodName(build.Name),
		Labels: map[string]string{"kova.cofy.dev/build-id": build.Name}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Env: []corev1.EnvVar{
			{Name: daemonclient.RunnerPodUIDEnv, Value: "forged"},
		}}}}}
	if err := controllerutil.SetControllerReference(build, pod, r.Scheme); err != nil {
		t.Fatal(err)
	}
	if err := r.stampGenesisPod(context.Background(), build, pod, "dddddddddddddddddddddddddddddddd"); err == nil {
		t.Fatal("configured env override displaced the Downward API Pod UID fence")
	}
	if len(pod.Annotations) != 0 || len(pod.Spec.Containers[0].Env) != 1 {
		t.Fatal("refused Pod stamp partially wrote identity data")
	}
}

func TestGenesisFinishRetainsAcceptedWitnessOnPodUncertainty(t *testing.T) {
	for _, mode := range []string{"transient Pod read", "same-name replacement", "tampered CR witness", "missing original Pod"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
				t.Fatalf("failed to persist original active charge: decision=%+v err=%v", decision, err)
			}
			starting, pod := persistGenesisStartingWitness(t, r, api, build)
			switch mode {
			case "transient Pod read":
				r.APIReader = failingGenesisPodReader{Reader: api.Client}
			case "same-name replacement", "missing original Pod":
				if err := api.Delete(ctx, pod); err != nil {
					t.Fatal(err)
				}
				if mode == "same-name replacement" {
					replacement := pod.DeepCopy()
					replacement.ResourceVersion = ""
					replacement.UID = "replacement-pod"
					if err := api.Create(ctx, replacement); err != nil {
						t.Fatal(err)
					}
				}
			case "tampered CR witness":
				starting.Status.AdmissionGenesisWitness.RunnerRequestID = "wrong-request"
			}
			if err := r.finish(ctx, starting, kovav1.PhaseFailed, "RunnerUnavailable", "uncertain runner"); err == nil {
				t.Fatal("accepted build terminalized without original runner witness")
			}
			var current kovav1.KovaBuild
			if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
				t.Fatal(err)
			}
			if current.Status.Phase != kovav1.PhaseStarting || current.Status.AdmissionGenesisWitness == nil {
				t.Fatalf("accepted witness/status was not retained: phase=%s witness=%+v", current.Status.Phase, current.Status.AdmissionGenesisWitness)
			}
			var active corev1.ConfigMap
			if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &active); err != nil {
				t.Fatal(err)
			}
			state, err := decodeReservations(&active)
			if err != nil || len(state.Active) != 1 {
				t.Fatalf("accepted active charge was lost: state=%+v err=%v", state, err)
			}
		})
	}
}

func TestGenesisRunnerStatusRejectsWrongRequestOrPodReplacementDuringExec(t *testing.T) {
	for _, mode := range []string{"wrong request", "Pod replaced during Exec"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			starting, pod := persistGenesisStartingWitness(t, r, api, build)
			r.Kube = &fakeKube{podClient: api.Client, execFn: func(opts kube.ExecOptions) error {
				if mode == "Pod replaced during Exec" {
					if err := api.Delete(ctx, pod); err != nil {
						return err
					}
					replacement := pod.DeepCopy()
					replacement.ResourceVersion = ""
					replacement.UID = "replacement-pod"
					if err := api.Create(ctx, replacement); err != nil {
						return err
					}
				}
				requestID := string(build.UID)
				if mode == "wrong request" {
					requestID = "another-build"
				}
				_, _ = fmt.Fprintf(opts.Stdout, `{"status":"running","requestId":%q}`, requestID)
				return nil
			}}
			if _, err := r.observeBuildStatus(ctx, runnerexec.Client{Kube: r.Kube}, starting); err == nil {
				t.Fatal("unbound runner status was accepted")
			}
		})
	}
}

func TestGenesisExportTargetsRejectsUnexpectedOrDuplicateResults(t *testing.T) {
	_, _, build := genesisAdmissionFixture(t)
	build.Spec.Targets = buildTargets("registry.example/app:tag")
	results := buildresult.Pending(build)
	if len(results) != 1 {
		t.Fatalf("test expected one result target, got %d", len(results))
	}
	query := "with-fail=true&summary=true"
	if results[0].Format == "oci" {
		query += "&oci=true"
	}
	good := fmt.Sprintf("{\"target\":%q,\"success\":true}\n", results[0].Repository)
	for _, data := range []string{
		"{\"target\":\"other.example/app:tag\",\"success\":true}\n",
		good + good,
		"not-json\n",
	} {
		if err := validateGenesisExportTargets([]byte(data), build, query); err == nil {
			t.Fatalf("unsafe export target passed: %q", data)
		}
	}
	if err := validateGenesisExportTargets([]byte(good), build, query); err != nil {
		t.Fatalf("exact export target was refused: %v", err)
	}
}

func TestGenesisDirectBuildRefusesMissingOrReplacedLedger(t *testing.T) {
	for _, tc := range []struct {
		name, ledger string
		replace      bool
	}{
		{"active lost", admissioncontract.ActiveLedgerName, false},
		{"queue lost", admissioncontract.QueueLedgerName, false},
		{"active replaced", admissioncontract.ActiveLedgerName, true},
		{"queue replaced", admissioncontract.QueueLedgerName, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			var cm corev1.ConfigMap
			if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: tc.ledger}, &cm); err != nil {
				t.Fatal(err)
			}
			if err := api.Delete(ctx, &cm); err != nil {
				t.Fatal(err)
			}
			if tc.replace {
				replacement := cm.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = types.UID("replacement-ledger")
				if err := api.Create(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.admission(ctx, build); err == nil {
				t.Fatal("direct/admin CR received a grant without the original pair")
			}
			if api.patches != 0 {
				t.Fatalf("patched admission ledger after pair loss: %d", api.patches)
			}
			if tc.ledger == admissioncontract.QueueLedgerName && !tc.replace {
				if err := r.queueStoreForNamespace(build.Namespace).EnsureInitialized(ctx); err == nil {
					t.Fatal("Genesis queue store recreated lost ledger")
				}
				var missing corev1.ConfigMap
				if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: tc.ledger}, &missing); err == nil {
					t.Fatal("Genesis queue store recreated lost ledger")
				}
			}
		})
	}
}

func TestGenesisAdmissionPumpRefusesLostPair(t *testing.T) {
	ctx := context.Background()
	r, api, _ := genesisAdmissionFixture(t)
	var queue corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: r.Cfg.Namespace, Name: queueadmission.ConfigMapName}, &queue); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &queue); err != nil {
		t.Fatal(err)
	}
	pump := NewAdmissionPump(api.Client, r.Cfg)
	pump.Genesis = r.Genesis
	if _, err := pump.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: r.Cfg.Namespace, Name: AdmissionLedgerName,
	}}); err == nil {
		t.Fatal("pump continued selecting admissions without the original pair")
	}
}

func TestGenesisDirectBuildGrantUsesConditionalLedgerPatch(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	decision, err := r.admission(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Admitted || api.patches != 1 {
		t.Fatalf("direct/admin CR did not receive exactly one guarded grant: decision=%+v patches=%d", decision, api.patches)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := state.Active[reservationKey(build)]; !exists {
		t.Fatal("conditional grant did not persist active reservation")
	}
	if err := r.Genesis.Check(ctx); err != nil {
		t.Fatal(fmt.Errorf("grant changed qualified pair: %w", err))
	}
}
