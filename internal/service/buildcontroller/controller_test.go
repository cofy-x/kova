package buildcontroller

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeKube struct {
	deleted   []string
	deleteErr error
	execFn    func(kube.ExecOptions) error
	execCalls [][]string
}

func (f *fakeKube) GetSecretData(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (f *fakeKube) PodExists(context.Context, string, string) (bool, error) {
	return false, nil
}

func (f *fakeKube) CreatePod(context.Context, *corev1.Pod) error {
	return nil
}

func (f *fakeKube) WaitPodReady(context.Context, string, string, time.Duration) error {
	return nil
}

func (f *fakeKube) DeletePod(_ context.Context, namespace string, name string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, namespace+"/"+name)
	return nil
}

func (f *fakeKube) WritePodLogsTail(context.Context, string, string, int64, io.Writer) error {
	return nil
}

func (f *fakeKube) ListPods(context.Context, string, io.Writer, bool) error {
	return nil
}

func (f *fakeKube) ListPodsWithOptions(context.Context, string, io.Writer, kube.ListPodsOptions) error {
	return nil
}

func (f *fakeKube) Exec(_ context.Context, _, _ string, opts kube.ExecOptions) error {
	f.execCalls = append(f.execCalls, append([]string(nil), opts.Command...))
	if f.execFn != nil {
		return f.execFn(opts)
	}
	return nil
}

func (f *fakeKube) ScaleDeployment(context.Context, string, string, int32) error {
	return nil
}

func TestReconcilerCreatesRunnerWithImmutableSourceFetcher(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "jobs",
		},
		Spec: kovav1.KovaBuildSpec{
			Targets: []string{"registry.local/example:dev"},
			Source: kovav1.KovaBuildSourceSpec{
				URI:    "oci://registry.local/sources/abc@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1},
		},
	}
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(build).
		Build()
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   &fakeKube{},
		Cfg: config.Config{
			RunnerImage:           "registry.local/kova:dev",
			RunnerImagePullPolicy: "IfNotPresent",
			BuildkitAddr:          "tcp://kova.kova.svc:9094",
			RegistryPlainHTTP:     []string{"registry.local"},
			JobTTL:                time.Hour,
			PollInterval:          time.Millisecond,
			RunnerNodeSelector:    map[string]string{"kova.cofy.io/source-node": "true"},
		},
	}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "abc"}}); err != nil {
		t.Fatal(err)
	}
	var withFinalizer kovav1.KovaBuild
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "abc"}, &withFinalizer); err != nil {
		t.Fatal(err)
	}
	if len(withFinalizer.Finalizers) != 1 || withFinalizer.Finalizers[0] != cleanupFinalizer {
		t.Fatalf("finalizers = %#v", withFinalizer.Finalizers)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "abc"}}); err != nil {
		t.Fatal(err)
	}

	var pod corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "kova-job-abc"}, &pod); err != nil {
		t.Fatal(err)
	}
	if got := pod.Labels["kova.cofy.dev/build-id"]; got != "abc" {
		t.Fatalf("build label = %q", got)
	}
	if got := pod.Spec.NodeSelector["kova.cofy.io/source-node"]; got != "true" {
		t.Fatalf("runner node selector = %q", got)
	}
	if len(pod.Spec.Volumes) == 0 || pod.Spec.Volumes[0].EmptyDir == nil {
		t.Fatalf("unexpected volumes: %#v", pod.Spec.Volumes)
	}
	if len(pod.Spec.Containers[0].VolumeMounts) == 0 || pod.Spec.Containers[0].VolumeMounts[0].MountPath != "/var/lib/kova/source" {
		t.Fatalf("unexpected mounts: %#v", pod.Spec.Containers[0].VolumeMounts)
	}
	if len(pod.Spec.InitContainers) != 1 || !strings.Contains(strings.Join(pod.Spec.InitContainers[0].Command, " "), "source fetch") {
		t.Fatalf("source init container = %#v", pod.Spec.InitContainers)
	}
	var updated kovav1.KovaBuild
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "abc"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseStarting {
		t.Fatalf("phase = %s", updated.Status.Phase)
	}
}

func TestReconcilerMaterializesHTTPSSource(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Spec: kovav1.KovaBuildSpec{
			Targets: []string{"registry.local/example:dev"},
			Source: kovav1.KovaBuildSourceSpec{
				URI: "https://sources.example.com/pending.zip", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1},
		},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	reconciler := KovaBuildReconciler{Client: client, Scheme: scheme, Kube: &fakeKube{}, Cfg: config.Config{RunnerImage: "registry.local/kova:dev"}}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "pending"}})
	if err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "kova-job-pending"}, &pod); err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "source-fetch" {
		t.Fatalf("init containers = %#v", pod.Spec.InitContainers)
	}
	if command := strings.Join(pod.Spec.InitContainers[0].Command, " "); !strings.Contains(command, "https://sources.example.com/pending.zip") {
		t.Fatalf("fetch command = %q", command)
	}
}

func TestSubmitWhenReadyFailsAfterRunnerStartupTimeout(t *testing.T) {
	scheme := testScheme(t)
	started := metav1.NewTime(time.Now().Add(-time.Minute))
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "timed-out", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Status: kovav1.KovaBuildStatus{
			Phase: kovav1.PhaseStarting, RunnerPodName: "kova-job-timed-out", StartedAt: &started,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "kova-job-timed-out", Namespace: "jobs"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	reconciler := KovaBuildReconciler{Client: client, Scheme: scheme, Kube: &fakeKube{}, Cfg: config.Config{WaitTimeout: time.Second}}

	if _, err := reconciler.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	var updated kovav1.KovaBuild
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "timed-out"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseFailed || !strings.Contains(updated.Status.Message, "did not become ready") {
		t.Fatalf("status = %#v", updated.Status)
	}
}

func TestSubmitWhenReadyFailsWhenRunnerDisappears(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Status: kovav1.KovaBuildStatus{
			Phase: kovav1.PhaseStarting, RunnerPodName: "kova-job-missing",
		},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	reconciler := KovaBuildReconciler{Client: client, Scheme: scheme, Kube: &fakeKube{}}

	if _, err := reconciler.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	var updated kovav1.KovaBuild
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "missing"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseFailed || !strings.Contains(updated.Status.Message, "disappeared") {
		t.Fatalf("status = %#v", updated.Status)
	}
}

func TestSubmitWhenReadyReportsSourceFetchFailureAsInvalidSource(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-source", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Status: kovav1.KovaBuildStatus{
			Phase: kovav1.PhaseStarting, RunnerPodName: "kova-job-invalid-source",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "kova-job-invalid-source", Namespace: "jobs"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{{
			Name: "source-fetch", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "source digest mismatch",
			}},
		}}},
	}
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	reconciler := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: &fakeKube{}}
	if _, err := reconciler.submitWhenReady(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	var updated kovav1.KovaBuild
	if err := crClient.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "invalid-source"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseFailed || updated.Status.Reason != "InvalidSource" || updated.Status.Message != "source digest mismatch" {
		t.Fatalf("status = %#v", updated.Status)
	}
}

func TestSubmitWhenReadyValidatesExactSourceTargetSetBeforeBuild(t *testing.T) {
	tests := []struct {
		name          string
		inspection    string
		inspectionErr error
		wantReason    string
		wantBuild     bool
	}{
		{name: "extra", inspection: `{"targets":["registry.example/a:dev","registry.example/extra:dev"]}`, wantReason: "InvalidTargets"},
		{name: "missing", inspection: `{"targets":[]}`, wantReason: "InvalidTargets"},
		{name: "different", inspection: `{"targets":["registry.example/b:dev"]}`, wantReason: "InvalidTargets"},
		{name: "duplicate", inspectionErr: errors.New("duplicate target"), wantReason: "InvalidSource"},
		{name: "same-set-different-order", inspection: `{"targets":["registry.example/b:dev","registry.example/a:dev"]}`, wantBuild: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			build := &kovav1.KovaBuild{
				ObjectMeta: metav1.ObjectMeta{Name: "contract", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
				Spec: kovav1.KovaBuildSpec{
					Targets: []string{"registry.example/a:dev", "registry.example/b:dev"},
					Build:   kovav1.KovaBuildOptions{Format: "oci", Concurrency: 1},
				},
				Status: kovav1.KovaBuildStatus{Phase: kovav1.PhaseStarting, RunnerPodName: "kova-job-contract"},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "kova-job-contract", Namespace: "jobs"},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{
					Type: corev1.PodReady, Status: corev1.ConditionTrue,
				}}},
			}
			crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
			kubeClient := &fakeKube{}
			kubeClient.execFn = func(opts kube.ExecOptions) error {
				command := strings.Join(opts.Command, " ")
				if strings.Contains(command, "source inspect") {
					if tt.inspectionErr != nil {
						return tt.inspectionErr
					}
					_, _ = io.WriteString(opts.Stdout, tt.inspection)
					return nil
				}
				_, _ = io.WriteString(opts.Stdout, `{"status":"running"}`)
				return nil
			}
			reconciler := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kubeClient}
			if _, err := reconciler.submitWhenReady(context.Background(), build); err != nil {
				t.Fatal(err)
			}
			var updated kovav1.KovaBuild
			if err := crClient.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "contract"}, &updated); err != nil {
				t.Fatal(err)
			}
			buildCalled := false
			for _, command := range kubeClient.execCalls {
				if strings.Contains(strings.Join(command, " "), " transport ") {
					buildCalled = true
				}
			}
			if buildCalled != tt.wantBuild {
				t.Fatalf("buildCalled=%v commands=%#v", buildCalled, kubeClient.execCalls)
			}
			if tt.wantBuild {
				if updated.Status.Phase != kovav1.PhaseRunning {
					t.Fatalf("status = %#v", updated.Status)
				}
			} else if updated.Status.Phase != kovav1.PhaseFailed || updated.Status.Reason != tt.wantReason {
				t.Fatalf("status = %#v", updated.Status)
			}
		})
	}
}

func TestReconcilerDeleteCleansPodAndFinalizer(t *testing.T) {
	scheme := testScheme(t)
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kovav1.KovaBuild{}).
		Build()
	kube := &fakeKube{}
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   kube,
	}
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "abc",
			Namespace:  "jobs",
			Finalizers: []string{cleanupFinalizer},
		},
		Status: kovav1.KovaBuildStatus{RunnerPodName: "kova-job-abc"},
	}

	if _, err := reconciler.reconcileDelete(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if len(kube.deleted) != 1 || kube.deleted[0] != "jobs/kova-job-abc" {
		t.Fatalf("deleted pods = %#v", kube.deleted)
	}
	if len(build.Finalizers) != 0 {
		t.Fatalf("finalizers = %#v", build.Finalizers)
	}
}

func TestReconcilerDeleteKeepsFinalizerWhenPodDeleteFails(t *testing.T) {
	scheme := testScheme(t)
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kovav1.KovaBuild{}).
		Build()
	kube := &fakeKube{deleteErr: errors.New("delete failed")}
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   kube,
	}
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "abc",
			Namespace:  "jobs",
			Finalizers: []string{cleanupFinalizer},
		},
		Status: kovav1.KovaBuildStatus{RunnerPodName: "kova-job-abc"},
	}

	if _, err := reconciler.reconcileDelete(context.Background(), build); err == nil {
		t.Fatal("expected error")
	}
	if len(build.Finalizers) != 1 || build.Finalizers[0] != cleanupFinalizer {
		t.Fatalf("finalizers = %#v", build.Finalizers)
	}
}

func TestTerminalFailurePreservesVerifiedPartialOutputs(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: "jobs"},
		Status: kovav1.KovaBuildStatus{Outputs: []kovav1.BuildOutput{{
			Format: "oci", Image: "registry.example.com/team/a:dev",
			ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		}}},
	}
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	reconciler := KovaBuildReconciler{Client: crClient}
	if err := reconciler.finish(context.Background(), build, kovav1.PhaseFailed, "ResultVerificationFailed", "another output failed"); err != nil {
		t.Fatal(err)
	}
	var updated kovav1.KovaBuild
	if err := crClient.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "partial"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseFailed || len(updated.Status.Outputs) != 1 || updated.Status.Outputs[0].ManifestDigest == "" {
		t.Fatalf("status = %#v", updated.Status)
	}
}

func TestAdmissionIsFIFOAndCapacityAware(t *testing.T) {
	scheme := testScheme(t)
	older := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "older", Namespace: "jobs", CreationTimestamp: metav1.NewTime(time.Unix(1, 0))},
		Spec:       kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Concurrency: 8}},
	}
	newer := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "newer", Namespace: "jobs", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))},
		Spec:       kovav1.KovaBuildSpec{Build: kovav1.KovaBuildOptions{Concurrency: 8}},
	}
	active := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "jobs"},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseRunning, AllocatedConcurrency: 3},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(older, newer, active).Build()
	reconciler := KovaBuildReconciler{Client: client, Cfg: config.Config{MaxActiveJobs: 2, WorkerSlots: 8}}

	decision, err := reconciler.admission(context.Background(), older)
	if err != nil || !decision.Admitted || decision.Allocation != 5 {
		t.Fatalf("older decision=%#v err=%v", decision, err)
	}
	decision, err = reconciler.admission(context.Background(), newer)
	if err != nil || decision.Admitted {
		t.Fatalf("newer decision=%#v err=%v", decision, err)
	}
}

func TestAdmissionRoundRobinsRequesters(t *testing.T) {
	scheme := testScheme(t)
	builds := []*kovav1.KovaBuild{
		{ObjectMeta: metav1.ObjectMeta{Name: "alice-1", Namespace: "jobs", CreationTimestamp: metav1.NewTime(time.Unix(1, 0))}, Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "alice"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alice-2", Namespace: "jobs", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))}, Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "alice"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "bob-1", Namespace: "jobs", CreationTimestamp: metav1.NewTime(time.Unix(3, 0))}, Spec: kovav1.KovaBuildSpec{Requester: kovav1.KovaBuildRequester{Username: "bob"}}},
	}
	objects := make([]client.Object, 0, len(builds))
	for _, build := range builds {
		objects = append(objects, build)
	}
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(objects...).Build()
	reconciler := KovaBuildReconciler{Client: crClient, Cfg: config.Config{MaxActiveJobs: 2, WorkerSlots: 2}}

	decision, err := reconciler.admission(context.Background(), builds[2])
	if err != nil || !decision.Admitted {
		t.Fatalf("bob decision=%#v err=%v", decision, err)
	}
	decision, err = reconciler.admission(context.Background(), builds[1])
	if err != nil || decision.Admitted {
		t.Fatalf("second alice decision=%#v err=%v", decision, err)
	}
}

func TestReconcilerProcessesCancellationRequest(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "cancel", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}, Annotations: map[string]string{kovav1.CancellationRequestedAnnotation: time.Now().Format(time.RFC3339Nano)}},
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseRunning, RunnerPodName: "kova-job-cancel", AllocatedConcurrency: 2},
	}
	crClient := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	kube := &fakeKube{}
	reconciler := KovaBuildReconciler{Client: crClient, Scheme: scheme, Kube: kube}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "cancel"}}); err != nil {
		t.Fatal(err)
	}
	var updated kovav1.KovaBuild
	if err := crClient.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "cancel"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseCancelled || len(kube.deleted) != 1 {
		t.Fatalf("status=%#v deleted=%#v", updated.Status, kube.deleted)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kovav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
