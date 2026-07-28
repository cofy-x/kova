package buildcontroller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeKube struct {
	deleted   []string
	deleteErr error
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

func (f *fakeKube) Exec(context.Context, string, string, kube.ExecOptions) error {
	return nil
}

func (f *fakeKube) ScaleDeployment(context.Context, string, string, int32) error {
	return nil
}

func TestReconcilerCreatesRunnerWithPVCMount(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "abc",
			Namespace: "jobs",
		},
		Spec: kovav1.KovaBuildSpec{
			Source: kovav1.KovaBuildSourceSpec{
				URI:    "file:///var/lib/kova/sources/builds/abc/source.zip",
				Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
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
			SourcePVCClaim:        "kova-sources",
			ArtifactRoot:          "/var/lib/kova/sources",
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
	if len(pod.Spec.Volumes) == 0 || pod.Spec.Volumes[0].PersistentVolumeClaim == nil || pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != "kova-sources" {
		t.Fatalf("unexpected volumes: %#v", pod.Spec.Volumes)
	}
	if len(pod.Spec.Containers[0].VolumeMounts) == 0 || pod.Spec.Containers[0].VolumeMounts[0].MountPath != "/var/lib/kova/sources" || !pod.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatalf("unexpected mounts: %#v", pod.Spec.Containers[0].VolumeMounts)
	}
	var updated kovav1.KovaBuild
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "abc"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != kovav1.PhaseStarting {
		t.Fatalf("phase = %s", updated.Status.Phase)
	}
}

func TestReconcilerMaterializesS3Source(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Spec: kovav1.KovaBuildSpec{Source: kovav1.KovaBuildSourceSpec{
			URI: "s3://builds/builds/pending/source.zip", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	reconciler := KovaBuildReconciler{Client: client, Scheme: scheme, Kube: &fakeKube{}, Cfg: config.Config{
		RunnerImage: "registry.local/kova:dev", ArtifactSecret: "s3-credentials",
		S3Endpoint: "objects.example.com", S3Bucket: "builds", S3Region: "us-east-1", S3Secure: true,
	}}
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
	if len(pod.Spec.InitContainers[0].EnvFrom) != 1 || pod.Spec.InitContainers[0].EnvFrom[0].SecretRef.Name != "s3-credentials" {
		t.Fatalf("fetch envFrom = %#v", pod.Spec.InitContainers[0].EnvFrom)
	}
	if command := strings.Join(pod.Spec.InitContainers[0].Command, " "); !strings.Contains(command, "--s3-endpoint objects.example.com") {
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

func TestReconcilerDeleteCleansPodSourceAndFinalizer(t *testing.T) {
	scheme := testScheme(t)
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kovav1.KovaBuild{}).
		Build()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "builds", "abc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "builds", "abc", "source.zip"), []byte("zip"), 0o644); err != nil {
		t.Fatal(err)
	}
	kube := &fakeKube{}
	store, err := artifactstore.NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   kube,
		Store:  store,
		Cfg:    config.Config{ArtifactRoot: root},
	}
	build := &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "abc",
			Namespace:  "jobs",
			Finalizers: []string{cleanupFinalizer},
		},
		Spec: kovav1.KovaBuildSpec{Source: kovav1.KovaBuildSourceSpec{
			URI: "file://" + filepath.Join(root, "builds", "abc", "source.zip"), Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
		Status: kovav1.KovaBuildStatus{RunnerPodName: "kova-job-abc"},
	}

	if _, err := reconciler.reconcileDelete(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "builds", "abc", "source.zip")); !os.IsNotExist(err) {
		t.Fatalf("source still exists or unexpected error: %v", err)
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
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "builds", "abc"), 0o755); err != nil {
		t.Fatal(err)
	}
	kube := &fakeKube{deleteErr: errors.New("delete failed")}
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   kube,
		Cfg:    config.Config{ArtifactRoot: root},
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
	if _, err := os.Stat(filepath.Join(root, "builds", "abc")); err != nil {
		t.Fatalf("source dir removed before pod cleanup succeeded: %v", err)
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
		Status:     kovav1.KovaBuildStatus{Phase: kovav1.PhaseRunning},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(older, newer, active).Build()
	reconciler := KovaBuildReconciler{Client: client, Cfg: config.Config{MaxActiveJobs: 2, WorkerSlots: 8}}

	admitted, activeCount, err := reconciler.admitted(context.Background(), older)
	if err != nil || !admitted || activeCount != 1 {
		t.Fatalf("older admitted=%t active=%d err=%v", admitted, activeCount, err)
	}
	admitted, _, err = reconciler.admitted(context.Background(), newer)
	if err != nil || admitted {
		t.Fatalf("newer admitted=%t err=%v", admitted, err)
	}
	if got := reconciler.allocatedConcurrency(older, activeCount+1); got != 4 {
		t.Fatalf("allocated concurrency=%d, want 4", got)
	}
}

func TestPersistResultsStoresFullSetAndBoundsStatus(t *testing.T) {
	store, err := artifactstore.NewFilesystem(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	build := &kovav1.KovaBuild{ObjectMeta: metav1.ObjectMeta{Name: "results"}}
	for range 101 {
		build.Status.Results = append(build.Status.Results, kovav1.BuildResult{Status: "succeeded"})
	}
	reconciler := KovaBuildReconciler{Store: store}
	if err := reconciler.persistResults(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if build.Status.ResultSummary.Total != 101 || build.Status.ResultSummary.Succeeded != 101 {
		t.Fatalf("summary = %#v", build.Status.ResultSummary)
	}
	if len(build.Status.Results) != 100 || build.Status.ResultArtifactURI == "" {
		t.Fatalf("inline=%d artifact=%q", len(build.Status.Results), build.Status.ResultArtifactURI)
	}
	reader, err := store.Open(context.Background(), build.Status.ResultArtifactURI)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || bytes.Count(raw, []byte(`"status":"succeeded"`)) != 101 {
		t.Fatalf("stored result count=%d err=%v", bytes.Count(raw, []byte(`"status":"succeeded"`)), err)
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
