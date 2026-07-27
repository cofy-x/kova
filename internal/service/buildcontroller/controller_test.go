package buildcontroller

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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
			Source: kovav1.KovaBuildSourceSpec{Ready: true, PVC: kovav1.KovaBuildPVCSource{
				ClaimName: "kova-sources",
				Path:      "builds/abc/source.zip",
				MountPath: "/var/lib/kova/sources",
			}},
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
			SourceRoot:            "/var/lib/kova/sources",
			SourceMountPath:       "/var/lib/kova/sources",
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

func TestReconcilerWaitsForCommittedSource(t *testing.T) {
	scheme := testScheme(t)
	build := &kovav1.KovaBuild{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "jobs", Finalizers: []string{cleanupFinalizer}},
		Spec: kovav1.KovaBuildSpec{Source: kovav1.KovaBuildSourceSpec{Ready: false, PVC: kovav1.KovaBuildPVCSource{
			ClaimName: "kova-sources", Path: "builds/pending/source.zip",
		}}},
	}
	client := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	reconciler := KovaBuildReconciler{Client: client, Scheme: scheme, Kube: &fakeKube{}, Cfg: config.Config{RunnerImage: "registry.local/kova:dev"}}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "pending"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("result = %#v, want delayed requeue", result)
	}
	var pod corev1.Pod
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "kova-job-pending"}, &pod); err == nil {
		t.Fatal("runner pod was created before source became ready")
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
	reconciler := KovaBuildReconciler{
		Client: client,
		Scheme: scheme,
		Kube:   kube,
		Cfg:    config.Config{SourceRoot: root},
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
	if _, err := os.Stat(filepath.Join(root, "builds", "abc")); !os.IsNotExist(err) {
		t.Fatalf("source dir still exists or unexpected error: %v", err)
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
		Cfg:    config.Config{SourceRoot: root},
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
