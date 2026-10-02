package buildcontroller

import (
	"context"
	"errors"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func replaceRunnerBeforeUIDDelete(t *testing.T, storage client.Client, replacementUID types.UID) func(context.Context, string, string, types.UID) error {
	t.Helper()
	return func(ctx context.Context, namespace, name string, expectedUID types.UID) error {
		var original corev1.Pod
		key := types.NamespacedName{Namespace: namespace, Name: name}
		if err := storage.Get(ctx, key, &original); err != nil {
			return err
		}
		if original.UID != expectedUID {
			return errors.New("delete was not pinned to the Pod observed before replacement")
		}
		if err := storage.Delete(ctx, &original); err != nil {
			return err
		}
		replacement := original.DeepCopy()
		replacement.ResourceVersion = ""
		replacement.UID = replacementUID
		return storage.Create(ctx, replacement)
	}
}

func assertReplacementRunner(t *testing.T, storage client.Reader, build *kovav1.KovaBuild, uid types.UID) {
	t.Helper()
	var pod corev1.Pod
	if err := storage.Get(context.Background(), types.NamespacedName{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod); err != nil {
		t.Fatal(err)
	}
	if pod.UID != uid {
		t.Fatalf("runner Pod UID = %s, want replacement %s", pod.UID, uid)
	}
}

func TestDeletingBuildKeepsFinalizerAndCapacityWhenPodNameIsReused(t *testing.T) {
	ctx := context.Background()
	r, storage, build := deletingActiveFixture(t)
	kubeClient := r.Kube.(*fakeKube)
	replacementUID := types.UID("replacement-runner")
	kubeClient.beforeUIDDelete = replaceRunnerBeforeUIDDelete(t, storage, replacementUID)
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: build.Namespace, Name: build.Name}})
	if !apierrors.IsConflict(err) {
		t.Fatalf("delete after Pod replacement error = %v, want Conflict", err)
	}
	assertReplacementRunner(t, storage, build, replacementUID)
	if len(kubeClient.deleted) != 0 {
		t.Fatalf("replacement Pod was deleted: %#v", kubeClient.deleted)
	}
	var current kovav1.KovaBuild
	if err := storage.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if current.DeletionTimestamp.IsZero() || len(current.Finalizers) == 0 {
		t.Fatalf("finalizer was released despite replacement: deleting=%t finalizers=%#v", !current.DeletionTimestamp.IsZero(), current.Finalizers)
	}
	_, reservations, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if entry, ok := reservations.Active[reservationKey(build)]; !ok || !entry.Closing {
		t.Fatalf("capacity was released despite replacement: active=%#v", reservations.Active)
	}
}

func TestTerminalBuildKeepsCapacityWhenPodNameIsReused(t *testing.T) {
	build := lifecycleBuild("terminal-replacement", kovav1.PhaseSucceeded)
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: storage}
	replacementUID := types.UID("replacement-runner")
	kubeClient.beforeUIDDelete = replaceRunnerBeforeUIDDelete(t, storage, replacementUID)
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient}
	initializeAdmissionForTest(t, &r)
	_, err := r.reconcileTerminal(context.Background(), build)
	if !apierrors.IsConflict(err) {
		t.Fatalf("terminal cleanup after Pod replacement error = %v, want Conflict", err)
	}
	assertReplacementRunner(t, storage, build, replacementUID)
	if len(kubeClient.deleted) != 0 {
		t.Fatalf("replacement Pod was deleted: %#v", kubeClient.deleted)
	}
}

func TestPollFailureWindowKeepsRunningBuildWhenPodNameIsReused(t *testing.T) {
	build := lifecycleBuild("poll-replacement", kovav1.PhaseRunning)
	since := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.PollFailureSince = &since
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build)).Build()
	kubeClient := &fakeKube{podClient: storage, execFn: func(kube.ExecOptions) error { return errors.New("runner status unavailable") }}
	replacementUID := types.UID("replacement-runner")
	kubeClient.beforeUIDDelete = replaceRunnerBeforeUIDDelete(t, storage, replacementUID)
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	_, err := r.pollBuild(context.Background(), build)
	if !apierrors.IsConflict(err) {
		t.Fatalf("poll cleanup after Pod replacement error = %v, want Conflict", err)
	}
	assertReplacementRunner(t, storage, build, replacementUID)
	if got := storedLifecycleBuild(t, storage, build.Name).Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("build moved to %s despite uncertain cleanup", got)
	}
}

func TestBuildExpiryKeepsRunningBuildWhenPodNameIsReused(t *testing.T) {
	build := lifecycleBuild("expiry-replacement", kovav1.PhaseRunning)
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.StartedAt = &started
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, readyLifecyclePod(build)).Build()
	kubeClient := &fakeKube{podClient: storage}
	replacementUID := types.UID("replacement-runner")
	kubeClient.beforeUIDDelete = replaceRunnerBeforeUIDDelete(t, storage, replacementUID)
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient, Cfg: config.Config{MaxBuildDuration: time.Minute}}
	_, _, err := r.expireActiveBuild(context.Background(), build)
	if !apierrors.IsConflict(err) {
		t.Fatalf("expiry cleanup after Pod replacement error = %v, want Conflict", err)
	}
	assertReplacementRunner(t, storage, build, replacementUID)
	if got := storedLifecycleBuild(t, storage, build.Name).Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("build moved to %s despite uncertain cleanup", got)
	}
}

func TestRunnerCleanupRefusesEmptyPodUID(t *testing.T) {
	build := lifecycleBuild("empty-pod-uid", kovav1.PhaseRunning)
	pod := testRunnerPod(build)
	pod.UID = ""
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, pod).Build()
	kubeClient := &fakeKube{podClient: storage}
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient}
	if err := r.deleteRunnerAndConfirm(context.Background(), build); err == nil {
		t.Fatal("empty Pod UID should block cleanup")
	}
	if len(kubeClient.deleted) != 0 {
		t.Fatalf("name-only Pod deletion occurred: %#v", kubeClient.deleted)
	}
	var stillPresent corev1.Pod
	if err := storage.Get(context.Background(), client.ObjectKeyFromObject(pod), &stillPresent); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerCleanupAcceptsAlreadyMissingPod(t *testing.T) {
	build := lifecycleBuild("already-gone", kovav1.PhaseSucceeded)
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(build).Build()
	kubeClient := &fakeKube{podClient: storage}
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient}
	if err := r.deleteRunnerAndConfirm(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if len(kubeClient.deleted) != 0 {
		t.Fatalf("unexpected delete for absent Pod: %#v", kubeClient.deleted)
	}
}

func TestPollFailureRejectsUnexpectedStatusPodName(t *testing.T) {
	build := lifecycleBuild("status-name-drift", kovav1.PhaseRunning)
	build.Status.RunnerPodName = "unexpected-runner"
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: storage}
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient, Cfg: config.Config{PollRetryWindow: time.Minute}}
	_, err := r.retryStatusObservation(context.Background(), build, errors.New("status unavailable"))
	if err == nil {
		t.Fatal("unexpected status Pod name should block cleanup")
	}
	if len(kubeClient.deleted) != 0 {
		t.Fatalf("unexpected Pod was deleted: %#v", kubeClient.deleted)
	}
	if got := storedLifecycleBuild(t, storage, build.Name).Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("build moved to %s despite Pod name drift", got)
	}
}

func TestExpiryRejectsUnexpectedStatusPodNameBeforeCancel(t *testing.T) {
	build := lifecycleBuild("expiry-name-drift", kovav1.PhaseRunning)
	build.Status.RunnerPodName = "unexpected-runner"
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.StartedAt = &started
	storage := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kubeClient := &fakeKube{podClient: storage}
	r := KovaBuildReconciler{Client: storage, APIReader: storage, Kube: kubeClient, Cfg: config.Config{MaxBuildDuration: time.Minute}}
	_, _, err := r.expireActiveBuild(context.Background(), build)
	if err == nil {
		t.Fatal("unexpected status Pod name should block expiry cleanup")
	}
	if len(kubeClient.execCalls) != 0 || len(kubeClient.deleted) != 0 {
		t.Fatalf("unexpected Pod was touched: exec=%#v deleted=%#v", kubeClient.execCalls, kubeClient.deleted)
	}
	if got := storedLifecycleBuild(t, storage, build.Name).Status.Phase; got != kovav1.PhaseRunning {
		t.Fatalf("build moved to %s despite Pod name drift", got)
	}
}

type replaceBuildOnTTLDelete struct {
	client.Client
	seenUID types.UID
}

func (c *replaceBuildOnTTLDelete) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	build, ok := obj.(*kovav1.KovaBuild)
	if !ok {
		return c.Client.Delete(ctx, obj, opts...)
	}
	options := (&client.DeleteOptions{}).ApplyOptions(opts)
	if options.Preconditions == nil || options.Preconditions.UID == nil {
		return errors.New("terminal KovaBuild delete lacks a UID precondition")
	}
	c.seenUID = *options.Preconditions.UID
	if c.seenUID != build.UID {
		return errors.New("terminal KovaBuild delete used the wrong UID")
	}
	var original kovav1.KovaBuild
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(build), &original); err != nil {
		return err
	}
	if err := c.Client.Delete(ctx, &original); err != nil {
		return err
	}
	replacement := original.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = types.UID("replacement-build")
	if err := c.Client.Create(ctx, replacement); err != nil {
		return err
	}
	return apierrors.NewConflict(schema.GroupResource{Group: kovav1.Group, Resource: "kovabuilds"}, build.Name, errors.New("UID precondition failed"))
}

func TestTerminalTTLDeletePinsBuildUID(t *testing.T) {
	build := lifecycleBuild("ttl-replacement", kovav1.PhaseSucceeded)
	build.Finalizers = nil
	finished := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	build.Status.FinishedAt = &finished
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	storage := &replaceBuildOnTTLDelete{Client: base}
	r := KovaBuildReconciler{Client: storage, APIReader: base, Kube: &fakeKube{podClient: base}, Cfg: config.Config{JobTTL: time.Minute}}
	initializeAdmissionForTest(t, &r)
	_, err := r.reconcileTerminal(context.Background(), build)
	if !apierrors.IsConflict(err) {
		t.Fatalf("TTL delete after Build replacement error = %v, want Conflict", err)
	}
	if storage.seenUID != build.UID {
		t.Fatalf("TTL delete UID = %s, want %s", storage.seenUID, build.UID)
	}
	var replacement kovav1.KovaBuild
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(build), &replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.UID != "replacement-build" {
		t.Fatalf("replacement Build UID = %s", replacement.UID)
	}
}
