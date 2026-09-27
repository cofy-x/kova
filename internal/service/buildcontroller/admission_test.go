package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type emptyCachedBuildList struct{ client.Client }

type admissionListCounter struct {
	client.Reader
	builds int
	pods   int
}

func (r *admissionListCounter) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch list.(type) {
	case *kovav1.KovaBuildList:
		r.builds++
	case *corev1.PodList:
		r.pods++
	}
	return r.Reader.List(ctx, list, opts...)
}

func (c emptyCachedBuildList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if builds, ok := list.(*kovav1.KovaBuildList); ok {
		builds.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

type uncertainConfigMapUpdate struct {
	client.Client
	mu      sync.Mutex
	failAt  int
	updates int
	failed  bool
}

type pausedActiveGrantUpdate struct {
	client.Client
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type pausedPodMissReader struct {
	client.Reader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *pausedPodMissReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if _, ok := obj.(*corev1.Pod); ok && apierrors.IsNotFound(err) {
		paused := false
		r.once.Do(func() {
			paused = true
			close(r.entered)
		})
		if paused {
			select {
			case <-r.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

func (c *pausedActiveGrantUpdate) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == reservationConfigMap {
		paused := false
		c.once.Do(func() {
			paused = true
			close(c.entered)
		})
		if paused {
			select {
			case <-c.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestCleanupFenceDefeatsPausedAdmissionCAS(t *testing.T) {
	for _, mode := range []string{"delete", "terminal", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			scheme := testScheme(t)
			build := queuedBuild("a", "alice", 1, 1)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
			cfg := admissionConfig()
			newLeader := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			initializeAdmissionForTest(t, &newLeader)
			paused := &pausedActiveGrantUpdate{Client: base, entered: make(chan struct{}), release: make(chan struct{})}
			oldLeader := KovaBuildReconciler{Client: paused, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}
			oldResult := make(chan error, 1)
			go func() {
				_, err := oldLeader.Reconcile(ctx, request)
				oldResult <- err
			}()
			select {
			case <-paused.entered:
			case err := <-oldResult:
				t.Fatalf("old reconcile exited before grant CAS: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("old admission did not reach grant CAS")
			}
			var current kovav1.KovaBuild
			if err := base.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: "a"}, &current); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "delete":
				if err := base.Delete(ctx, &current); err != nil {
					t.Fatal(err)
				}
			case "terminal":
				current.Status.Phase = kovav1.PhaseFailed
				if err := base.Status().Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if current.Annotations == nil {
					current.Annotations = map[string]string{}
				}
				current.Annotations[kovav1.CancellationRequestedAnnotation] = time.Now().Format(time.RFC3339Nano)
				if err := base.Update(ctx, &current); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := newLeader.Reconcile(ctx, request); err != nil {
				t.Fatal(err)
			}
			close(paused.release)
			select {
			case err := <-oldResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("old reconcile did not finish after fence")
			}
			_, state, err := newLeader.readReservations(ctx, "jobs")
			if err != nil {
				t.Fatal(err)
			}
			if state.Fence == 0 || len(state.Active) != 0 {
				t.Fatalf("cleanup did not fence stale grant: fence=%d active=%#v", state.Fence, state.Active)
			}
			var pods corev1.PodList
			if err := base.List(ctx, &pods, client.InNamespace("jobs")); err != nil || len(pods.Items) != 0 {
				t.Fatalf("old reconcile created orphan Pod: %d err=%v", len(pods.Items), err)
			}
		})
	}
}

func TestCleanupRechecksPodAfterEarlierInFlightCreateCompletes(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	build := queuedBuild("a", "alice", 1, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	oldLeader := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &oldLeader)
	if decision, err := oldLeader.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("old grant=%#v err=%v", decision, err)
	}
	attempt, err := oldLeader.beginPodCreate(ctx, build)
	if err != nil {
		t.Fatal(err)
	}
	var terminal kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: "a"}, &terminal); err != nil {
		t.Fatal(err)
	}
	terminal.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(ctx, &terminal); err != nil {
		t.Fatal(err)
	}
	reader := &pausedPodMissReader{Reader: base, entered: make(chan struct{}), release: make(chan struct{})}
	newLeader := KovaBuildReconciler{Client: base, APIReader: reader, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: admissionConfig()}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}
	cleanupResult := make(chan error, 1)
	go func() {
		_, err := newLeader.Reconcile(ctx, request)
		cleanupResult <- err
	}()
	select {
	case <-reader.entered:
	case err := <-cleanupResult:
		t.Fatalf("cleanup did not reach absent Pod read: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach absent Pod read")
	}
	_, state, err := newLeader.readReservations(ctx, "jobs")
	if err != nil || !state.Active[reservationKey(build)].Closing {
		t.Fatalf("cleanup did not close grant before Pod read: %#v err=%v", state.Active, err)
	}
	pod := testRunnerPod(build)
	pod.Annotations = map[string]string{podCreateAttemptKey: attempt}
	if err := base.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := oldLeader.completePodCreate(ctx, build, attempt); err != nil {
		t.Fatal(err)
	}
	close(reader.release)
	select {
	case err := <-cleanupResult:
		if err == nil {
			t.Fatal("cleanup released active grant after stale absent-Pod read")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after Pod appeared")
	}
	_, state, err = newLeader.readReservations(ctx, "jobs")
	if err != nil || len(state.Active) != 1 {
		t.Fatalf("live Pod lost its charged grant: %#v err=%v", state.Active, err)
	}
	if _, err := newLeader.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	_, state, err = newLeader.readReservations(ctx, "jobs")
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("grant remained after verified Pod deletion: %#v err=%v", state.Active, err)
	}
}

func (c *uncertainConfigMapUpdate) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.updates++
		failAt := c.failAt
		if failAt == 0 {
			failAt = 1
		}
		if c.updates == failAt {
			c.failed = true
			if err := c.Client.Update(ctx, obj, opts...); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestPodCreateFenceSurvivesLostLedgerResponse(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(fmt.Sprintf("update-%d", failAt), func(t *testing.T) {
			scheme := testScheme(t)
			build := queuedBuild("a", "alice", 1, 1)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
			uncertain := &uncertainConfigMapUpdate{Client: base, failAt: failAt}
			r := KovaBuildReconciler{Client: uncertain, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
			initializeAdmissionForTest(t, &r)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
				t.Fatal(err)
			}
			if !uncertain.failed {
				t.Fatalf("did not inject lost response for ConfigMap Update %d", failAt)
			}
			var pod corev1.Pod
			if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: buildPodName("a")}, &pod); err != nil {
				t.Fatal(err)
			}
			if pod.Annotations[podCreateAttemptKey] == "" {
				t.Fatal("Pod lacks fenced create nonce")
			}
			_, state, err := r.readReservations(context.Background(), "jobs")
			if err != nil {
				t.Fatal(err)
			}
			entry := state.Active[reservationKey(build)]
			if len(entry.InFlight) != 0 {
				t.Fatalf("resolved Pod Create retained nonce: %#v", entry.InFlight)
			}
		})
	}
}

func TestRestartedControllerDoesNotRetryUnresolvedPodCreate(t *testing.T) {
	scheme := testScheme(t)
	build := queuedBuild("a", "alice", 1, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r)
	if decision, err := r.admission(context.Background(), build); err != nil || !decision.Admitted {
		t.Fatalf("initial admission = %#v, %v", decision, err)
	}
	first, err := r.beginPodCreate(context.Background(), build)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate process loss before the result of its Pod Create is known.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err == nil {
		t.Fatal("replacement leader retried an unresolved Pod Create")
	}
	_, state, err := r.readReservations(context.Background(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	entry := state.Active[reservationKey(build)]
	if len(entry.InFlight) != 1 || entry.InFlight[0] != first {
		t.Fatalf("replacement leader changed unresolved attempts: %#v", entry.InFlight)
	}
	var current kovav1.KovaBuild
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &current); err != nil {
		t.Fatal(err)
	}
	if !apiMeta.IsStatusConditionTrue(current.Status.Conditions, admissionRecoveryCondition) {
		t.Fatal("unresolved Pod Create was not surfaced to the API")
	}
	late := testRunnerPod(build)
	late.Annotations = map[string]string{podCreateAttemptKey: first}
	if err := base.Create(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseStarting || apiMeta.IsStatusConditionTrue(current.Status.Conditions, admissionRecoveryCondition) {
		t.Fatalf("persisted Pod did not resolve recovery: %#v", current.Status)
	}
}

type uncertainPodCreate struct {
	client.Client
	failed bool
}

type forbiddenPodCreate struct{ client.Client }

func (c forbiddenPodCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("test PodSecurity rejection"))
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestDefinitivePodCreateRejectionReleasesNonceAndCapacity(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	build := queuedBuild("rejected", "alice", 1, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	r := KovaBuildReconciler{Client: forbiddenPodCreate{Client: base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r)
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: build.Name}}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	_, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	entry := state.Active[reservationKey(build)]
	if len(entry.InFlight) != 0 {
		t.Fatalf("definitive Pod rejection retained nonce: %#v", entry.InFlight)
	}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	_, state, err = r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Active) != 0 {
		t.Fatalf("definitive Pod rejection leaked active capacity: %#v", state.Active)
	}
	var current kovav1.KovaBuild
	if err := base.Get(ctx, request.NamespacedName, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != kovav1.PhaseFailed || current.Status.Reason != "RunnerCreateRejected" {
		t.Fatalf("unexpected rejection status: %#v", current.Status)
	}
}

func (c *uncertainPodCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*corev1.Pod); ok && !c.failed {
		c.failed = true
		if err := c.Client.Create(ctx, obj, opts...); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	return c.Client.Create(ctx, obj, opts...)
}

type failedStartingStatus struct {
	client.SubResourceWriter
	failed bool
}

func (s *failedStartingStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if build, ok := obj.(*kovav1.KovaBuild); ok && build.Status.Phase == kovav1.PhaseStarting && !s.failed {
		s.failed = true
		return errors.New("injected status failure")
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

type statusFailureClient struct {
	client.Client
	writer *failedStartingStatus
}

func (c *statusFailureClient) Status() client.SubResourceWriter { return c.writer }

func queuedBuild(name, requester string, order int64, slots int) *kovav1.KovaBuild {
	targets := buildTargets("registry.local/example:" + name)
	if slots > 1 {
		targets = buildTargets("registry.local/example:"+name+"-1", "registry.local/example:"+name+"-2")
	}
	return &kovav1.KovaBuild{
		TypeMeta: metav1.TypeMeta{APIVersion: kovav1.Group + "/" + kovav1.Version, Kind: "KovaBuild"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "jobs", UID: types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(time.Unix(order, 0)), Finalizers: []string{cleanupFinalizer}},
		Spec: kovav1.KovaBuildSpec{
			Requester: kovav1.KovaBuildRequester{Username: requester},
			Targets:   targets,
			Source: kovav1.KovaBuildSourceSpec{URI: "https://sources.example.com/build.zip",
				Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			Build: kovav1.KovaBuildOptions{Format: "oci", Concurrency: slots},
		},
	}
}

func admissionConfig() config.Config {
	return config.Config{
		RunnerImage: "registry.local/kova:dev", BuildkitPlatformAddrs: map[string]string{"linux/amd64": "tcp://buildkit:9094"},
		MaxActiveJobs: 2, MaxActiveJobsPerRequester: 1, MaxQueuedJobs: 1000, MaxQueuedJobsPerRequester: 100,
		WorkerSlots: 3, PollInterval: time.Millisecond,
	}
}

func initializeAdmissionForTest(t *testing.T, r *KovaBuildReconciler) {
	t.Helper()
	if _, _, err := r.initializeReservations(context.Background(), "jobs"); err != nil {
		t.Fatal(err)
	}
}

func completeAndReleaseAdmissionForTest(t *testing.T, r *KovaBuildReconciler, writer client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	var build kovav1.KovaBuild
	if err := writer.Get(ctx, client.ObjectKey{Namespace: "jobs", Name: name}, &build); err != nil {
		t.Fatal(err)
	}
	build.Status.Phase = kovav1.PhaseSucceeded
	if err := writer.Status().Update(ctx, &build); err != nil {
		t.Fatal(err)
	}
	if err := r.fenceReservation(ctx, &build); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, &build); err != nil {
		t.Fatal(err)
	}
}

func TestSequentialSingleSlotAdmissionRotatesDurablyAcrossLeaders(t *testing.T) {
	ctx := context.Background()
	first := queuedBuild("alice-1", "alice", 1, 1)
	second := queuedBuild("alice-2", "alice", 2, 1)
	other := queuedBuild("bob-1", "bob", 3, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(first, second, other).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	oldLeader := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &oldLeader)
	if decision, err := oldLeader.admission(ctx, first); err != nil || !decision.Admitted {
		t.Fatalf("first Alice grant = %#v, %v", decision, err)
	}
	if decision, err := oldLeader.admission(ctx, other); err != nil || decision.Admitted {
		t.Fatalf("Bob unexpectedly granted before Alice release = %#v, %v", decision, err)
	}
	_, waiting, err := oldLeader.readReservations(ctx, "jobs")
	if err != nil || waiting.LastGrantedRequesterHash != queueadmission.HashRequester("alice") {
		t.Fatalf("waiting attempt changed cursor = %q, %v", waiting.LastGrantedRequesterHash, err)
	}
	completeAndReleaseAdmissionForTest(t, &oldLeader, base, first.Name)

	// A replacement reconciler reads the cursor from the ledger. The oldest
	// remaining CR belongs to Alice, but Bob must receive the next free slot.
	newLeader := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	if decision, err := newLeader.admission(ctx, other); err != nil || !decision.Admitted {
		t.Fatalf("Bob grant after Alice release = %#v, %v", decision, err)
	}
	if decision, err := newLeader.admission(ctx, second); err != nil || decision.Admitted {
		t.Fatalf("second Alice grant before Bob release = %#v, %v", decision, err)
	}
	completeAndReleaseAdmissionForTest(t, &newLeader, base, other.Name)
	if decision, err := oldLeader.admission(ctx, second); err != nil || !decision.Admitted {
		t.Fatalf("second Alice grant after Bob release = %#v, %v", decision, err)
	}
	_, state, err := oldLeader.readReservations(ctx, "jobs")
	if err != nil || state.LastGrantedRequesterHash != queueadmission.HashRequester("alice") {
		t.Fatalf("durable cursor = %q, %v", state.LastGrantedRequesterHash, err)
	}
}

func TestAdmissionCursorDoesNotSkipLessServedRequesterAfterOutOfOrderGrant(t *testing.T) {
	first := queuedBuild("alice-1", "alice", 1, 1)
	other := queuedBuild("bob-1", "bob", 2, 1)
	third := queuedBuild("charlie-1", "charlie", 3, 1)
	moreBob := queuedBuild("bob-2", "bob", 4, 1)
	active := map[string]activeReservation{
		reservationKey(other): {BuildName: other.Name, Requester: "bob", Slots: 1},
	}
	builds := []kovav1.KovaBuild{*first, *other, *third, *moreBob}
	cursor := queueadmission.HashRequester("bob")
	if decision := decideAdmission(first, builds, active, 2, 2, 2, cursor); !decision.Admitted {
		t.Fatalf("Alice lost remaining capacity after Bob granted out of order: %#v", decision)
	}
	if decision := decideAdmission(third, builds, active, 2, 2, 2, cursor); decision.Admitted {
		t.Fatalf("Charlie jumped ahead of less-served, older Alice: %#v", decision)
	}
}

func TestAdmissionCursorFallsBackWhenRequesterDisappearsAndNewOneArrives(t *testing.T) {
	ctx := context.Background()
	first := queuedBuild("alice-1", "alice", 1, 1)
	other := queuedBuild("bob-1", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(first, other).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if decision, err := r.admission(ctx, first); err != nil || !decision.Admitted {
		t.Fatalf("Alice grant = %#v, %v", decision, err)
	}
	completeAndReleaseAdmissionForTest(t, &r, base, first.Name)
	newcomer := queuedBuild("charlie-1", "charlie", 3, 1)
	if err := base.Create(ctx, newcomer); err != nil {
		t.Fatal(err)
	}
	if decision, err := r.admission(ctx, other); err != nil || !decision.Admitted {
		t.Fatalf("oldest remaining Bob grant = %#v, %v", decision, err)
	}
	completeAndReleaseAdmissionForTest(t, &r, base, other.Name)
	returning := queuedBuild("alice-2", "alice", 4, 1)
	if err := base.Create(ctx, returning); err != nil {
		t.Fatal(err)
	}
	if decision, err := r.admission(ctx, newcomer); err != nil || !decision.Admitted {
		t.Fatalf("Charlie grant before returning Alice = %#v, %v", decision, err)
	}
}

func TestAdmissionCursorSkipsCancelledAndDeletingQueueHeads(t *testing.T) {
	ctx := context.Background()
	first := queuedBuild("alice-1", "alice", 1, 1)
	cancelled := queuedBuild("alice-2", "alice", 2, 1)
	cancelled.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "requested"}
	deleting := queuedBuild("bob-1", "bob", 3, 1)
	deletingAt := metav1.NewTime(time.Unix(5, 0))
	deleting.DeletionTimestamp = &deletingAt
	eligible := queuedBuild("charlie-1", "charlie", 4, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(first, cancelled, deleting, eligible).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if decision, err := r.admission(ctx, first); err != nil || !decision.Admitted {
		t.Fatalf("first grant = %#v, %v", decision, err)
	}
	completeAndReleaseAdmissionForTest(t, &r, base, first.Name)
	if decision, err := r.admission(ctx, eligible); err != nil || !decision.Admitted {
		t.Fatalf("eligible grant past cancelled/deleting heads = %#v, %v", decision, err)
	}
}

func TestAdmissionCursorUpdatesOnlyWithCommittedCASGrant(t *testing.T) {
	ctx := context.Background()
	first := queuedBuild("alice-1", "alice", 1, 1)
	other := queuedBuild("bob-1", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).
		WithObjects(first, other).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 2, 2
	fast := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &fast)
	paused := &pausedActiveGrantUpdate{Client: base, entered: make(chan struct{}), release: make(chan struct{})}
	releasePaused := sync.OnceFunc(func() { close(paused.release) })
	defer releasePaused()
	slow := KovaBuildReconciler{Client: paused, APIReader: base, Cfg: cfg}
	result := make(chan error, 1)
	go func() {
		decision, err := slow.admission(ctx, first)
		if err == nil && !decision.Admitted {
			err = fmt.Errorf("Alice was not granted after CAS retry")
		}
		result <- err
	}()
	select {
	case <-paused.entered:
	case err := <-result:
		t.Fatalf("slow grant exited before CAS pause: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("slow grant did not reach CAS update")
	}
	if decision, err := fast.admission(ctx, other); err != nil || !decision.Admitted {
		t.Fatalf("concurrent Bob grant = %#v, %v", decision, err)
	}
	_, before, err := fast.readReservations(ctx, "jobs")
	if err != nil || before.LastGrantedRequesterHash != queueadmission.HashRequester("bob") || len(before.Active) != 1 {
		t.Fatalf("cursor before paused CAS resumed = %#v, %v", before, err)
	}
	releasePaused()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow grant did not retry after CAS conflict")
	}
	_, after, err := fast.readReservations(ctx, "jobs")
	if err != nil || after.LastGrantedRequesterHash != queueadmission.HashRequester("alice") || len(after.Active) != 2 {
		t.Fatalf("cursor and grants after CAS retry = %#v, %v", after, err)
	}
}

func TestAdmissionCursorAcceptsLegacyLedgerAndRejectsMalformedValue(t *testing.T) {
	legacy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: reservationConfigMap, Namespace: "jobs"},
		Data:       map[string]string{reservationDataKey: `{"version":1,"maxJobs":1,"maxPerRequester":1,"workerSlots":1,"active":{}}`},
	}
	state, err := decodeReservations(legacy)
	if err != nil || state.LastGrantedRequesterHash != "" {
		t.Fatalf("legacy ledger cursor = %q, %v", state.LastGrantedRequesterHash, err)
	}
	corrupt := legacy.DeepCopy()
	corrupt.Data[reservationDataKey] = `{"version":1,"maxJobs":1,"maxPerRequester":1,"workerSlots":1,"lastGrantedRequesterHash":"not-a-hash","active":{}}`
	if _, err := decodeReservations(corrupt); err == nil {
		t.Fatal("malformed durable cursor did not fail closed")
	}
}

func TestMissingActiveLedgerAfterStartupFailsClosed(t *testing.T) {
	for _, mode := range []string{"admit", "fence", "terminal", "delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
			cfg := admissionConfig()
			queue := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
			r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
			initializeAdmissionForTest(t, &r)
			if err := queue.EnsureInitialized(ctx); err != nil {
				t.Fatal(err)
			}
			build := queuedBuild("a", "alice", 1, 1)
			if err := base.Create(ctx, build); err != nil {
				t.Fatal(err)
			}
			if mode == "terminal" {
				build.Status.Phase = kovav1.PhaseFailed
				if err := base.Status().Update(ctx, build); err != nil {
					t.Fatal(err)
				}
			}
			var cm corev1.ConfigMap
			key := client.ObjectKey{Namespace: "jobs", Name: reservationConfigMap}
			if err := base.Get(ctx, key, &cm); err != nil {
				t.Fatal(err)
			}
			if err := base.Delete(ctx, &cm); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "admit":
				_, err = r.admission(ctx, build)
			case "fence":
				err = r.fenceReservation(ctx, build)
			case "terminal":
				_, err = r.reconcileTerminal(ctx, build)
			case "delete":
				_, err = r.reconcileDelete(ctx, build)
			}
			if !apierrors.IsNotFound(err) {
				t.Fatalf("missing active ledger did not block %s: %v", mode, err)
			}
			if err := base.Get(ctx, key, &cm); !apierrors.IsNotFound(err) {
				t.Fatalf("%s recreated missing active ledger: %v", mode, err)
			}
			var pods corev1.PodList
			if err := base.List(ctx, &pods, client.InNamespace("jobs")); err != nil || len(pods.Items) != 0 {
				t.Fatalf("%s created runner Pod: %d, err=%v", mode, len(pods.Items), err)
			}
			if err := EnsureAdmissionLedger(ctx, base, base, "jobs", cfg); err == nil {
				t.Fatalf("startup recreated missing active ledger while build exists after %s", mode)
			}
		})
	}
}

func TestStartupCreatesActiveLedgerInEmptyNamespace(t *testing.T) {
	ctx := context.Background()
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	cfg := admissionConfig()
	queue := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
	if err := EnsureAdmissionLedger(ctx, base, base, "jobs", cfg); err != nil {
		t.Fatal(err)
	}
	if err := queue.EnsureInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	if err := CheckAdmissionLedger(ctx, base, "jobs", cfg); err != nil {
		t.Fatal(err)
	}
}

type countingStatusClient struct {
	client.Client
	writes int
}

type countingStatusWriter struct {
	client.SubResourceWriter
	parent *countingStatusClient
}

func (c *countingStatusClient) Status() client.SubResourceWriter {
	return &countingStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

func (w *countingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.parent.writes++
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestUnchangedCapacityWaitDoesNotRewriteBuildStatus(t *testing.T) {
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	counted := &countingStatusClient{Client: base}
	r := KovaBuildReconciler{Client: counted, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if decision, err := r.admission(context.Background(), a); err != nil || !decision.Admitted {
		t.Fatalf("blocker admission = %#v, err=%v", decision, err)
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if counted.writes != 1 {
		t.Fatalf("first capacity wait status writes = %d", counted.writes)
	}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if counted.writes != 1 {
		t.Fatalf("unchanged capacity wait rewrote status: %d", counted.writes)
	}
}

func TestQueueIntentReleasesOnlyAfterActiveGrant(t *testing.T) {
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester = 1, 1
	if err := EnsureAdmissionLedger(context.Background(), base, base, "jobs", cfg); err != nil {
		t.Fatal(err)
	}
	store := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1}
	if err := store.EnsureInitialized(context.Background()); err != nil {
		t.Fatal(err)
	}
	build := queuedBuild("a", "alice", 1, 1)
	intent, fresh, err := store.Reserve(context.Background(), build)
	if err != nil || !fresh {
		t.Fatalf("queue reserve fresh=%t err=%v", fresh, err)
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
	if err := base.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Lookup(context.Background(), "a"); err != nil || found {
		t.Fatalf("queue remained after active commit: found=%t err=%v", found, err)
	}
	_, active, err := r.readReservations(context.Background(), "jobs")
	if err != nil || len(active.Active) != 1 {
		t.Fatalf("active grant missing after queue release: %#v err=%v", active, err)
	}
	if _, fresh, err := store.Reserve(context.Background(), queuedBuild("b", "bob", 2, 1)); err != nil || !fresh {
		t.Fatalf("queue slot was not released after activation: fresh=%t err=%v", fresh, err)
	}
}

func TestActiveLedgerRejectsReplicaWithDifferentLimits(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	cfg := admissionConfig()
	r1 := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r1)
	if decision, err := r1.admission(context.Background(), a); err != nil || !decision.Admitted {
		t.Fatalf("first replica admission=%#v err=%v", decision, err)
	}
	cfg.WorkerSlots++
	r2 := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	if _, err := r2.admission(context.Background(), b); err == nil {
		t.Fatal("replica with different worker limit accepted a grant")
	}
}

type failedQueueRelease struct{ client.Client }

func (c failedQueueRelease) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == queueadmission.ConfigMapName {
		return errors.New("injected queue release failure")
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestFailedQueueReleaseOvercountsUntilControllerRestart(t *testing.T) {
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester = 1, 1
	if err := EnsureAdmissionLedger(context.Background(), base, base, "jobs", cfg); err != nil {
		t.Fatal(err)
	}
	store := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1}
	if err := store.EnsureInitialized(context.Background()); err != nil {
		t.Fatal(err)
	}
	build := queuedBuild("a", "alice", 1, 1)
	intent, _, err := store.Reserve(context.Background(), build)
	if err != nil {
		t.Fatal(err)
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
	if err := base.Create(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	r1 := KovaBuildReconciler{Client: failedQueueRelease{Client: base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	if _, err := r1.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err == nil {
		t.Fatal("failed queue release unexpectedly created runner")
	}
	_, active, err := r1.readReservations(context.Background(), "jobs")
	if err != nil || len(active.Active) != 1 {
		t.Fatalf("active grant did not survive queue release failure: %#v err=%v", active, err)
	}
	if _, found, err := store.Lookup(context.Background(), "a"); err != nil || !found {
		t.Fatalf("queue intent was prematurely freed: found=%t err=%v", found, err)
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods); err != nil || len(pods.Items) != 0 {
		t.Fatalf("runner created before queue transition: %d err=%v", len(pods.Items), err)
	}
	r2 := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Lookup(context.Background(), "a"); err != nil || found {
		t.Fatalf("restart did not finish queue release: found=%t err=%v", found, err)
	}
}

func TestUnreservedHTTPBuildCannotReachActivePod(t *testing.T) {
	scheme := testScheme(t)
	build := queuedBuild("a", "alice", 1, 1)
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: "00112233445566778899aabbccddeeff"}
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("missing queue intent did not fail closed: %v", err)
	}
	var current kovav1.KovaBuild
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &current); err != nil {
		t.Fatal(err)
	}
	if !apiMeta.IsStatusConditionTrue(current.Status.Conditions, admissionRecoveryCondition) {
		t.Fatal("queue ledger drift was not exposed through build status")
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods); err != nil || len(pods.Items) != 0 {
		t.Fatalf("runner escaped missing queue intent: %d err=%v", len(pods.Items), err)
	}
}

func TestTerminalAndDeletionReleaseQueuedIntentAfterPodCheck(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting-%t", deleting), func(t *testing.T) {
			scheme := testScheme(t)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
			cfg := admissionConfig()
			cfg.MaxQueuedJobs, cfg.MaxQueuedJobsPerRequester = 1, 1
			r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			initializeAdmissionForTest(t, &r)
			store := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: 1, RequesterLimit: 1}
			if err := store.EnsureInitialized(context.Background()); err != nil {
				t.Fatal(err)
			}
			build := queuedBuild("a", "alice", 1, 1)
			intent, _, err := store.Reserve(context.Background(), build)
			if err != nil {
				t.Fatal(err)
			}
			build.Annotations = map[string]string{queueadmission.IntentAnnotation: intent.Nonce}
			if err := base.Create(context.Background(), build); err != nil {
				t.Fatal(err)
			}
			if deleting {
				if _, err := r.reconcileDelete(context.Background(), build); err != nil {
					t.Fatal(err)
				}
			} else {
				build.Status.Phase = kovav1.PhaseFailed
				if err := base.Status().Update(context.Background(), build); err != nil {
					t.Fatal(err)
				}
				if _, err := r.reconcileTerminal(context.Background(), build); err != nil {
					t.Fatal(err)
				}
			}
			if _, found, err := store.Lookup(context.Background(), "a"); err != nil || found {
				t.Fatalf("queue entry not released after verified cleanup: found=%t err=%v", found, err)
			}
		})
	}
}

func TestConcurrentReconcilesReserveBeforePodWithStaleCache(t *testing.T) {
	builds := []*kovav1.KovaBuild{
		queuedBuild("alice-1", "alice", 1, 2), queuedBuild("alice-2", "alice", 2, 2),
		queuedBuild("bob-1", "bob", 3, 2), queuedBuild("bob-2", "bob", 4, 2),
		queuedBuild("charlie-1", "charlie", 5, 2), queuedBuild("charlie-2", "charlie", 6, 2),
	}
	objects := make([]client.Object, 0, len(builds))
	for _, build := range builds {
		objects = append(objects, build)
	}
	scheme := testScheme(t)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(objects...).Build()
	// A manager cache stuck at an empty List cannot affect admission or counts.
	r1 := KovaBuildReconciler{Client: emptyCachedBuildList{base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	r2 := KovaBuildReconciler{Client: emptyCachedBuildList{base}, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r1)
	var wg sync.WaitGroup
	errCh := make(chan error, len(builds))
	for i, build := range builds {
		reconciler := &r1
		if i%2 == 1 {
			reconciler = &r2
		}
		wg.Add(1)
		go func(name string, reconciler *KovaBuildReconciler) {
			defer wg.Done()
			_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: name}})
			errCh <- err
		}(build.Name, reconciler)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods, client.InNamespace("jobs")); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("runner Pods = %d, want 2", len(pods.Items))
	}
	admitted := map[string]bool{}
	for _, pod := range pods.Items {
		admitted[pod.Labels["kova.cofy.dev/build-id"]] = true
	}
	if !admitted["alice-1"] || !admitted["bob-1"] {
		t.Fatalf("fair queue admitted %#v", admitted)
	}
	_, ledger, err := r1.readReservations(context.Background(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	used := 0
	for _, entry := range ledger.Active {
		used += entry.Slots
	}
	if len(ledger.Active) != 2 || used != 3 {
		t.Fatalf("active reservations=%d worker slots=%d", len(ledger.Active), used)
	}
}

func TestIneligibleQueuedBuildDoesNotConsumeVirtualFairShare(t *testing.T) {
	for _, mode := range []string{"deleting", "cancelling"} {
		t.Run(mode, func(t *testing.T) {
			old := queuedBuild("old", "alice", 1, 1)
			old.Status.Phase = kovav1.PhaseQueued
			switch mode {
			case "deleting":
				deletingAt := metav1.NewTime(time.Unix(3, 0))
				old.DeletionTimestamp = &deletingAt
			case "cancelling":
				old.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: "requested"}
			}
			eligible := queuedBuild("eligible", "bob", 2, 1)
			scheme := testScheme(t)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(old, eligible).Build()
			cfg := admissionConfig()
			cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
			r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			initializeAdmissionForTest(t, &r)
			decision, err := r.admission(context.Background(), eligible)
			if err != nil || !decision.Admitted || decision.Allocation != 1 {
				t.Fatalf("eligible build blocked by %s queued build: decision=%#v err=%v", mode, decision, err)
			}
			_, ledger, err := r.readReservations(context.Background(), "jobs")
			if err != nil || len(ledger.Active) != 1 || ledger.Active[reservationKey(eligible)].Slots != 1 {
				t.Fatalf("wrong active reservation after %s queued build: %#v err=%v", mode, ledger.Active, err)
			}
		})
	}
}

func TestSaturatedAdmissionSkipsFullListsAtDepth(t *testing.T) {
	for _, depth := range []int{100, 500, 1000} {
		t.Run(fmt.Sprintf("queued-%d", depth), func(t *testing.T) {
			blocker := queuedBuild("blocker", "active", 1, 1)
			objects := []client.Object{blocker}
			for i := 0; i < depth; i++ {
				objects = append(objects, queuedBuild(fmt.Sprintf("queued-%04d", i), fmt.Sprintf("requester-%d", i), int64(i+2), 1))
			}
			base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(objects...).Build()
			cfg := admissionConfig()
			cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
			r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
			initializeAdmissionForTest(t, &r)
			if decision, err := r.admission(context.Background(), blocker); err != nil || !decision.Admitted {
				t.Fatalf("blocker grant=%#v err=%v", decision, err)
			}
			counter := &admissionListCounter{Reader: base}
			r.APIReader = counter
			for i := 0; i < 5; i++ {
				candidate := objects[1+i%depth].(*kovav1.KovaBuild)
				decision, err := r.admission(context.Background(), candidate)
				if err != nil || decision.Admitted || decision.Message != "waiting for an active job slot" {
					t.Fatalf("saturated decision=%#v err=%v", decision, err)
				}
			}
			if counter.builds != 0 || counter.pods != 0 {
				t.Fatalf("saturated polls performed full Lists: builds=%d pods=%d", counter.builds, counter.pods)
			}
		})
	}
}

func TestSaturatedAdmissionMatchesFullDecision(t *testing.T) {
	candidate := queuedBuild("candidate", "alice", 2, 1)
	for _, tc := range []struct {
		name             string
		requester        string
		maxJobs          int
		maxRequesterJobs int
		workerSlots      int
		wantMessage      string
	}{
		{name: "active-jobs", requester: "bob", maxJobs: 1, maxRequesterJobs: 2, workerSlots: 2, wantMessage: "waiting for an active job slot"},
		{name: "worker-slots", requester: "bob", maxJobs: 2, maxRequesterJobs: 2, workerSlots: 1, wantMessage: "waiting for worker capacity"},
		{name: "requester-jobs", requester: "alice", maxJobs: 2, maxRequesterJobs: 1, workerSlots: 2, wantMessage: "waiting for fair-share capacity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			active := map[string]activeReservation{"blocker": {BuildName: "blocker", Requester: tc.requester, Slots: 1}}
			fast, saturated := saturatedAdmission(candidate, active, tc.maxJobs, tc.maxRequesterJobs, tc.workerSlots)
			full := decideAdmission(candidate, []kovav1.KovaBuild{*candidate}, active, tc.maxJobs, tc.maxRequesterJobs, tc.workerSlots, "")
			if !saturated || fast != full || fast.Message != tc.wantMessage {
				t.Fatalf("fast=%#v saturated=%t full=%#v", fast, saturated, full)
			}
		})
	}
}

func TestSaturatedAdmissionKeepsQueueDriftFailClosed(t *testing.T) {
	ctx := context.Background()
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	store := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
	if err := store.EnsureInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	blocker := queuedBuild("blocker", "active", 1, 1)
	drift := queuedBuild("drift", "other", 2, 1)
	drift.Annotations = map[string]string{queueadmission.IntentAnnotation: "00112233445566778899aabbccddeeff"}
	for _, build := range []*kovav1.KovaBuild{blocker, drift} {
		if err := base.Create(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	if decision, err := r.admission(ctx, blocker); err != nil || !decision.Admitted {
		t.Fatalf("blocker grant=%#v err=%v", decision, err)
	}
	counter := &admissionListCounter{Reader: base}
	r.APIReader = counter
	if _, err := r.admission(ctx, drift); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("saturated admission hid queue intent drift: %v", err)
	}
	if counter.builds != 0 || counter.pods != 0 {
		t.Fatalf("drift check performed full Lists: builds=%d pods=%d", counter.builds, counter.pods)
	}
}

func TestSaturatedAdmissionChecksOrphanBeforeNextGrant(t *testing.T) {
	ctx := context.Background()
	blocker := queuedBuild("blocker", "active", 1, 1)
	next := queuedBuild("next", "other", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(blocker, next).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r := KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, &r)
	if decision, err := r.admission(ctx, blocker); err != nil || !decision.Admitted {
		t.Fatalf("blocker grant=%#v err=%v", decision, err)
	}
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "jobs", Labels: map[string]string{"kova.cofy.dev/build-id": "missing"}}}
	if err := base.Create(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if decision, err := r.admission(ctx, next); err != nil || decision.Admitted {
		t.Fatalf("saturated next grant=%#v err=%v", decision, err)
	}
	if err := r.fenceReservation(ctx, blocker); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, blocker); err != nil {
		t.Fatal(err)
	}
	if _, err := r.admission(ctx, next); err == nil || !strings.Contains(err.Error(), "no matching KovaBuild owner") {
		t.Fatalf("unreserved orphan did not block next grant: %v", err)
	}
	_, state, err := r.readReservations(ctx, "jobs")
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("orphan path wrote a new grant: %#v err=%v", state.Active, err)
	}
}

func TestReservationSurvivesUnknownWriteAndRestart(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	uncertain := &uncertainConfigMapUpdate{Client: base}
	r1 := KovaBuildReconciler{Client: uncertain, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	initializeAdmissionForTest(t, &r1)
	if _, err := r1.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first reconcile error = %v", err)
	}
	if !uncertain.failed {
		t.Fatal("did not inject an uncertain ConfigMap update")
	}
	r2 := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
		t.Fatal(err)
	}
	var pods corev1.PodList
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pod created without a reservation recovery: %d", len(pods.Items))
	}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "a" {
		t.Fatalf("recovered Pods = %#v", pods.Items)
	}
	var finished kovav1.KovaBuild
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &finished); err != nil {
		t.Fatal(err)
	}
	finished.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(context.Background(), &finished); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.reconcileTerminal(context.Background(), &finished); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := base.List(context.Background(), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "b" {
		t.Fatalf("capacity did not transfer after verified cleanup: %#v", pods.Items)
	}
}

func TestPodCreateUnknownResultAndStatusFailureHoldCapacity(t *testing.T) {
	for _, mode := range []string{"pod-create", "status-update"} {
		t.Run(mode, func(t *testing.T) {
			scheme := testScheme(t)
			a := queuedBuild("a", "alice", 1, 1)
			b := queuedBuild("b", "bob", 2, 1)
			base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
			cfg := admissionConfig()
			cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
			var writer client.Client
			if mode == "pod-create" {
				writer = &uncertainPodCreate{Client: base}
			} else {
				writer = &statusFailureClient{Client: base, writer: &failedStartingStatus{SubResourceWriter: base.Status()}}
			}
			r1 := KovaBuildReconciler{Client: writer, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
			initializeAdmissionForTest(t, &r1)
			if _, err := r1.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); mode == "status-update" && err == nil {
				t.Fatal("injected status failure did not reach reconcile")
			}
			r2 := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
			if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
				t.Fatal(err)
			}
			var pods corev1.PodList
			if err := base.List(context.Background(), &pods); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "a" {
				t.Fatalf("capacity escaped after %s: %#v", mode, pods.Items)
			}
			if mode == "status-update" {
				if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "a"}}); err != nil {
					t.Fatal(err)
				}
			}
			_, ledger, err := r2.readReservations(context.Background(), "jobs")
			if err != nil || len(ledger.Active) != 1 {
				t.Fatalf("ledger after %s: %#v, err=%v", mode, ledger, err)
			}
			var finished kovav1.KovaBuild
			if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &finished); err != nil {
				t.Fatal(err)
			}
			finished.Status.Phase = kovav1.PhaseFailed
			if err := base.Status().Update(context.Background(), &finished); err != nil {
				t.Fatal(err)
			}
			if _, err := r2.reconcileTerminal(context.Background(), &finished); err != nil {
				t.Fatal(err)
			}
			if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: "b"}}); err != nil {
				t.Fatal(err)
			}
			if err := base.List(context.Background(), &pods); err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 1 || pods.Items[0].Labels["kova.cofy.dev/build-id"] != "b" {
				t.Fatalf("capacity did not recover after %s cleanup: %#v", mode, pods.Items)
			}
		})
	}
}

func TestMissingReservationForLivePodFailsClosed(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	r := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r)
	if err := base.Create(context.Background(), testRunnerPod(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.admission(context.Background(), b); err == nil {
		t.Fatal("unreserved runner Pod did not block admission")
	} else if got := err.Error(); got == "" {
		t.Fatal(fmt.Errorf("empty drift error"))
	}
}

func TestFailedPodCleanupRetainsReservation(t *testing.T) {
	build := queuedBuild("a", "alice", 1, 1)
	build.Status.Phase = kovav1.PhaseRunning
	build.Status.RunnerPodName = buildPodName(build.Name)
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(build, testRunnerPod(build)).Build()
	kube := &fakeKube{podClient: base, deleteErr: errors.New("injected delete failure")}
	r := KovaBuildReconciler{Client: base, APIReader: base, Kube: kube, Cfg: admissionConfig()}
	initializeAdmissionForTest(t, &r)
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 1 {
		t.Fatalf("initial ledger = %#v, err=%v", state, err)
	}
	build.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileTerminal(context.Background(), build); err == nil {
		t.Fatal("expected cleanup failure")
	}
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 1 {
		t.Fatalf("ledger released before Pod cleanup: %#v, err=%v", state, err)
	}
	kube.deleteErr = nil
	if _, err := r.reconcileTerminal(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	if _, state, err := r.readReservations(context.Background(), "jobs"); err != nil || len(state.Active) != 0 {
		t.Fatalf("ledger retained after confirmed cleanup: %#v, err=%v", state, err)
	}
}

func TestLateOldLeaderPodCreateCannotEscapeReservation(t *testing.T) {
	scheme := testScheme(t)
	a := queuedBuild("a", "alice", 1, 1)
	b := queuedBuild("b", "bob", 2, 1)
	base := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&kovav1.KovaBuild{}).WithObjects(a, b).Build()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	oldLeader := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{}, Cfg: cfg}
	newLeader := KovaBuildReconciler{Client: base, APIReader: base, Scheme: scheme, Kube: &fakeKube{podClient: base}, Cfg: cfg}
	initializeAdmissionForTest(t, &oldLeader)
	if decision, err := oldLeader.admission(context.Background(), a); err != nil || !decision.Admitted {
		t.Fatalf("old leader reservation = %#v, err=%v", decision, err)
	}
	attempt, err := oldLeader.beginPodCreate(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	// The replacement sees a terminal CR before the old leader's Pod CREATE
	// reaches the API server. No Pod yet is not proof that the old call ended.
	a.Status.Phase = kovav1.PhaseFailed
	if err := base.Status().Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := newLeader.reconcileTerminal(context.Background(), a); err == nil {
		t.Fatal("replacement released an in-flight Pod create")
	}
	var observed kovav1.KovaBuild
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &observed); err != nil {
		t.Fatal(err)
	}
	if !apiMeta.IsStatusConditionTrue(observed.Status.Conditions, admissionRecoveryCondition) {
		t.Fatalf("unknown Pod create was not surfaced in status: %#v", observed.Status.Conditions)
	}
	if decision, err := newLeader.admission(context.Background(), b); err != nil || decision.Admitted {
		t.Fatalf("new build admitted during in-flight Create: %#v, err=%v", decision, err)
	}
	latePod := testRunnerPod(a)
	latePod.Annotations = map[string]string{podCreateAttemptKey: attempt}
	if err := base.Create(context.Background(), latePod); err != nil {
		t.Fatal(err)
	}
	// Observing the exact attempt nonce proves that call committed; cleanup
	// can now remove its marker, delete its Pod, and release the capacity.
	if _, err := newLeader.reconcileTerminal(context.Background(), &observed); err != nil {
		t.Fatal(err)
	}
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: "jobs", Name: "a"}, &observed); err != nil {
		t.Fatal(err)
	}
	if apiMeta.IsStatusConditionTrue(observed.Status.Conditions, admissionRecoveryCondition) {
		t.Fatalf("resolved Pod create still reports recovery required: %#v", observed.Status.Conditions)
	}
	if decision, err := newLeader.admission(context.Background(), b); err != nil || !decision.Admitted {
		t.Fatalf("new build did not receive released capacity: %#v, err=%v", decision, err)
	}
}
