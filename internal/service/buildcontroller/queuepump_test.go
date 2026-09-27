package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func pumpFixture(t *testing.T, cfg config.Config, builds ...*kovav1.KovaBuild) (*AdmissionPump, *KovaBuildReconciler, client.Client) {
	t.Helper()
	ctx := context.Background()
	cfg.Namespace = "jobs"
	base := crfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&kovav1.KovaBuild{}).Build()
	r := &KovaBuildReconciler{Client: base, APIReader: base, Cfg: cfg}
	initializeAdmissionForTest(t, r)
	queue := queueadmission.Store{Client: base, Reader: base, Namespace: "jobs", GlobalLimit: cfg.MaxQueuedJobs, RequesterLimit: cfg.MaxQueuedJobsPerRequester}
	if err := queue.EnsureInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	for _, build := range builds {
		if err := base.Create(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	return NewAdmissionPump(base, cfg), r, base
}

func pumpRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "jobs", Name: name}}
}

func wakeName(t *testing.T, p *AdmissionPump) string {
	t.Helper()
	select {
	case e := <-p.WakeEvents():
		return e.Object.GetName()
	default:
		t.Fatal("expected a targeted KovaBuild wake")
		return ""
	}
}

func assertNoWake(t *testing.T, p *AdmissionPump) {
	t.Helper()
	select {
	case e := <-p.WakeEvents():
		t.Fatalf("unexpected wake for %s", e.Object.GetName())
	default:
	}
}

type pumpReadCounter struct {
	client.Reader
	activeGets int
	queueGets  int
	buildGets  int
	buildLists int
	podLists   int
}

func (r *pumpReadCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *corev1.ConfigMap:
		if key.Name == reservationConfigMap {
			r.activeGets++
		} else if key.Name == queueadmission.ConfigMapName {
			r.queueGets++
		}
	case *kovav1.KovaBuild:
		r.buildGets++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *pumpReadCounter) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch list.(type) {
	case *kovav1.KovaBuildList:
		r.buildLists++
	case *corev1.PodList:
		r.podLists++
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestAdmissionPumpWakesOnlyFairCandidateAfterRelease(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	blocker := queuedBuild("blocker", "active", 1, 1)
	alice := queuedBuild("alice", "alice", 2, 1)
	bob := queuedBuild("bob", "bob", 3, 1)
	p, r, base := pumpFixture(t, cfg, blocker, alice, bob)
	if decision, err := r.admission(ctx, blocker); err != nil || !decision.Admitted {
		t.Fatalf("blocker grant=%#v err=%v", decision, err)
	}
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	assertNoWake(t, p)
	completeAndReleaseAdmissionForTest(t, r, base, blocker.Name)
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != alice.Name {
		t.Fatalf("first wake=%q, want %q", got, alice.Name)
	}
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	assertNoWake(t, p)
	if decision, err := r.admission(ctx, alice); err != nil || !decision.Admitted {
		t.Fatalf("first candidate grant=%#v err=%v", decision, err)
	}
	completeAndReleaseAdmissionForTest(t, r, base, alice.Name)
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != bob.Name {
		t.Fatalf("next wake=%q, want %q", got, bob.Name)
	}
}

func TestAdmissionPumpGrantEventWakesNextFairRequester(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots, cfg.MaxActiveJobsPerRequester = 2, 2, 1
	aliceFirst := queuedBuild("alice-first", "alice", 1, 1)
	aliceSecond := queuedBuild("alice-second", "alice", 2, 1)
	bob := queuedBuild("bob", "bob", 3, 1)
	p, r, _ := pumpFixture(t, cfg, aliceFirst, aliceSecond, bob)
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != aliceFirst.Name {
		t.Fatalf("first wake=%q, want %q", got, aliceFirst.Name)
	}
	if decision, err := r.admission(ctx, aliceFirst); err != nil || !decision.Admitted {
		t.Fatalf("first grant=%#v err=%v", decision, err)
	}
	// The committed grant changes active-ledger RV. The pump must not keep
	// waking Alice's older backlog while her per-requester slot is full.
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != bob.Name {
		t.Fatalf("next wake=%q, want %q", got, bob.Name)
	}
}

func TestAdmissionPumpAuditRecoversGrantBeforeWakeAndLostWatch(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	first := queuedBuild("first", "alice", 1, 1)
	second := queuedBuild("second", "bob", 2, 1)
	p, r, base := pumpFixture(t, cfg, first, second)
	if decision, err := r.admission(ctx, first); err != nil || !decision.Admitted {
		t.Fatalf("first grant=%#v err=%v", decision, err)
	}
	// The old leader may exit after the CAS grant but before its Pod create.
	// A new leader's startup audit must wake the still-Queued grant holder.
	replacement := NewAdmissionPump(base, p.Cfg)
	if _, err := replacement.Reconcile(ctx, pumpRequest(admissionAuditRequestName)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, replacement); got != first.Name {
		t.Fatalf("recovered grant=%q, want %q", got, first.Name)
	}
	completeAndReleaseAdmissionForTest(t, r, base, first.Name)
	// Simulate losing the active-ConfigMap watch event for release. The
	// periodic audit still selects the next fair candidate.
	if _, err := replacement.Reconcile(ctx, pumpRequest(admissionAuditRequestName)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, replacement); got != second.Name {
		t.Fatalf("audit wake=%q, want %q", got, second.Name)
	}
}

func TestAdmissionPumpSaturatedDepthSkipsFullListsAndTimers(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	blocker := queuedBuild("blocker", "active", 1, 1)
	p, r, base := pumpFixture(t, cfg, blocker)
	for i := 0; i < 1000; i++ {
		build := queuedBuild(fmt.Sprintf("queued-%04d", i), fmt.Sprintf("requester-%d", i), int64(i+2), 1)
		if err := base.Create(ctx, build); err != nil {
			t.Fatal(err)
		}
	}
	if decision, err := r.admission(ctx, blocker); err != nil || !decision.Admitted {
		t.Fatalf("blocker grant=%#v err=%v", decision, err)
	}
	counter := &pumpReadCounter{Reader: base}
	p.Reader = counter
	for i := 0; i < 10; i++ {
		if result, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err != nil || result.RequeueAfter != 0 {
			t.Fatalf("saturated pump result=%#v err=%v", result, err)
		}
	}
	if counter.activeGets != 10 || counter.queueGets != 0 || counter.buildGets != 0 || counter.buildLists != 0 || counter.podLists != 0 {
		t.Fatalf("saturated pump reads: active=%d queue=%d build=%d buildLists=%d podLists=%d",
			counter.activeGets, counter.queueGets, counter.buildGets, counter.buildLists, counter.podLists)
	}
	assertNoWake(t, p)
	queued := queuedBuild("one", "requester", 1002, 1)
	if err := base.Create(ctx, queued); err != nil {
		t.Fatal(err)
	}
	r.APIReader = counter
	result, err := r.Reconcile(ctx, pumpRequest(queued.Name))
	if err != nil || result.RequeueAfter != 0 || result.Requeue {
		t.Fatalf("queued reconcile self-timer=%#v err=%v", result, err)
	}
	var stored kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(queued), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Phase != kovav1.PhaseQueued {
		t.Fatalf("first observation did not persist Queued: %q", stored.Status.Phase)
	}
	// Skipping the queue read while full is safe: the first possible grant
	// still fails closed if that ledger has disappeared in the meantime.
	if err := base.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: queueadmission.ConfigMapName, Namespace: "jobs"}}); err != nil {
		t.Fatal(err)
	}
	completeAndReleaseAdmissionForTest(t, r, base, blocker.Name)
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err == nil {
		t.Fatal("missing queue ledger allowed a possible grant")
	}
	assertNoWake(t, p)
}

func TestAdmissionPumpAuditBoundsLargeActiveGrantScanAndWakesOnlyUnfinished(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 81, 81
	builds := make([]*kovav1.KovaBuild, 0, 81)
	for i := 0; i < 80; i++ {
		builds = append(builds, queuedBuild(fmt.Sprintf("running-%03d", i), fmt.Sprintf("requester-%03d", i), int64(i+1), 1))
	}
	pending := queuedBuild("z-pending", "pending", 81, 1)
	builds = append(builds, pending)
	p, r, base := pumpFixture(t, cfg, builds...)
	for _, item := range builds[:80] {
		var current kovav1.KovaBuild
		if err := base.Get(ctx, client.ObjectKeyFromObject(item), &current); err != nil {
			t.Fatal(err)
		}
		current.Status.Phase = kovav1.PhaseRunning
		if err := base.Status().Update(ctx, &current); err != nil {
			t.Fatal(err)
		}
	}
	var queued kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(pending), &queued); err != nil {
		t.Fatal(err)
	}
	queued.Status.Phase = kovav1.PhaseQueued
	if err := base.Status().Update(ctx, &queued); err != nil {
		t.Fatal(err)
	}
	cm, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range builds {
		state.Active[reservationKey(item)] = activeReservation{BuildName: item.Name, Requester: requesterKey(item), Slots: 1}
	}
	if err := r.writeReservations(ctx, cm, state); err != nil {
		t.Fatal(err)
	}
	counter := &pumpReadCounter{Reader: base}
	p.Reader = counter
	for i := 0; i < 2; i++ {
		before := counter.buildGets
		result, err := p.Reconcile(ctx, pumpRequest(admissionAuditRequestName))
		if err != nil {
			t.Fatal(err)
		}
		if result.RequeueAfter != admissionAuditContinuation {
			t.Fatalf("audit %d continuation=%s, want %s", i, result.RequeueAfter, admissionAuditContinuation)
		}
		if got := counter.buildGets - before; got != admissionAuditGrantBatch {
			t.Fatalf("audit %d inspected %d grants, want bounded %d", i, got, admissionAuditGrantBatch)
		}
		assertNoWake(t, p)
	}
	result, err := p.Reconcile(ctx, pumpRequest(admissionAuditRequestName))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("completed audit still requested a continuation: %s", result.RequeueAfter)
	}
	if got := wakeName(t, p); got != pending.Name {
		t.Fatalf("unfinished grant wake=%q, want %q", got, pending.Name)
	}
	assertNoWake(t, p)
	if counter.buildGets != 81 || counter.queueGets != 0 || counter.buildLists != 0 || counter.podLists != 0 {
		t.Fatalf("unbounded saturated audit reads: builds=%d queue=%d buildLists=%d podLists=%d",
			counter.buildGets, counter.queueGets, counter.buildLists, counter.podLists)
	}
}

func TestAdmissionPumpAuditWakesFencedCleanup(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	closing := queuedBuild("closing", "alice", 1, 1)
	p, r, base := pumpFixture(t, cfg, closing)
	var current kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(closing), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = kovav1.PhaseRunning
	if err := base.Status().Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	cm, state, err := r.readReservations(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	state.Active[reservationKey(closing)] = activeReservation{BuildName: closing.Name, Requester: requesterKey(closing), Slots: 1, Closing: true}
	if err := r.writeReservations(ctx, cm, state); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reconcile(ctx, pumpRequest(admissionAuditRequestName)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != closing.Name {
		t.Fatalf("fenced cleanup wake=%q, want %q", got, closing.Name)
	}
}

func TestAdmissionPumpKeepsDriftAndOrphanFailClosed(t *testing.T) {
	ctx := context.Background()
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	bad := queuedBuild("bad", "alice", 1, 1)
	bad.Annotations = map[string]string{queueadmission.IntentAnnotation: "00112233445566778899aabbccddeeff"}
	good := queuedBuild("good", "bob", 2, 1)
	p, _, base := pumpFixture(t, cfg, bad, good)
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("queue drift error=%v", err)
	}
	if got := wakeName(t, p); got != bad.Name {
		t.Fatalf("drift wake=%q, want %q", got, bad.Name)
	}
	assertNoWake(t, p)
	if err := base.Delete(ctx, bad); err != nil {
		t.Fatal(err)
	}
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "orphan", Namespace: "jobs", Labels: map[string]string{"kova.cofy.dev/build-id": "absent"},
	}}
	if err := base.Create(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reconcile(ctx, pumpRequest(reservationConfigMap)); err == nil {
		t.Fatal("orphan Pod did not block admission")
	}
	assertNoWake(t, p)
}

func TestAdmissionPumpSkipsCancelledAndDeletingQueueHeads(t *testing.T) {
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	cancelled := queuedBuild("cancelled", "alice", 1, 1)
	cancelled.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: time.Now().Format(time.RFC3339)}
	deleting := queuedBuild("deleting", "alice", 2, 1)
	stamp := metav1.Now()
	deleting.DeletionTimestamp = &stamp
	eligible := queuedBuild("eligible", "bob", 3, 1)
	state := reservationState{Active: map[string]activeReservation{}}
	if got := nextAdmissionCandidate([]kovav1.KovaBuild{*cancelled, *deleting, *eligible}, state, cfg); got == nil || got.Name != eligible.Name {
		t.Fatalf("candidate=%v, want eligible", got)
	}
}

func TestAdmissionPumpBuildEventFilterAndLeaderAuditKick(t *testing.T) {
	filter := admissionBuildEvents("jobs")
	build := queuedBuild("candidate", "alice", 1, 1)
	if !filter.Create(event.CreateEvent{Object: build}) {
		t.Fatal("new build did not trigger admission pump")
	}
	queued := build.DeepCopy()
	queued.Status.Phase = kovav1.PhaseQueued
	if !filter.Update(event.UpdateEvent{ObjectOld: build, ObjectNew: queued}) {
		t.Fatal("first Queued status did not trigger admission pump")
	}
	running := queued.DeepCopy()
	running.Status.Phase = kovav1.PhaseRunning
	verifying := running.DeepCopy()
	verifying.Status.Phase = kovav1.PhaseVerifying
	if filter.Update(event.UpdateEvent{ObjectOld: running, ObjectNew: verifying}) {
		t.Fatal("active phase transition caused an unnecessary pump scan")
	}
	cancelled := queued.DeepCopy()
	cancelled.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: time.Now().Format(time.RFC3339)}
	if !filter.Update(event.UpdateEvent{ObjectOld: queued, ObjectNew: cancelled}) {
		t.Fatal("queued cancellation did not trigger admission pump")
	}
	kick := make(chan event.GenericEvent, 1)
	ticker := &admissionAuditTicker{namespace: "jobs", kick: kick}
	if !ticker.NeedLeaderElection() {
		t.Fatal("audit ticker must be leader-only")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ticker.Start(ctx) }()
	select {
	case e := <-kick:
		if e.Object.GetNamespace() != "jobs" || e.Object.GetName() != admissionAuditRequestName {
			t.Fatalf("startup audit key=%s/%s", e.Object.GetNamespace(), e.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("leader startup did not enqueue an audit")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
