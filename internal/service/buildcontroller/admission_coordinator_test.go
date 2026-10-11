package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func coordinatorPermit(t *testing.T, g *AdmissionCoordinator) *freshGrantPermit {
	t.Helper()
	p, err := g.tryBegin(t.Context(), "jobs")
	if err != nil || p == nil {
		t.Fatalf("local permit: permit=%v err=%v", p, err)
	}
	return p
}

func assertCoordinatorHeld(t *testing.T, g *AdmissionCoordinator, permit *freshGrantPermit) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.holder != permit {
		t.Fatal("local interval unexpectedly changed its holder")
	}
}

func assertNoCoordinatorRelease(t *testing.T, g *AdmissionCoordinator) {
	t.Helper()
	select {
	case <-g.ReleaseEvents():
		t.Fatal("release without pending demand generated a wake")
	default:
	}
}

func coordinatorPump(r *KovaBuildReconciler) *AdmissionPump {
	p := NewAdmissionPump(r.APIReader, r.Cfg)
	p.Genesis, p.AdmissionCoordinator = r.Genesis, r.AdmissionCoordinator
	return p
}

func TestAdmissionCoordinatorCoalescesThousandBusyCallers(t *testing.T) {
	g := NewAdmissionCoordinator("jobs")
	first := coordinatorPermit(t, g)
	first.release()
	assertNoCoordinatorRelease(t, g)
	holder := coordinatorPermit(t, g)
	var workers sync.WaitGroup
	failures := make(chan error, 1000)
	for range 1000 {
		workers.Go(func() {
			p, err := g.tryBegin(t.Context(), "jobs")
			if p != nil || err != nil {
				failures <- fmt.Errorf("busy try returned permit=%v err=%v", p, err)
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	assertCoordinatorHeld(t, g, holder)
	holder.release()
	select {
	case e := <-g.ReleaseEvents():
		if e.Object.GetNamespace() != "jobs" || e.Object.GetName() != admissionReleaseRequestName {
			t.Fatal("release did not target the independent Pump key")
		}
	default:
		t.Fatal("coalesced demand lost its release event")
	}
	assertNoCoordinatorRelease(t, g)
	successor := coordinatorPermit(t, g)
	holder.release() // A delayed duplicate cannot release the new holder.
	assertCoordinatorHeld(t, g, successor)
	successor.release()
	assertNoCoordinatorRelease(t, g)
}

func TestAdmissionCoordinatorCancellationAndScope(t *testing.T) {
	g := NewAdmissionCoordinator("jobs")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if permit, err := g.tryBegin(ctx, "jobs"); permit != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled try: permit=%v err=%v", permit, err)
	}
	if _, err := g.tryBegin(t.Context(), "replacement"); err == nil {
		t.Fatal("wrong Namespace obtained a permit")
	}
	var zero AdmissionCoordinator
	if _, err := zero.tryBegin(t.Context(), "jobs"); err == nil {
		t.Fatal("unconfigured zero coordinator obtained a permit")
	}
	// Cancel while the try is racing for the short state lock. It must
	// recheck context after obtaining the lock, before becoming the holder.
	ctx, cancel = context.WithCancel(t.Context())
	g.mu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		permit, err := g.tryBegin(ctx, "jobs")
		if permit != nil {
			permit.release()
			err = fmt.Errorf("cancelled racing try obtained a permit")
		}
		done <- err
	}()
	<-started
	cancel()
	g.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("racing cancellation: %v", err)
	}
	assertCoordinatorHeld(t, g, nil)
	assertNoCoordinatorRelease(t, g)
}

func TestAdmissionCoordinatorReleaseDemandRaceAndFullChannel(t *testing.T) {
	for range 100 {
		g := NewAdmissionCoordinator("jobs")
		holder := coordinatorPermit(t, g)
		busyResult := make(chan bool, 1)
		go func() {
			busy, _, err := g.deferIfBusy("jobs")
			if err != nil {
				t.Error(err)
			}
			busyResult <- busy
		}()
		holder.release()
		if <-busyResult {
			select {
			case <-g.ReleaseEvents():
			default:
				t.Fatal("release raced an observed busy interval and lost its demand")
			}
		}
		assertCoordinatorHeld(t, g, nil)
	}
	g := NewAdmissionCoordinator("jobs")
	for range 3 {
		holder := coordinatorPermit(t, g)
		if busy, _, err := g.deferIfBusy("jobs"); !busy || err != nil {
			t.Fatal("Pump did not atomically leave its demand")
		}
		holder.release()
	}
	if len(g.release) != 1 {
		t.Fatalf("full release channel did not coalesce: %d", len(g.release))
	}
	busy, generation, err := g.deferIfBusy("jobs")
	if busy || generation != 3 || err != nil {
		t.Fatalf("coalesced kick lost the latest state: busy=%t generation=%d err=%v", busy, generation, err)
	}
}

func TestGenesisAdmissionCoordinatorBusyHasNoEffectsOrTimers(t *testing.T) {
	r, api, first := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	holder := coordinatorPermit(t, r.AdmissionCoordinator)
	defer holder.release()
	builds := []*kovav1.KovaBuild{first}
	for i := 1; i < 1000; i++ {
		build := queuedBuild(fmt.Sprintf("busy-%04d", i), "bob", int64(i+1), 1)
		if err := api.Create(t.Context(), build); err != nil {
			t.Fatal(err)
		}
		builds = append(builds, build)
	}
	counted := &capacityCountingClient{Client: api.Client}
	r.Client, r.APIReader = counted, counted
	for _, build := range builds {
		result, err := r.startBuild(t.Context(), build)
		if err != nil || result != (ctrl.Result{}) {
			t.Fatalf("busy start: result=%#v err=%v", result, err)
		}
	}
	if counted.mutations != 0 || api.patches != 0 {
		t.Fatalf("busy jobs caused effects: client=%d ledger=%d", counted.mutations, api.patches)
	}
	var current kovav1.KovaBuild
	if err := api.Get(t.Context(), client.ObjectKeyFromObject(first), &current); err != nil || current.Status.Phase != "" || len(current.Status.Conditions) != 0 {
		t.Fatalf("local defer was written as capacity status: phase=%s err=%v", current.Status.Phase, err)
	}
	assertCoordinatorHeld(t, r.AdmissionCoordinator, holder)
}

func TestGenesisAdmissionCoordinatorDoesNotSwallowExistingOrSaturated(t *testing.T) {
	cfg := admissionConfig()
	cfg.MaxActiveJobs, cfg.WorkerSlots = 1, 1
	r, api, build := genesisAdmissionFixtureWithConfig(t, cfg)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	if decision, err := r.admission(t.Context(), build); err != nil || !decision.Admitted {
		t.Fatalf("initial grant: decision=%#v err=%v", decision, err)
	}
	holder := coordinatorPermit(t, r.AdmissionCoordinator)
	defer holder.release()
	if decision, err := r.admission(t.Context(), build); err != nil || !decision.Admitted || decision.Deferred != 0 {
		t.Fatalf("existing grant was locally deferred: decision=%#v err=%v", decision, err)
	}
	other := queuedBuild("saturated", "bob", 2, 1)
	if err := api.Create(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	if decision, err := r.admission(t.Context(), other); err != nil || decision.Admitted || decision.Deferred != 0 || decision.Message != "waiting for an active job slot" {
		t.Fatalf("durable saturation was swallowed: decision=%#v err=%v", decision, err)
	}
	// Pod arming/completion and runner grant observation remain outside the
	// local interval, even while an unrelated fresh grant owns its permit.
	current, _ := persistGenesisStartingWitness(t, r, api, build)
	if _, _, err := r.directGenesisGrant(t.Context(), current); err != nil {
		t.Fatalf("Pod/runtime path waited for local fresh grant: %v", err)
	}
	assertCoordinatorHeld(t, r.AdmissionCoordinator, holder)
}

func TestGenesisAdmissionCoordinatorLeavesWorkersForCancelDelete(t *testing.T) {
	r, api, first := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	holder := coordinatorPermit(t, r.AdmissionCoordinator)
	defer holder.release()
	builds := []*kovav1.KovaBuild{first}
	for i := 1; i < 8; i++ {
		build := queuedBuild(fmt.Sprintf("worker-%d", i), "bob", int64(i+1), 1)
		if err := api.Create(t.Context(), build); err != nil {
			t.Fatal(err)
		}
		builds = append(builds, build)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, len(builds))
	for _, build := range builds {
		go func() {
			result, err := r.startBuild(ctx, build)
			if result != (ctrl.Result{}) {
				err = fmt.Errorf("local waiter scheduled itself: %#v", result)
			}
			done <- err
		}()
	}
	for range builds {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	first.Annotations = map[string]string{kovav1.CancellationRequestedAnnotation: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := api.Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(first)}); err != nil {
		t.Fatalf("cancellation waited for local permit: %v", err)
	}
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(first), &current); err != nil || current.Status.Phase != kovav1.PhaseCancelled {
		t.Fatalf("cancellation did not progress: phase=%s err=%v", current.Status.Phase, err)
	}
	deleted := builds[1]
	if err := api.Delete(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(deleted)}); err != nil {
		t.Fatalf("deletion waited for local permit: %v", err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(deleted), &kovav1.KovaBuild{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deletion did not finish its finalizer while the gate was held: %v", err)
	}
	assertCoordinatorHeld(t, r.AdmissionCoordinator, holder)
}

func TestGenesisAdmissionCoordinatorUnknownGrantRemainsHeld(t *testing.T) {
	for _, stage := range []string{"grant-cas", "arm-cas", "receipt-create", "pin-cas"} {
		t.Run(stage, func(t *testing.T) {
			r, api, build := genesisAdmissionFixture(t)
			r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
			counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
			r.RecoveryReceipts = counted
			switch stage {
			case "grant-cas":
				r.Genesis.Bootstrap.API = &loseActivePatchResponse{CoreAPI: api, at: 1}
			case "arm-cas":
				r.Genesis.Bootstrap.API = &loseActivePatchResponse{CoreAPI: api, at: 2}
			case "receipt-create":
				counted.lose = true
			case "pin-cas":
				r.Genesis.Bootstrap.API = &failActivePatch{CoreAPI: api, at: 3}
			}
			if decision, err := r.admission(t.Context(), build); err == nil || decision.Admitted {
				t.Fatalf("unknown grant did not stop: decision=%#v err=%v", decision, err)
			}
			assertCoordinatorHeld(t, r.AdmissionCoordinator, nil)
			if busy, generation, err := r.AdmissionCoordinator.deferIfBusy("jobs"); busy || generation != 1 || err != nil {
				t.Fatalf("unknown did not close precisely one local interval: busy=%t generation=%d err=%v", busy, generation, err)
			}
			expectedCreates := 0
			if stage == "receipt-create" || stage == "pin-cas" {
				expectedCreates = 1
			}
			if counted.creates != expectedCreates {
				t.Fatalf("unknown stage changed receipt effects: got=%d want=%d", counted.creates, expectedCreates)
			}
			r.Genesis.Bootstrap.API = api
			before := counted.creates
			if _, err := r.admission(t.Context(), build); !errors.Is(err, errGrantUnpinned) || counted.creates != before {
				t.Fatalf("local release replayed receipt: err=%v creates=%d before=%d", err, counted.creates, before)
			}
			if grant := readGenesisGrant(t, r, build); grant.GrantNonce == "" || grant.GrantReceiptUID != "" {
				t.Fatal("local release discarded or upgraded the uncertain durable grant")
			}
			coordinatorPermit(t, r.AdmissionCoordinator).release()
			assertNoCoordinatorRelease(t, r.AdmissionCoordinator)
		})
	}
}

func TestGenesisAdmissionPumpReleaseWakesSameUIDAndRVOnce(t *testing.T) {
	r, _, build := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	p := coordinatorPump(r)
	if _, err := p.Reconcile(t.Context(), pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != build.Name {
		t.Fatal("initial wake selected the wrong build")
	}
	originalRV := p.lastWakeLedgerRV
	holder := coordinatorPermit(t, r.AdmissionCoordinator)
	if decision, err := r.admission(t.Context(), build); err != nil || decision.Deferred != admissionLocalDeferred {
		t.Fatalf("candidate did not defer: decision=%#v err=%v", decision, err)
	}
	holder.release()
	<-r.AdmissionCoordinator.ReleaseEvents()
	if result, err := p.Reconcile(t.Context(), pumpRequest(admissionReleaseRequestName)); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("release caused audit/timer: result=%#v err=%v", result, err)
	}
	if got := wakeName(t, p); got != build.Name || p.lastWakeLedgerRV != originalRV || p.lastWakeReleaseGeneration != 1 {
		t.Fatal("same UID/RV release was suppressed")
	}
	if _, err := p.Reconcile(t.Context(), pumpRequest(admissionReleaseRequestName)); err != nil {
		t.Fatal(err)
	}
	assertNoWake(t, p)
	// A rejected/no-effect interval with no parked demand must not create a
	// self-sustaining release -> wake -> rejected grant -> release loop.
	coordinatorPermit(t, r.AdmissionCoordinator).release()
	assertNoCoordinatorRelease(t, r.AdmissionCoordinator)
}

func TestGenesisAdmissionPumpBusyRegistersDemandAndLostKickAudits(t *testing.T) {
	r, _, build := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	p := coordinatorPump(r)
	for _, name := range []string{reservationConfigMap, admissionReleaseRequestName} {
		holder := coordinatorPermit(t, r.AdmissionCoordinator)
		if result, err := p.Reconcile(t.Context(), pumpRequest(name)); err != nil || result != (ctrl.Result{}) {
			t.Fatalf("busy Pump: result=%#v err=%v", result, err)
		}
		assertNoWake(t, p)
		holder.release()
		select {
		case <-r.AdmissionCoordinator.ReleaseEvents(): // Simulate a lost event.
		default:
			t.Fatal("Pump consumed all held-interval events without registering demand")
		}
		if _, err := p.Reconcile(t.Context(), pumpRequest(admissionAuditRequestName)); err != nil {
			t.Fatal(err)
		}
		if got := wakeName(t, p); got != build.Name {
			t.Fatal("original audit failed to recover a lost local release kick")
		}
	}
}

func TestGenesisAdmissionPumpReleaseDoesNotScanActiveGrants(t *testing.T) {
	r, api, first := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	if decision, err := r.admission(t.Context(), first); err != nil || !decision.Admitted {
		t.Fatalf("grant: decision=%#v err=%v", decision, err)
	}
	other := queuedBuild("next", "bob", 2, 1)
	if err := api.Create(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	p := coordinatorPump(r)
	if result, err := p.Reconcile(t.Context(), pumpRequest(admissionReleaseRequestName)); err != nil || result != (ctrl.Result{}) {
		t.Fatalf("release started an audit: result=%#v err=%v", result, err)
	}
	if got := wakeName(t, p); got != other.Name {
		t.Fatalf("release woke an already granted build instead of the next fair candidate: %s", got)
	}
	assertNoWake(t, p)
	if p.auditRemaining != 0 || p.auditCursor != "" {
		t.Fatal("local releases changed the bounded periodic active-grant audit")
	}
	if decision, err := r.admission(t.Context(), other); err != nil || !decision.Admitted {
		t.Fatalf("next fair grant: decision=%#v err=%v", decision, err)
	}
	_, state, err := r.readReservations(t.Context(), r.Cfg.Namespace)
	if err != nil || len(state.Active) != 2 {
		t.Fatalf("local coordination changed durable capacity: active=%d err=%v", len(state.Active), err)
	}
	if state.LastGrantedRequesterHash != queueadmission.HashRequester("bob") {
		t.Fatal("local wake generation replaced the durable fairness cursor")
	}
}

func TestGenesisAdmissionPumpAuditStillWakesExistingGrantWhileBusy(t *testing.T) {
	r, _, build := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	if decision, err := r.admission(t.Context(), build); err != nil || !decision.Admitted {
		t.Fatalf("grant: decision=%#v err=%v", decision, err)
	}
	holder := coordinatorPermit(t, r.AdmissionCoordinator)
	defer holder.release()
	p := coordinatorPump(r)
	if _, err := p.Reconcile(t.Context(), pumpRequest(admissionAuditRequestName)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != build.Name {
		t.Fatal("busy gate suppressed an unfinished durable grant's audit")
	}
	assertCoordinatorHeld(t, r.AdmissionCoordinator, holder)
}

type coordinatorSerializedCore struct {
	admissiongenesis.CoreAPI
	mu sync.Mutex
}

func (a *coordinatorSerializedCore) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
}

type coordinatorReceiptCounter struct {
	ReceiptConfigMaps
	mu      sync.Mutex
	creates int
}

func (a *coordinatorReceiptCounter) Create(ctx context.Context, cm *corev1.ConfigMap, options metav1.CreateOptions) (*corev1.ConfigMap, error) {
	a.mu.Lock()
	a.creates++
	a.mu.Unlock()
	return a.ReceiptConfigMaps.Create(ctx, cm, options)
}

func TestGenesisAdmissionCoordinatorSeparateReplicasRetainCAS(t *testing.T) {
	r, api, build := genesisAdmissionFixture(t)
	r.AdmissionCoordinator = NewAdmissionCoordinator(r.Cfg.Namespace)
	r.Genesis.Bootstrap.API = &coordinatorSerializedCore{CoreAPI: api}
	counted := &coordinatorReceiptCounter{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	other := &KovaBuildReconciler{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme, Cfg: r.Cfg,
		Genesis: r.Genesis, RecoveryReceipts: r.RecoveryReceipts, AdmissionCoordinator: NewAdmissionCoordinator(r.Cfg.Namespace)}
	results := make(chan admissionDecision, 2)
	var workers sync.WaitGroup
	for _, replica := range []*KovaBuildReconciler{r, other} {
		workers.Go(func() {
			decision, _ := replica.admission(t.Context(), build)
			results <- decision
		})
	}
	workers.Wait()
	close(results)
	admitted := 0
	for decision := range results {
		if decision.Admitted {
			admitted++
		}
	}
	if admitted == 0 {
		t.Fatal("separate local coordinators prevented every durable grant")
	}
	_, state, err := r.readReservations(t.Context(), r.Cfg.Namespace)
	if err != nil || len(state.Active) != 1 {
		t.Fatalf("separate coordinators overbooked: active=%d err=%v", len(state.Active), err)
	}
	var receipts corev1.ConfigMapList
	if err := api.List(t.Context(), &receipts, client.InNamespace(r.Genesis.Bootstrap.Receipt.Contract.ReceiptNamespace)); err != nil || len(receipts.Items) != 1 {
		t.Fatalf("cross-replica CAS replayed the receipt Create: receipts=%d err=%v", len(receipts.Items), err)
	}
	grant := state.Active[reservationKey(build)]
	if grant.GrantReceiptUID == "" || grant.Slots != 1 {
		t.Fatal("separate coordinators lost the pinned durable grant")
	}
	if counted.creates != 1 {
		t.Fatalf("separate coordinators issued %d receipt Create attempts", counted.creates)
	}
}

func TestLegacyAdmissionIgnoresCoordinator(t *testing.T) {
	build := queuedBuild("legacy", "alice", 1, 1)
	p, r, _ := pumpFixture(t, admissionConfig(), build)
	g := NewAdmissionCoordinator("jobs")
	r.AdmissionCoordinator, p.AdmissionCoordinator = g, g
	holder := coordinatorPermit(t, g)
	defer holder.release()
	if _, err := p.Reconcile(t.Context(), pumpRequest(reservationConfigMap)); err != nil {
		t.Fatal(err)
	}
	if got := wakeName(t, p); got != build.Name {
		t.Fatal("legacy Pump waited for a Genesis-only coordinator")
	}
	if decision, err := r.admission(t.Context(), build); err != nil || !decision.Admitted || decision.Deferred != 0 {
		t.Fatalf("legacy grant changed: decision=%#v err=%v", decision, err)
	}
	assertCoordinatorHeld(t, g, holder)
}
