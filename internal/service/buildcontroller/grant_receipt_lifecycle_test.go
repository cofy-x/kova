package buildcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type failActivePatch struct {
	admissiongenesis.CoreAPI
	at, calls int
}

type loseActivePatchResponse struct {
	admissiongenesis.CoreAPI
	at, calls int
}

func (a *loseActivePatchResponse) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.ActiveLedgerName {
		a.calls++
		if a.calls == a.at {
			if _, err := a.CoreAPI.PatchConfigMap(ctx, namespace, name, body); err != nil {
				return nil, err
			}
			return nil, context.DeadlineExceeded
		}
	}
	return a.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
}

type pauseSecondActivePatch struct {
	admissiongenesis.CoreAPI
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
}

func (a *pauseSecondActivePatch) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.ActiveLedgerName {
		a.mu.Lock()
		a.calls++
		second := a.calls == 2
		a.mu.Unlock()
		if second {
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

func (a *failActivePatch) PatchConfigMap(ctx context.Context, namespace, name string, body []byte) (*corev1.ConfigMap, error) {
	if name == admissioncontract.ActiveLedgerName {
		a.calls++
		if a.calls == a.at {
			return nil, context.DeadlineExceeded
		}
	}
	return a.CoreAPI.PatchConfigMap(ctx, namespace, name, body)
}

type countGrantReceiptCreates struct {
	ReceiptConfigMaps
	creates       int
	lose          bool
	writeThenLose bool
}

func (a *countGrantReceiptCreates) Create(ctx context.Context, cm *corev1.ConfigMap, opts metav1.CreateOptions) (*corev1.ConfigMap, error) {
	a.creates++
	if a.lose {
		return nil, context.DeadlineExceeded
	}
	if a.writeThenLose {
		if _, err := a.ReceiptConfigMaps.Create(ctx, cm, opts); err != nil {
			return nil, err
		}
		return nil, context.DeadlineExceeded
	}
	return a.ReceiptConfigMaps.Create(ctx, cm, opts)
}

type loseGrantDeleteResponse struct{ ReceiptConfigMaps }

func (a loseGrantDeleteResponse) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	if err := a.ReceiptConfigMaps.Delete(ctx, name, options); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

func readGenesisGrant(t *testing.T, r *KovaBuildReconciler, build *kovav1.KovaBuild) activeReservation {
	t.Helper()
	_, state, err := r.readReservations(context.Background(), build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	grant, ok := state.Active[reservationKey(build)]
	if !ok {
		t.Fatal("Genesis active charge is absent")
	}
	return grant
}

func terminalGenesisGrant(t *testing.T, r *KovaBuildReconciler, api *controllerGenesisAPI, build *kovav1.KovaBuild) *kovav1.KovaBuild {
	t.Helper()
	ctx := context.Background()
	var current kovav1.KovaBuild
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = kovav1.PhaseSucceeded
	if err := api.Status().Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
		t.Fatal(err)
	}
	if err := r.fenceReservation(ctx, &current); err != nil {
		t.Fatal(err)
	}
	return &current
}

func TestGenesisGrantPinsImmutableReceiptBeforePodAttempt(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	decision, err := r.admission(ctx, build)
	if err != nil || !decision.Admitted || counted.creates != 1 || api.patches != 3 {
		t.Fatalf("grant=%+v err=%v receipt creates=%d ledger patches=%d", decision, err, counted.creates, api.patches)
	}
	grant := readGenesisGrant(t, r, build)
	if grant.GrantNonce == "" || grant.GrantFence == 0 || grant.GrantObservedRV == "" ||
		grant.GrantReceiptUID == "" || grant.GrantReceiptDigest == "" {
		t.Fatalf("grant lacks durable receipt identity: %+v", grant)
	}
	if _, err := r.admission(ctx, build); err != nil || counted.creates != 1 {
		t.Fatalf("replacement reconcile replayed receipt Create: err=%v creates=%d", err, counted.creates)
	}
	if _, err := r.beginPodCreate(ctx, build); err != nil {
		t.Fatalf("pinned grant could not begin Pod attempt: %v", err)
	}
	if counted.creates != 1 {
		t.Fatalf("Pod attempt replayed grant receipt Create: %d", counted.creates)
	}
}

func TestGenesisGrantLostWriteResponsesNeedExactReadback(t *testing.T) {
	for _, mode := range []string{"receipt-create", "receipt-pin"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, api, build := genesisAdmissionFixture(t)
			counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
			r.RecoveryReceipts = counted
			if mode == "receipt-create" {
				counted.writeThenLose = true
			} else {
				r.Genesis.Bootstrap.API = &loseActivePatchResponse{CoreAPI: api, at: 3}
			}
			decision, err := r.admission(ctx, build)
			if err != nil || !decision.Admitted || counted.creates != 1 {
				t.Fatalf("lost response did not resolve by exact read: grant=%+v err=%v creates=%d", decision, err, counted.creates)
			}
			grant := readGenesisGrant(t, r, build)
			if grant.GrantReceiptUID == "" || grant.GrantReceiptDigest == "" {
				t.Fatalf("lost response left unpinned receipt: %+v", grant)
			}
			if _, err := r.admission(ctx, build); err != nil || counted.creates != 1 {
				t.Fatalf("reconcile replayed receipt Create after lost response: err=%v creates=%d", err, counted.creates)
			}
		})
	}
}

func TestGenesisUnarmedGrantNeverCreatesReceiptAndCanCloseByCAS(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	fault := &failActivePatch{CoreAPI: api, at: 2} // First grant CAS succeeds; receipt arm does not.
	r.Genesis.Bootstrap.API = fault
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	if _, err := r.admission(ctx, build); err == nil {
		t.Fatal("unknown grant arm unexpectedly permitted receipt Create")
	}
	grant := readGenesisGrant(t, r, build)
	if grant.GrantNonce == "" || grant.GrantObservedRV != "" || counted.creates != 0 {
		t.Fatalf("unarmed grant lost its no-effect boundary: %+v creates=%d", grant, counted.creates)
	}
	r.Genesis.Bootstrap.API = api
	if _, err := r.admission(ctx, build); !errors.Is(err, errGrantUnpinned) || counted.creates != 0 {
		t.Fatalf("replacement leader armed unpinned grant: err=%v creates=%d", err, counted.creates)
	}
	if _, err := r.beginPodCreate(ctx, build); !errors.Is(err, errGrantUnpinned) {
		t.Fatalf("unarmed grant authorized Pod attempt: %v", err)
	}
	terminal := terminalGenesisGrant(t, r, api, build)
	if err := r.releaseReservation(ctx, terminal); err != nil {
		t.Fatalf("unarmed no-effect grant could not close: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("unarmed grant remained charged after terminal CAS: active=%d err=%v", len(state.Active), err)
	}
}

func TestGenesisLostGrantCASResponseCannotArmReceiptOnRestart(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	r.Genesis.Bootstrap.API = &loseActivePatchResponse{CoreAPI: api, at: 1}
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	if _, err := r.admission(ctx, build); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost grant CAS response did not stop fresh owner: %v", err)
	}
	grant := readGenesisGrant(t, r, build)
	if grant.GrantNonce == "" || grant.GrantObservedRV != "" || counted.creates != 0 {
		t.Fatalf("lost grant CAS response incorrectly armed receipt: %+v creates=%d", grant, counted.creates)
	}
	r.Genesis.Bootstrap.API = api
	if _, err := r.admission(ctx, build); !errors.Is(err, errGrantUnpinned) || counted.creates != 0 {
		t.Fatalf("restart replayed grant receipt Create: err=%v creates=%d", err, counted.creates)
	}
}

func TestGenesisCleanupCASDefeatsPausedGrantReceiptArm(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	paused := &pauseSecondActivePatch{CoreAPI: api, entered: make(chan struct{}), release: make(chan struct{})}
	r.Genesis.Bootstrap.API = paused
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	result := make(chan error, 1)
	go func() {
		_, err := r.admission(ctx, build)
		result <- err
	}()
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		close(paused.release)
		t.Fatal("fresh owner did not pause before its receipt-arm CAS")
	}
	if grant := readGenesisGrant(t, r, build); grant.GrantObservedRV != "" {
		close(paused.release)
		t.Fatalf("paused arm was already durable: %+v", grant)
	}
	terminal := terminalGenesisGrant(t, r, api, build)
	if err := r.releaseReservation(ctx, terminal); err != nil {
		close(paused.release)
		t.Fatalf("cleanup could not close unarmed grant: %v", err)
	}
	close(paused.release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("paused owner armed a grant after cleanup")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paused owner did not return after cleanup CAS")
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 || counted.creates != 0 {
		t.Fatalf("late receipt or active charge after cleanup: active=%d creates=%d err=%v", len(state.Active), counted.creates, err)
	}
}

func TestGenesisArmedUnknownGrantHoldsUntilExactLateReceipt(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts, lose: true}
	r.RecoveryReceipts = counted
	if _, err := r.admission(ctx, build); !errors.Is(err, recoveryreceipt.ErrUnconfirmed) || counted.creates != 1 {
		t.Fatalf("lost receipt Create response was not held: err=%v creates=%d", err, counted.creates)
	}
	grant := readGenesisGrant(t, r, build)
	if grant.GrantObservedRV == "" || grant.GrantReceiptUID != "" {
		t.Fatalf("unknown receipt did not retain armed charge: %+v", grant)
	}
	if _, err := r.admission(ctx, build); !errors.Is(err, errGrantUnpinned) || counted.creates != 1 {
		t.Fatalf("replacement leader replayed unknown receipt Create: err=%v creates=%d", err, counted.creates)
	}
	terminal := terminalGenesisGrant(t, r, api, build)
	if err := r.releaseReservation(ctx, terminal); err == nil {
		t.Fatal("armed grant was released from receipt NotFound")
	}
	if readGenesisGrant(t, r, build).GrantObservedRV == "" {
		t.Fatal("unknown armed grant lost its capacity charge")
	}
	intent, err := r.grantIntent(ctx, terminal, grant)
	if err != nil {
		t.Fatal(err)
	}
	late, err := recoveryreceipt.NewGrantConfigMap(intent)
	if err != nil {
		t.Fatal(err)
	}
	late.UID = types.UID("late-grant-uid")
	if err := api.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseReservation(ctx, terminal); err != nil {
		t.Fatalf("exact late receipt was not settled: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("settled grant retained charge: active=%d err=%v", len(state.Active), err)
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(late), &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("settled late grant receipt still exists: %v", err)
	}
}

func TestGenesisGrantCleanupMarkerSurvivesLostDeleteResponse(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	terminal := terminalGenesisGrant(t, r, api, build)
	original := r.RecoveryReceipts
	r.RecoveryReceipts = loseGrantDeleteResponse{ReceiptConfigMaps: original}
	if err := r.releaseReservation(ctx, terminal); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost receipt Delete response unexpectedly settled: %v", err)
	}
	grant := readGenesisGrant(t, r, build)
	if !grant.GrantCleanupReady || grant.GrantReceiptUID == "" {
		t.Fatalf("receipt Delete occurred before durable cleanup marker: %+v", grant)
	}
	r.RecoveryReceipts = original
	if err := r.releaseReservation(ctx, terminal); err != nil {
		t.Fatalf("restart did not complete marked receipt deletion: %v", err)
	}
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil || len(state.Active) != 0 {
		t.Fatalf("marked cleanup retained active charge: active=%d err=%v", len(state.Active), err)
	}
}

func TestGenesisGrantReceiptReplacementCannotAuthorizePodOrRelease(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	if _, err := r.admission(ctx, build); err != nil {
		t.Fatal(err)
	}
	grant := readGenesisGrant(t, r, build)
	intent, err := r.grantIntent(ctx, build, grant)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := recoveryreceipt.NewGrantConfigMap(intent)
	if err != nil {
		t.Fatal(err)
	}
	var original corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKeyFromObject(expected), &original); err != nil {
		t.Fatal(err)
	}
	if err := api.Delete(ctx, &original); err != nil {
		t.Fatal(err)
	}
	replacement := original.DeepCopy()
	replacement.UID, replacement.ResourceVersion = "replacement-grant-uid", ""
	if err := api.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := r.admission(ctx, build); !errors.Is(err, recoveryreceipt.ErrChanged) {
		t.Fatalf("same-name grant replacement authorized admission: %v", err)
	}
	if _, err := r.beginPodCreate(ctx, build); !errors.Is(err, recoveryreceipt.ErrChanged) {
		t.Fatalf("same-name grant replacement authorized Pod attempt: %v", err)
	}
	terminal := terminalGenesisGrant(t, r, api, build)
	if err := r.releaseReservation(ctx, terminal); !errors.Is(err, recoveryreceipt.ErrChanged) {
		t.Fatalf("same-name grant replacement released capacity: %v", err)
	}
	if readGenesisGrant(t, r, build).GrantReceiptUID != grant.GrantReceiptUID {
		t.Fatal("replacement changed original grant charge")
	}
}
