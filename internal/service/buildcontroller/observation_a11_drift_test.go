package buildcontroller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Another same-original queue writer changes mutable intent data between the
// verifier's GET and grantIntent's independent Lookup. Both GETs must remain
// fresh inside a single read-only Observation.
type a11SecondQueueChange struct {
	client.Reader
	actor  client.Client
	mutate func(*corev1.ConfigMap) error
	reads  int
}

func (r *a11SecondQueueChange) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*corev1.ConfigMap); ok && key.Name == queueadmission.ConfigMapName {
		r.reads++
		if r.reads == 2 {
			var current corev1.ConfigMap
			if err := r.actor.Get(ctx, key, &current); err != nil {
				return err
			}
			if err := r.mutate(&current); err != nil {
				return err
			}
			if err := r.actor.Update(ctx, &current); err != nil {
				return err
			}
		}
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestA11ObservationRetainsFreshQueueVerifyThenLookup(t *testing.T) {
	for _, mode := range []string{"nonce", "receipt-uid", "receipt-digest", "cleanup-kind"} {
		for _, observed := range []bool{false, true} {
			path := "ordinary"
			if observed {
				path = "observed"
			}
			t.Run(mode+"/"+path, func(t *testing.T) { testA11ObservationSecondQueueChange(t, mode, observed) })
		}
	}
}

func testA11ObservationSecondQueueChange(t *testing.T, mode string, observed bool) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	queue := r.queueStoreForNamespace(build.Namespace)
	entry, fresh, err := queue.Reserve(ctx, build)
	if err != nil || !fresh {
		t.Fatalf("queue reserve: fresh=%t err=%v", fresh, err)
	}
	intent, err := queue.ReceiptIntent(build, entry)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := recoveryreceipt.RecordQueueOnce(ctx, r.RecoveryReceipts, intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.PinReceipt(ctx, build, entry, witness); err != nil {
		t.Fatal(err)
	}
	build.Annotations = map[string]string{queueadmission.IntentAnnotation: entry.Nonce,
		queueadmission.ReceiptUIDAnnotation:    witness.ReceiptUID,
		queueadmission.ReceiptDigestAnnotation: witness.DataDigest}
	if err := api.Update(ctx, build); err != nil {
		t.Fatal(err)
	}
	if decision, err := r.admission(ctx, build); err != nil || !decision.Admitted {
		t.Fatalf("queued grant: decision=%+v err=%v", decision, err)
	}
	_, active, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	reader := &a11SecondQueueChange{Reader: api.Client, actor: api.Client,
		mutate: func(current *corev1.ConfigMap) error {
			old := current.Data[queueadmission.LedgerDataKey]
			var changed string
			switch mode {
			case "nonce":
				changed = strings.Replace(old, entry.Nonce, strings.Repeat("e", 32), 1)
			case "receipt-uid":
				changed = strings.Replace(old, witness.ReceiptUID, "receipt-"+strings.Repeat("f", 32), 1)
			case "receipt-digest":
				changed = strings.Replace(old, witness.DataDigest, "sha256:"+strings.Repeat("f", 64), 1)
			case "cleanup-kind":
				changed = strings.Replace(old, `"receiptDigest":"`+witness.DataDigest+`"`, `"receiptDigest":"`+witness.DataDigest+`","cleanupKind":"terminal"`, 1)
			}
			if changed == old {
				return errors.New("test failed to change the second queue observation")
			}
			current.Data[queueadmission.LedgerDataKey] = changed
			return nil
		}}
	r.APIReader = reader
	if observed {
		err = r.Genesis.Observe(ctx, func(observation *admissiongenesis.Observation) error {
			_, readErr := r.grantIntentWithObservation(nil, build, active.Active[reservationKey(build)], observation)
			return readErr
		})
	} else {
		_, err = r.grantIntent(ctx, build, active.Active[reservationKey(build)])
	}
	if !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("second fresh queue read failed to detect %s drift: %v", mode, err)
	}
	if reader.reads != 2 {
		t.Fatalf("Verify and Lookup were collapsed to %d queue GETs", reader.reads)
	}
}

type a11QueueExitDrift struct {
	client.Reader
	reads  int
	revoke func()
}

func (r *a11QueueExitDrift) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if _, ok := object.(*corev1.ConfigMap); ok && key.Name == queueadmission.ConfigMapName {
		r.reads++
		if r.reads == 2 {
			r.revoke()
		}
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestA11ObservationExitRevocationBlocksReceiptCreateAndHoldsCharge(t *testing.T) {
	ctx := context.Background()
	r, api, build := genesisAdmissionFixture(t)
	cm, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	state.Fence = 1
	grant := activeReservation{BuildName: build.Name, Requester: requesterKey(build), Slots: 1,
		GrantNonce: strings.Repeat("a", 32), GrantFence: 1}
	state.Active[reservationKey(build)] = grant
	if err := r.writeReservations(ctx, cm, state); err != nil {
		t.Fatal(err)
	}
	counted := &countGrantReceiptCreates{ReceiptConfigMaps: r.RecoveryReceipts}
	r.RecoveryReceipts = counted
	revoked := false
	r.Genesis.ReceiptCheck = func(context.Context) error {
		if revoked {
			return admissiongenesis.ErrChanged
		}
		return nil
	}
	reader := &a11QueueExitDrift{Reader: api.Client, revoke: func() { revoked = true }}
	r.APIReader = reader
	if err := r.finishFreshGrant(ctx, build, grant); !errors.Is(err, admissiongenesis.ErrChanged) {
		t.Fatalf("exit revocation did not block receipt Create: %v", err)
	}
	if reader.reads != 2 || counted.creates != 0 {
		t.Fatalf("read-only exit failed to fence Create: queueGET=%d receiptCreate=%d", reader.reads, counted.creates)
	}
	var held corev1.ConfigMap
	if err := api.Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: AdmissionLedgerName}, &held); err != nil {
		t.Fatal(err)
	}
	final, err := decodeReservations(&held)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := final.Active[reservationKey(build)]
	if !ok || entry.GrantObservedRV == "" || entry.GrantReceiptUID != "" || entry.Closing {
		t.Fatalf("unknown receipt outcome released or completed the held arm: %+v", entry)
	}
}

type a11WrongLedgerRead struct {
	client.Reader
	name string
	mode string
}

func (r a11WrongLedgerRead) Get(ctx context.Context, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, object, opts...); err != nil {
		return err
	}
	if cm, ok := object.(*corev1.ConfigMap); ok && key.Name == r.name {
		if r.mode == "uid" {
			cm.UID = types.UID("same-name-replacement")
		} else if r.name == AdmissionLedgerName {
			cm.Data[AdmissionLedgerDataKey] = "invalid"
		} else {
			cm.Data[queueadmission.LedgerDataKey] = "invalid"
		}
	}
	return nil
}

func TestA11ObservationEachFreshLedgerGetChecksOriginalUIDAndSchema(t *testing.T) {
	for _, ledger := range []string{AdmissionLedgerName, queueadmission.ConfigMapName} {
		for _, mode := range []string{"uid", "data"} {
			t.Run(ledger+"-"+mode, func(t *testing.T) {
				r, api, build := genesisAdmissionFixture(t)
				r.APIReader = a11WrongLedgerRead{Reader: api.Client, name: ledger, mode: mode}
				err := r.Genesis.Observe(t.Context(), func(observation *admissiongenesis.Observation) error {
					if ledger == AdmissionLedgerName {
						_, _, readErr := r.readReservationsWithObservation(nil, build.Namespace, observation)
						return readErr
					}
					return r.queueStoreForNamespace(build.Namespace).CheckReadyObserved(observation)
				})
				if err == nil {
					t.Fatal("fresh same-name replacement or invalid ledger was accepted")
				}
			})
		}
	}
}
