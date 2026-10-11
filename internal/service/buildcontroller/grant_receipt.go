package buildcontroller

import (
	"context"
	"errors"
	"fmt"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ReceiptConfigMaps must be a typed, direct client scoped to the immutable
// dedicated receipt Namespace. It makes one wire attempt for each Create or
// Delete; a retry is never inferred from a failed readback.
type ReceiptConfigMaps interface {
	recoveryreceipt.ConfigMaps
	Delete(context.Context, string, metav1.DeleteOptions) error
}

var errGrantUnpinned = errors.New("active grant receipt is not pinned; recovery observation only")

func (r *KovaBuildReconciler) checkGrantReceiptNamespace(ctx context.Context, namespace string) error {
	return r.checkGrantReceiptNamespaceWithObservation(ctx, namespace, nil)
}

func (r *KovaBuildReconciler) checkGrantReceiptNamespaceWithObservation(ctx context.Context, namespace string, observation *admissiongenesis.Observation) error {
	if r.Genesis == nil || r.RecoveryReceipts == nil {
		return fmt.Errorf("Genesis grant requires a direct receipt ConfigMap client")
	}
	if observation != nil {
		if err := observation.AssertNoOrdinaryContext(r.Genesis, ctx); err != nil {
			return err
		}
	}
	queue := r.queueStoreForNamespace(namespace)
	if observation != nil {
		return queue.CheckReceiptNamespaceObserved(observation)
	}
	return queue.CheckReceiptNamespace(ctx)
}

// grantIntent reconstructs the entire immutable payload from the original
// Genesis, the exact KovaBuild and queue charge, and the active grant's
// persisted nonce/fence/observed RV. A later ledger RV is never substituted.
func (r *KovaBuildReconciler) grantIntent(ctx context.Context, build *kovav1.KovaBuild, grant activeReservation) (recoveryreceipt.GrantIntent, error) {
	return r.grantIntentWithObservation(ctx, build, grant, nil)
}

func (r *KovaBuildReconciler) grantIntentWithObservation(ctx context.Context, build *kovav1.KovaBuild, grant activeReservation, observation *admissiongenesis.Observation) (recoveryreceipt.GrantIntent, error) {
	if r.Genesis == nil || build == nil || build.UID == "" || grant.BuildName != build.Name ||
		grant.Requester != requesterKey(build) || !validReservationHex(grant.GrantNonce, 32) ||
		grant.GrantFence == 0 || !validGrantObservedRV(grant.GrantObservedRV) {
		return recoveryreceipt.GrantIntent{}, fmt.Errorf("Genesis grant has no reconstructible pre-effect identity")
	}
	if observation == nil {
		if err := r.Genesis.Check(ctx); err != nil {
			return recoveryreceipt.GrantIntent{}, err
		}
	} else if err := observation.AssertNoOrdinaryContext(r.Genesis, ctx); err != nil {
		return recoveryreceipt.GrantIntent{}, err
	}
	receipt := r.Genesis.Bootstrap.Receipt
	if r.Cfg.WorkerPoolID == "" || r.Cfg.WorkerPoolID != receipt.Contract.WorkerPoolID {
		return recoveryreceipt.GrantIntent{}, fmt.Errorf("configured worker pool differs from immutable admission Genesis")
	}
	queue := r.queueStoreForNamespace(build.Namespace)
	var err error
	var verifiedEntry queueadmission.Intent
	var verifiedFound bool
	if observation == nil {
		verifiedEntry, verifiedFound, err = queue.VerifyForBuildSnapshot(ctx, build)
	} else {
		verifiedEntry, verifiedFound, err = queue.VerifyForBuildObserved(build, observation)
	}
	if err != nil {
		return recoveryreceipt.GrantIntent{}, err
	}
	var queueEntry queueadmission.Intent
	var found bool
	if observation == nil {
		queueEntry, found, err = queue.Lookup(ctx, build.Name)
	} else {
		queueEntry, found, err = queue.LookupObserved(build.Name, observation)
	}
	if err != nil {
		return recoveryreceipt.GrantIntent{}, err
	}
	if found != verifiedFound || (found && queueEntry != verifiedEntry) {
		return recoveryreceipt.GrantIntent{}, queueadmission.ErrDrift
	}
	requestDigest, err := queueadmission.DigestBuild(build)
	if err != nil {
		return recoveryreceipt.GrantIntent{}, err
	}
	mode := "direct"
	var queueLink *recoveryreceipt.ReceiptLink
	if nonce := build.Annotations[queueadmission.IntentAnnotation]; nonce != "" {
		if !found || queueEntry.Nonce != nonce || queueEntry.ReceiptUID == "" || queueEntry.ReceiptDigest == "" ||
			queueEntry.RequestDigest != requestDigest || queueEntry.CleanupKind != "" {
			return recoveryreceipt.GrantIntent{}, queueadmission.ErrDrift
		}
		queueIntent, err := queue.ReceiptIntent(build, queueEntry)
		if err != nil {
			return recoveryreceipt.GrantIntent{}, err
		}
		queueCM, err := recoveryreceipt.NewQueueConfigMap(queueIntent)
		if err != nil {
			return recoveryreceipt.GrantIntent{}, err
		}
		queueLink = &recoveryreceipt.ReceiptLink{Name: queueCM.Name, UID: queueEntry.ReceiptUID, DataDigest: queueEntry.ReceiptDigest}
		mode = "queued"
	} else if found {
		return recoveryreceipt.GrantIntent{}, queueadmission.ErrDrift
	}
	w := r.Genesis.Original
	return recoveryreceipt.GrantIntent{
		Build: recoveryreceipt.PinnedBuild{
			Namespace: build.Namespace, NamespaceUID: w.NamespaceUID,
			ReceiptNamespace: receipt.Contract.ReceiptNamespace, ReceiptNamespaceUID: receipt.Contract.ReceiptNamespaceUID,
			GenesisName: receipt.GenesisName, GenesisUID: w.GenesisUID, Generation: receipt.Contract.Generation,
			ActiveLedgerUID: w.ActiveLedgerUID, QueueLedgerUID: w.QueueLedgerUID,
			BuildName: build.Name, BuildUID: string(build.UID),
			RequesterName: build.Spec.Requester.Username, RequesterUID: build.Spec.Requester.UID,
			RequesterHash: queueadmission.HashRequester(build.Spec.Requester.Username),
			RequestDigest: requestDigest, SourceDigest: build.Spec.Source.Digest,
			WorkerPoolID: r.Cfg.WorkerPoolID,
		},
		AdmissionMode: mode, QueueReceipt: queueLink, GrantNonce: grant.GrantNonce,
		ActiveLedgerFence: grant.GrantFence, ActiveLedgerRV: grant.GrantObservedRV,
		AllocatedWorkerSlots: grant.Slots,
	}, nil
}

func sameGrant(left, right activeReservation) bool {
	return left.BuildName == right.BuildName && left.Requester == right.Requester && left.Slots == right.Slots &&
		left.GrantNonce == right.GrantNonce && left.GrantFence == right.GrantFence
}

// armFreshGrant is callable only by the fresh grant-CAS owner. Persisting the
// direct observation RV in a second CAS is the receipt-Create arm: a cleanup
// CAS that closes/removes an unarmed entry makes this arm conflict. The RV is
// opaque and may be newer than the first grant CAS response.
func (r *KovaBuildReconciler) armFreshGrant(ctx context.Context, build *kovav1.KovaBuild, original activeReservation) (activeReservation, error) {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || !sameGrant(entry, original) || entry.Closing || entry.GrantObservedRV != "" || entry.GrantReceiptUID != "" {
			return activeReservation{}, fmt.Errorf("fresh grant lost its unarmed active charge")
		}
		if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
			return activeReservation{}, err
		}
		if !validGrantObservedRV(cm.ResourceVersion) {
			return activeReservation{}, fmt.Errorf("active ledger observation has unsupported opaque resourceVersion")
		}
		entry.GrantObservedRV = cm.ResourceVersion
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return activeReservation{}, err
				}
				continue
			}
			// A lost arm response cannot authorize a receipt Create, even if a
			// later read finds this RV. Restart can observe but never replay it.
			return activeReservation{}, err
		}
		_, confirmed, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		observed, ok := confirmed.Active[reservationKey(build)]
		if !ok || !sameGrant(observed, original) || observed.GrantObservedRV != entry.GrantObservedRV ||
			observed.Closing || observed.GrantReceiptUID != "" {
			return activeReservation{}, fmt.Errorf("armed grant changed before receipt Create")
		}
		return observed, nil
	}
	return activeReservation{}, fmt.Errorf("active admission ledger is busy arming grant for %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) observeGrantReceipt(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) (recoveryreceipt.EffectWitness, error) {
	if r.Genesis == nil {
		return recoveryreceipt.EffectWitness{}, fmt.Errorf("Genesis grant observation requires an original guard")
	}
	var witness recoveryreceipt.EffectWitness
	err := r.Genesis.Observe(ctx, func(observation *admissiongenesis.Observation) error {
		var readErr error
		witness, readErr = r.observeGrantReceiptWithObservation(nil, build, entry, observation)
		return readErr
	})
	return witness, err
}

func (r *KovaBuildReconciler) observeGrantReceiptWithObservation(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation, observation *admissiongenesis.Observation) (recoveryreceipt.EffectWitness, error) {
	if err := observation.AssertNoOrdinaryContext(r.Genesis, ctx); err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	if err := r.checkGrantReceiptNamespaceWithObservation(nil, build.Namespace, observation); err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	intent, err := r.grantIntentWithObservation(nil, build, entry, observation)
	if err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	var witness recoveryreceipt.EffectWitness
	err = observation.Read(r.Genesis, func(readCtx context.Context) error {
		var readErr error
		witness, readErr = recoveryreceipt.ObserveGrant(readCtx, r.RecoveryReceipts, intent)
		return readErr
	})
	if err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	if err := r.checkGrantReceiptNamespaceWithObservation(nil, build.Namespace, observation); err != nil {
		return recoveryreceipt.EffectWitness{}, err
	}
	return witness, nil
}

func (r *KovaBuildReconciler) verifyPinnedGrant(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) error {
	if r.Genesis == nil {
		return fmt.Errorf("Genesis grant verification requires an original guard")
	}
	return r.Genesis.Observe(ctx, func(observation *admissiongenesis.Observation) error {
		return r.verifyPinnedGrantWithObservation(nil, build, entry, observation)
	})
}

func (r *KovaBuildReconciler) verifyPinnedGrantWithObservation(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation, observation *admissiongenesis.Observation) error {
	if err := observation.AssertNoOrdinaryContext(r.Genesis, ctx); err != nil {
		return err
	}
	if entry.Closing {
		return fmt.Errorf("%w: %s/%s grant is closing", errAdmissionClosed, build.Namespace, build.Name)
	}
	if entry.GrantReceiptUID == "" || entry.GrantReceiptDigest == "" {
		// Observation may distinguish a present receipt from a missing one,
		// but neither result gives a replacement leader Create/Pod authority.
		if entry.GrantObservedRV != "" {
			_, _ = r.observeGrantReceiptWithObservation(nil, build, entry, observation)
		}
		return errGrantUnpinned
	}
	witness, err := r.observeGrantReceiptWithObservation(nil, build, entry, observation)
	if err != nil {
		return err
	}
	if witness.ReceiptUID != entry.GrantReceiptUID || witness.DataDigest != entry.GrantReceiptDigest {
		return recoveryreceipt.ErrChanged
	}
	return nil
}

func (r *KovaBuildReconciler) pinGrantReceipt(ctx context.Context, build *kovav1.KovaBuild, armed activeReservation, witness recoveryreceipt.EffectWitness) (activeReservation, error) {
	if !validGrantReceiptUID(witness.ReceiptUID) || !validGrantReceiptDigest(witness.DataDigest) {
		return activeReservation{}, recoveryreceipt.ErrChanged
	}
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || !sameGrant(entry, armed) || entry.GrantObservedRV != armed.GrantObservedRV || entry.GrantCleanupReady {
			return activeReservation{}, fmt.Errorf("grant changed before receipt pin")
		}
		if entry.GrantReceiptUID != "" {
			if entry.GrantReceiptUID != witness.ReceiptUID || entry.GrantReceiptDigest != witness.DataDigest {
				return activeReservation{}, recoveryreceipt.ErrChanged
			}
			return entry, nil
		}
		entry.GrantReceiptUID, entry.GrantReceiptDigest = witness.ReceiptUID, witness.DataDigest
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return activeReservation{}, err
				}
				continue
			}
			// A direct read may resolve this pin's lost response; it never
			// authorizes another receipt Create.
			_, observed, readErr := r.readReservations(ctx, build.Namespace)
			if readErr == nil {
				pinned, ok := observed.Active[reservationKey(build)]
				if ok && sameGrant(pinned, armed) && pinned.GrantObservedRV == armed.GrantObservedRV &&
					pinned.GrantReceiptUID == witness.ReceiptUID && pinned.GrantReceiptDigest == witness.DataDigest {
					return pinned, nil
				}
			}
			return activeReservation{}, err
		}
		_, observed, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return activeReservation{}, err
		}
		pinned, ok := observed.Active[reservationKey(build)]
		if !ok || !sameGrant(pinned, armed) || pinned.GrantObservedRV != armed.GrantObservedRV ||
			pinned.GrantReceiptUID != witness.ReceiptUID || pinned.GrantReceiptDigest != witness.DataDigest {
			return activeReservation{}, recoveryreceipt.ErrChanged
		}
		return pinned, nil
	}
	return activeReservation{}, fmt.Errorf("active admission ledger is busy pinning grant receipt for %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) finishFreshGrant(ctx context.Context, build *kovav1.KovaBuild, original activeReservation) error {
	armed, err := r.armFreshGrant(ctx, build, original)
	if err != nil {
		return err
	}
	var intent recoveryreceipt.GrantIntent
	if err := r.Genesis.Observe(ctx, func(observation *admissiongenesis.Observation) error {
		if err := observation.Read(r.Genesis, func(readCtx context.Context) error {
			return r.ensureLiveAdmissionBuild(readCtx, build)
		}); err != nil {
			return err
		}
		if err := r.checkGrantReceiptNamespaceWithObservation(nil, build.Namespace, observation); err != nil {
			return err
		}
		var readErr error
		intent, readErr = r.grantIntentWithObservation(nil, build, armed, observation)
		return readErr
	}); err != nil {
		return err
	}
	witness, err := recoveryreceipt.RecordGrantOnce(ctx, r.RecoveryReceipts, intent)
	if err != nil {
		return err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	pinned, err := r.pinGrantReceipt(ctx, build, armed, witness)
	if err != nil {
		return err
	}
	if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
		return err
	}
	return r.verifyPinnedGrant(ctx, build, pinned)
}

// releaseGenesisGrantReceipt runs only after a closing active charge, a
// terminal/deleting CR, no in-flight Pod nonce, and direct Pod absence. An
// unarmed grant can be removed by CAS: its paused creator must arm first and
// will conflict. An armed grant cannot treat NotFound as no-effect evidence.
// The exact observed receipt must be pinned before a durable cleanup marker
// permits UID-preconditioned deletion and post-Delete absence confirmation.
func (r *KovaBuildReconciler) releaseGenesisGrantReceipt(ctx context.Context, build *kovav1.KovaBuild, cm *corev1.ConfigMap, state reservationState, entry activeReservation) (bool, error) {
	if entry.GrantObservedRV == "" {
		return true, nil
	}
	if entry.GrantReceiptUID == "" {
		witness, err := r.observeGrantReceipt(ctx, build, entry)
		if err != nil {
			return false, fmt.Errorf("armed grant receipt outcome is unknown: %w", err)
		}
		if _, err := r.pinGrantReceipt(ctx, build, entry, witness); err != nil {
			return false, err
		}
		return false, nil
	}
	if !entry.GrantCleanupReady {
		if err := r.verifyPinnedGrantForCleanup(ctx, build, entry); err != nil {
			return false, err
		}
		entry.GrantCleanupReady = true
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := r.deletePinnedGrantReceipt(ctx, build, entry); err != nil {
		return false, err
	}
	return true, nil
}

func (r *KovaBuildReconciler) verifyPinnedGrantForCleanup(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) error {
	witness, err := r.observeGrantReceipt(ctx, build, entry)
	if err != nil {
		return err
	}
	if witness.ReceiptUID != entry.GrantReceiptUID || witness.DataDigest != entry.GrantReceiptDigest {
		return recoveryreceipt.ErrChanged
	}
	return nil
}

func (r *KovaBuildReconciler) deletePinnedGrantReceipt(ctx context.Context, build *kovav1.KovaBuild, entry activeReservation) error {
	if !entry.GrantCleanupReady || !entry.Closing || entry.GrantReceiptUID == "" {
		return fmt.Errorf("grant receipt lacks durable cleanup disposition")
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	intent, err := r.grantIntent(ctx, build, entry)
	if err != nil {
		return err
	}
	expected, err := recoveryreceipt.NewGrantConfigMap(intent)
	if err != nil {
		return err
	}
	observed, err := r.RecoveryReceipts.Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		witness, err := recoveryreceipt.QualifyGrant(intent, observed)
		if err != nil || witness.ReceiptUID != entry.GrantReceiptUID || witness.DataDigest != entry.GrantReceiptDigest {
			return recoveryreceipt.ErrChanged
		}
		uid := types.UID(entry.GrantReceiptUID)
		if err := r.RecoveryReceipts.Delete(ctx, expected.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if _, err := r.RecoveryReceipts.Get(ctx, expected.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		if err == nil {
			return fmt.Errorf("grant receipt still exists after UID-preconditioned Delete")
		}
		return err
	}
	if err := r.checkGrantReceiptNamespace(ctx, build.Namespace); err != nil {
		return err
	}
	return r.Genesis.Check(ctx)
}
