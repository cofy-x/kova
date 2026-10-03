package buildcontroller

import (
	"context"
	"errors"
	"testing"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type receiptDeleteFailure struct {
	client.Client
	deleteFirst bool
	calls       int
}

func (c *receiptDeleteFailure) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok {
		c.calls++
		if c.deleteFirst {
			if err := c.Client.Delete(ctx, obj, opts...); err != nil {
				return err
			}
		}
		return context.DeadlineExceeded
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func pinnedQueueFixture(t *testing.T) (queueadmission.Store, client.Client, *kovav1.KovaBuild, queueadmission.Intent, recoveryreceipt.ObservedQueueIntent) {
	t.Helper()
	ctx := context.Background()
	r, api, _ := genesisAdmissionFixture(t)
	store := r.queueStoreForNamespace("jobs")
	build := queuedBuild("receipted-http", "alice", 2, 1)
	entry, fresh, err := store.Reserve(ctx, build)
	if err != nil || !fresh {
		t.Fatalf("reserve: entry=%#v fresh=%t err=%v", entry, fresh, err)
	}
	intent, err := store.ReceiptIntent(build, entry)
	if err != nil {
		t.Fatal(err)
	}
	cm, err := recoveryreceipt.NewQueueConfigMap(intent)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "queue-receipt-original"
	if err := api.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	observed, err := api.GetConfigMap(ctx, cm.Namespace, cm.Name)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := recoveryreceipt.QualifyQueue(intent, observed)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PinReceipt(ctx, build, entry, witness); err != nil {
		t.Fatal(err)
	}
	build.Annotations = map[string]string{
		queueadmission.IntentAnnotation:        entry.Nonce,
		queueadmission.ReceiptUIDAnnotation:    witness.ReceiptUID,
		queueadmission.ReceiptDigestAnnotation: witness.DataDigest,
	}
	return store, api.Client, build, entry, witness
}

func TestGenesisQueueReceiptHoldsCapacityUntilTerminalCleanup(t *testing.T) {
	ctx := context.Background()
	store, base, build, _, witness := pinnedQueueFixture(t)
	if err := store.VerifyForBuild(ctx, build); err != nil {
		t.Fatalf("pinned queue receipt was not verified: %v", err)
	}
	if err := base.Create(ctx, build); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseForBuild(ctx, build); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("nonterminal build released queue quota: %v", err)
	}
	if current, found, err := store.Lookup(ctx, build.Name); err != nil || !found || current.ReceiptUID != witness.ReceiptUID {
		t.Fatalf("active build lost its queue charge: entry=%#v found=%t err=%v", current, found, err)
	}
	var persisted kovav1.KovaBuild
	if err := base.Get(ctx, client.ObjectKeyFromObject(build), &persisted); err != nil {
		t.Fatal(err)
	}
	persisted.Status.Phase = kovav1.PhaseSucceeded
	if err := base.Status().Update(ctx, &persisted); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseForBuild(ctx, &persisted); err != nil {
		t.Fatalf("terminal receipt cleanup: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err != nil || found {
		t.Fatalf("settled queue charge remains: found=%t err=%v", found, err)
	}
	var receipt corev1.ConfigMap
	if err := base.Get(ctx, client.ObjectKey{Namespace: witness.Intent.ReceiptNamespace, Name: witness.ReceiptName}, &receipt); !apierrors.IsNotFound(err) {
		t.Fatalf("exact settled receipt remains: %v", err)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(build), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Annotations[queueadmission.ReceiptSettledAnnotation] != witness.ReceiptUID {
		t.Fatalf("terminal CR lacks exact receipt-settled witness: %#v", persisted.Annotations)
	}
	if err := store.ReleaseForBuild(ctx, &persisted); err != nil {
		t.Fatalf("terminal cleanup is not restart-safe: %v", err)
	}
}

func TestGenesisQueueReceiptReplacementCannotReleaseCapacity(t *testing.T) {
	ctx := context.Background()
	store, base, build, _, witness := pinnedQueueFixture(t)
	var original corev1.ConfigMap
	if err := base.Get(ctx, client.ObjectKey{Namespace: witness.Intent.ReceiptNamespace, Name: witness.ReceiptName}, &original); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, &original); err != nil {
		t.Fatal(err)
	}
	replacement := original.DeepCopy()
	replacement.UID, replacement.ResourceVersion = "replacement-uid", ""
	if err := base.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyForBuild(ctx, build); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("replacement qualified as pinned receipt: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err != nil || !found {
		t.Fatalf("replacement freed capacity: found=%t err=%v", found, err)
	}
}

func TestGenesisReceiptNamespaceReplacementCannotReleaseCapacity(t *testing.T) {
	ctx := context.Background()
	store, base, build, _, witness := pinnedQueueFixture(t)
	var namespace corev1.Namespace
	if err := base.Get(ctx, client.ObjectKey{Name: witness.Intent.ReceiptNamespace}, &namespace); err != nil {
		t.Fatal(err)
	}
	namespace.UID = "replacement-receipt-namespace-uid"
	if err := base.Update(ctx, &namespace); err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyForBuild(ctx, build); err == nil {
		t.Fatalf("replacement receipt namespace qualified: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err == nil && !found {
		t.Fatal("replacement receipt namespace freed queue capacity")
	}
}

func TestGenesisRejectedCleanupMarkerSurvivesUnknownDelete(t *testing.T) {
	ctx := context.Background()
	store, base, build, entry, witness := pinnedQueueFixture(t)
	denied := &receiptDeleteFailure{Client: base}
	store.Client = denied
	var before corev1.ConfigMap
	if err := base.Get(ctx, client.ObjectKey{Namespace: witness.Intent.ReceiptNamespace, Name: witness.ReceiptName}, &before); err != nil {
		t.Fatalf("receipt disappeared before test Delete: %v", err)
	}
	if err := store.ReleaseRejectedReceipt(ctx, build, witness); !errors.Is(err, context.DeadlineExceeded) {
		var after corev1.ConfigMap
		readErr := base.Get(ctx, client.ObjectKey{Namespace: witness.Intent.ReceiptNamespace, Name: witness.ReceiptName}, &after)
		t.Fatalf("unknown receipt Delete should block cleanup: %v (calls=%d read=%v)", err, denied.calls, readErr)
	}
	marked, found, err := store.Lookup(ctx, build.Name)
	if err != nil || !found || marked.CleanupKind != "rejected" || marked.ReceiptUID != witness.ReceiptUID {
		t.Fatalf("rejection marker was not durably retained: entry=%#v found=%t err=%v", marked, found, err)
	}
	if err := store.ReleaseForBuild(ctx, build); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("rejected cleanup became terminal cleanup: %v", err)
	}
	store.Client = base
	if err := store.ResumeRejectedCleanup(ctx, build.Name, entry.Nonce); err != nil {
		t.Fatalf("persisted rejection could not resume safely: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err != nil || found {
		t.Fatalf("rejected charge remains after exact cleanup: found=%t err=%v", found, err)
	}
}

func TestGenesisRejectedLostDeleteResponseRequiresDirectAbsence(t *testing.T) {
	ctx := context.Background()
	store, base, build, _, witness := pinnedQueueFixture(t)
	store.Client = &receiptDeleteFailure{Client: base, deleteFirst: true}
	if err := store.ReleaseRejectedReceipt(ctx, build, witness); err != nil {
		t.Fatalf("lost Delete response with directly absent exact receipt stayed charged: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err != nil || found {
		t.Fatalf("settled rejected charge remains: found=%t err=%v", found, err)
	}
}

func TestGenesisQueueReceiptCleanupRequiresExactCRUID(t *testing.T) {
	ctx := context.Background()
	store, base, build, _, _ := pinnedQueueFixture(t)
	build.Status.Phase = kovav1.PhaseSucceeded
	if err := base.Create(ctx, build); err != nil {
		t.Fatal(err)
	}
	wrong := build.DeepCopy()
	wrong.UID = "different-cr-uid"
	if err := store.ReleaseForBuild(ctx, wrong); !errors.Is(err, queueadmission.ErrDrift) {
		t.Fatalf("wrong CR UID released receipt: %v", err)
	}
	if _, found, err := store.Lookup(ctx, build.Name); err != nil || !found {
		t.Fatalf("wrong CR UID freed capacity: found=%t err=%v", found, err)
	}
}
