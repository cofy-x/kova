package recoveryreceipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type scriptedConfigMaps struct {
	create func(*corev1.ConfigMap) (*corev1.ConfigMap, error)
	get    func(string) (*corev1.ConfigMap, error)
	puts   int
	reads  int
}

func (s *scriptedConfigMaps) Create(_ context.Context, cm *corev1.ConfigMap, _ metav1.CreateOptions) (*corev1.ConfigMap, error) {
	s.puts++
	return s.create(cm)
}

func (s *scriptedConfigMaps) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.ConfigMap, error) {
	s.reads++
	return s.get(name)
}

func exampleQueueIntent() QueueIntent {
	requesterHash := sha256.Sum256([]byte("alice"))
	return QueueIntent{
		Namespace: "runner-new", NamespaceUID: "namespace-original-uid",
		ReceiptNamespace: "receipts-new", ReceiptNamespaceUID: "receipts-original-uid",
		GenesisName: "kova-service-admission-genesis", GenesisUID: "genesis-original-uid",
		Generation:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ActiveLedgerUID: "active-original-uid", QueueLedgerUID: "queue-original-uid",
		BuildName: "idem-bounded-build", RequesterName: "alice", RequesterHash: hex.EncodeToString(requesterHash[:]),
		RequesterUID:  "principal-uid",
		RequestDigest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		SourceDigest:  "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		QueueNonce:    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}
}

func serverReceipt(t *testing.T, q QueueIntent, uid string) *corev1.ConfigMap {
	t.Helper()
	cm, err := NewQueueConfigMap(q)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = types.UID(uid)
	cm.ResourceVersion = "rv-opaque"
	return cm
}

func TestQueueReceiptCreateAndDirectReadback(t *testing.T) {
	q := exampleQueueIntent()
	cm := serverReceipt(t, q, "receipt-original-uid")
	api := &scriptedConfigMaps{
		create: func(proposal *corev1.ConfigMap) (*corev1.ConfigMap, error) {
			if proposal.Name != cm.Name || proposal.Namespace != q.ReceiptNamespace ||
				proposal.Immutable == nil || !*proposal.Immutable || len(proposal.OwnerReferences) != 0 {
				t.Fatal("Create did not send the immutable, unowned original receipt")
			}
			return cm.DeepCopy(), nil
		},
		get: func(name string) (*corev1.ConfigMap, error) {
			if name != cm.Name {
				t.Fatalf("read %q, want %q", name, cm.Name)
			}
			return cm.DeepCopy(), nil
		},
	}
	witness, err := RecordQueueOnce(context.Background(), api, q)
	if err != nil {
		t.Fatal(err)
	}
	if witness.ReceiptUID != string(cm.UID) || witness.ResourceVersion != cm.ResourceVersion || witness.DataDigest == "" || witness.Intent != q || api.puts != 1 || api.reads != 1 {
		t.Fatalf("incorrect receipt witness or call count: %+v, puts=%d reads=%d", witness, api.puts, api.reads)
	}
}

func TestQueueReceiptLostCreateResultNeedsExactRead(t *testing.T) {
	q := exampleQueueIntent()
	cm := serverReceipt(t, q, "receipt-original-uid")
	api := &scriptedConfigMaps{
		create: func(*corev1.ConfigMap) (*corev1.ConfigMap, error) { return nil, errors.New("response lost") },
		get:    func(string) (*corev1.ConfigMap, error) { return cm.DeepCopy(), nil },
	}
	if _, err := RecordQueueOnce(context.Background(), api, q); err != nil {
		t.Fatalf("exact direct read did not establish receipt evidence: %v", err)
	}
	if api.puts != 1 || api.reads != 1 {
		t.Fatalf("uncertain Create was replayed: puts=%d reads=%d", api.puts, api.reads)
	}

	api.get = func(string) (*corev1.ConfigMap, error) {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, cm.Name)
	}
	if _, err := ObserveQueue(context.Background(), api, q); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("missing restart read should retain unknown Create, got %v", err)
	}
}

func TestQueueReceiptAlreadyExistsIsEvidenceOnly(t *testing.T) {
	q := exampleQueueIntent()
	cm := serverReceipt(t, q, "peer-receipt-uid")
	api := &scriptedConfigMaps{
		create: func(*corev1.ConfigMap) (*corev1.ConfigMap, error) {
			return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, cm.Name)
		},
		get: func(string) (*corev1.ConfigMap, error) { return cm.DeepCopy(), nil },
	}
	if _, err := RecordQueueOnce(context.Background(), api, q); !errors.Is(err, ErrUnconfirmed) || api.puts != 1 || api.reads != 1 {
		t.Fatalf("pre-existing receipt armed a fresh effect: %v, puts=%d reads=%d", err, api.puts, api.reads)
	}
	observed, err := ObserveQueue(context.Background(), api, q)
	if err != nil || observed.ReceiptUID != string(cm.UID) {
		t.Fatalf("pre-existing receipt was not still observable as evidence: %+v, %v", observed, err)
	}
}

func TestQueueReceiptCreateSuccessWithoutReadIsUnconfirmed(t *testing.T) {
	q := exampleQueueIntent()
	cm := serverReceipt(t, q, "receipt-original-uid")
	api := &scriptedConfigMaps{
		create: func(*corev1.ConfigMap) (*corev1.ConfigMap, error) { return cm.DeepCopy(), nil },
		get: func(string) (*corev1.ConfigMap, error) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, cm.Name)
		},
	}
	if _, err := RecordQueueOnce(context.Background(), api, q); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("successful response without direct evidence armed effect: %v", err)
	}
	if api.puts != 1 || api.reads != 1 {
		t.Fatalf("unexpected request count: puts=%d reads=%d", api.puts, api.reads)
	}
}

func TestQueueReceiptRejectsChangedFactsAndReplacement(t *testing.T) {
	q := exampleQueueIntent()
	for name, mutate := range map[string]func(*corev1.ConfigMap){
		"wrong source digest":  func(cm *corev1.ConfigMap) { cm.Data["sourceDigest"] = "sha256:" + q.RequestDigest },
		"wrong namespace UID":  func(cm *corev1.ConfigMap) { cm.Data["namespaceUID"] = "replacement" },
		"wrong ledger UID":     func(cm *corev1.ConfigMap) { cm.Data["activeLedgerUID"] = "replacement" },
		"wrong nonce":          func(cm *corev1.ConfigMap) { cm.Data["queueNonce"] = "ffffffffffffffffffffffffffffffff" },
		"wrong requester UID":  func(cm *corev1.ConfigMap) { cm.Data["requesterUID"] = "replacement" },
		"wrong requester name": func(cm *corev1.ConfigMap) { cm.Data["requesterName"] = "mallory" },
		"extra data":           func(cm *corev1.ConfigMap) { cm.Data["surprise"] = "value" },
		"missing data":         func(cm *corev1.ConfigMap) { delete(cm.Data, "requestDigest") },
		"mutable":              func(cm *corev1.ConfigMap) { cm.Immutable = nil },
		"owner garbage collection": func(cm *corev1.ConfigMap) {
			cm.OwnerReferences = []metav1.OwnerReference{{UID: "build-uid", Name: q.BuildName}}
		},
		"deleting":    func(cm *corev1.ConfigMap) { now := metav1.Now(); cm.DeletionTimestamp = &now },
		"wrong name":  func(cm *corev1.ConfigMap) { cm.Name = "same-name-replacement" },
		"wrong label": func(cm *corev1.ConfigMap) { cm.Labels[queueReceiptLabel] = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			cm := serverReceipt(t, q, "receipt-original-uid")
			mutate(cm)
			if _, err := QualifyQueue(q, cm); !errors.Is(err, ErrChanged) {
				t.Fatalf("changed receipt was accepted: %v", err)
			}
		})
	}

	created := serverReceipt(t, q, "receipt-original-uid")
	replaced := serverReceipt(t, q, "receipt-replacement-uid")
	api := &scriptedConfigMaps{
		create: func(*corev1.ConfigMap) (*corev1.ConfigMap, error) { return created.DeepCopy(), nil },
		get:    func(string) (*corev1.ConfigMap, error) { return replaced.DeepCopy(), nil },
	}
	if _, err := RecordQueueOnce(context.Background(), api, q); !errors.Is(err, ErrChanged) {
		t.Fatalf("same-name replacement after Create was accepted: %v", err)
	}
	api.create = func(*corev1.ConfigMap) (*corev1.ConfigMap, error) {
		return created.DeepCopy(), errors.New("response carried a partial error")
	}
	if _, err := RecordQueueOnce(context.Background(), api, q); !errors.Is(err, ErrChanged) {
		t.Fatalf("non-nil Create response with replacement UID was accepted: %v", err)
	}
}

func TestInvalidQueueReceiptDoesNotCallAPI(t *testing.T) {
	q := exampleQueueIntent()
	q.SourceDigest = "sha256:WRONG"
	api := &scriptedConfigMaps{}
	if _, err := RecordQueueOnce(context.Background(), api, q); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid digest reached API: %v", err)
	}
	if api.puts != 0 || api.reads != 0 {
		t.Fatalf("invalid receipt sent requests: puts=%d reads=%d", api.puts, api.reads)
	}
}
