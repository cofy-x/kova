// Package recoveryreceipt records bounded admission facts independently of
// the quota ledgers. A receipt is evidence of a pre-effect intent, never a
// permit to replay an uncertain Kubernetes Create or release capacity. Normal
// settled builds must eventually delete their receipts with a UID precondition,
// after all related CR/Pod effects are closed; incident evidence must remain
// until an authorized disposition. This package deliberately does not impose a
// TTL or turn receipts into a durable per-build workflow history.
package recoveryreceipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	queueReceiptPrefix    = "kova-admission-intent-"
	queueReceiptLabel     = "kova.cofy.dev/admission-receipt"
	queueReceiptData      = "queue-intent"
	queueVersion          = "1"
	maxQueueDataJSONBytes = 16 * 1024
)

var (
	ErrInvalid     = errors.New("invalid admission recovery receipt")
	ErrChanged     = errors.New("admission recovery receipt changed")
	ErrUnconfirmed = errors.New("admission recovery receipt is unconfirmed")
)

// QueueIntent is the exact, bounded fact set written after a successful queue
// reservation and before the one authorized KovaBuild Create. The two ledger
// UIDs and installation identity come from the already committed Genesis.
// RequestDigest is the queue ledger's canonical KovaBuild-spec digest; the
// source digest is repeated explicitly for recovery and wrong-source checks.
type QueueIntent struct {
	Namespace       string
	NamespaceUID    string
	GenesisName     string
	GenesisUID      string
	Generation      string
	ActiveLedgerUID string
	QueueLedgerUID  string
	BuildName       string
	RequesterName   string
	RequesterHash   string
	RequesterUID    string
	RequestDigest   string
	SourceDigest    string
	QueueNonce      string
}

// ObservedQueueIntent binds the exact receipt to one Kubernetes object UID.
// This is an evidence witness, not side-effect authorization.
type ObservedQueueIntent struct {
	Intent          QueueIntent
	ReceiptName     string
	ReceiptUID      string
	ResourceVersion string
}

// ConfigMaps is implemented by client-go's typed, direct ConfigMap client.
// Its transport must make one wire attempt for Create; this package calls it
// exactly once and never infers non-persistence from a missing readback.
type ConfigMaps interface {
	Create(context.Context, *corev1.ConfigMap, metav1.CreateOptions) (*corev1.ConfigMap, error)
	Get(context.Context, string, metav1.GetOptions) (*corev1.ConfigMap, error)
}

func lowerHex(value string, n int) bool {
	if len(value) != n {
		return false
	}
	for _, b := range []byte(value) {
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return false
		}
	}
	return true
}

func opaqueUID(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value)
}

func (q QueueIntent) validate() error {
	if len(validation.IsDNS1123Label(q.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(q.GenesisName)) != 0 ||
		len(validation.IsDNS1123Subdomain(q.BuildName)) != 0 ||
		!opaqueUID(q.NamespaceUID) || !opaqueUID(q.GenesisUID) ||
		!opaqueUID(q.ActiveLedgerUID) || !opaqueUID(q.QueueLedgerUID) ||
		!lowerHex(q.Generation, 32) || !lowerHex(q.QueueNonce, 32) ||
		q.RequesterName == "" || !utf8.ValidString(q.RequesterName) || utf8.RuneCountInString(q.RequesterName) > 253 ||
		!lowerHex(q.RequesterHash, 64) || !lowerHex(q.RequestDigest, 64) ||
		!utf8.ValidString(q.RequesterUID) || utf8.RuneCountInString(q.RequesterUID) > 253 ||
		!strings.HasPrefix(q.SourceDigest, "sha256:") || !lowerHex(strings.TrimPrefix(q.SourceDigest, "sha256:"), 64) {
		return ErrInvalid
	}
	requesterHash := sha256.Sum256([]byte(q.RequesterName))
	if hex.EncodeToString(requesterHash[:]) != q.RequesterHash {
		return ErrInvalid
	}
	encoded, err := json.Marshal(q.data())
	if err != nil || len(encoded) > maxQueueDataJSONBytes {
		return ErrInvalid
	}
	return nil
}

func (q QueueIntent) name() string {
	return queueReceiptPrefix + q.QueueNonce
}

func (q QueueIntent) data() map[string]string {
	return map[string]string{
		"version":         queueVersion,
		"namespaceName":   q.Namespace,
		"namespaceUID":    q.NamespaceUID,
		"genesisName":     q.GenesisName,
		"genesisUID":      q.GenesisUID,
		"generation":      q.Generation,
		"activeLedgerUID": q.ActiveLedgerUID,
		"queueLedgerUID":  q.QueueLedgerUID,
		"buildName":       q.BuildName,
		"requesterName":   q.RequesterName,
		"requesterHash":   q.RequesterHash,
		"requesterUID":    q.RequesterUID,
		"requestDigest":   q.RequestDigest,
		"sourceDigest":    q.SourceDigest,
		"queueNonce":      q.QueueNonce,
	}
}

// NewQueueConfigMap builds an immutable, unowned receipt. Owner references
// would let normal CR deletion erase evidence before incident disposition.
func NewQueueConfigMap(q QueueIntent) (*corev1.ConfigMap, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	immutable := true
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: q.Namespace,
			Name:      q.name(),
			Labels:    map[string]string{queueReceiptLabel: queueReceiptData},
		},
		Immutable: &immutable,
		Data:      q.data(),
	}, nil
}

// QualifyQueue accepts only the exact original data and object identity. It
// intentionally does not use a label List or absence as evidence.
func QualifyQueue(q QueueIntent, cm *corev1.ConfigMap) (ObservedQueueIntent, error) {
	if err := q.validate(); err != nil {
		return ObservedQueueIntent{}, err
	}
	if cm == nil || cm.Namespace != q.Namespace || cm.Name != q.name() ||
		!opaqueUID(string(cm.UID)) || cm.ResourceVersion == "" ||
		cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable ||
		len(cm.BinaryData) != 0 || len(cm.OwnerReferences) != 0 ||
		len(cm.Data) != len(q.data()) || cm.Labels[queueReceiptLabel] != queueReceiptData {
		return ObservedQueueIntent{}, ErrChanged
	}
	for key, want := range q.data() {
		if cm.Data[key] != want {
			return ObservedQueueIntent{}, ErrChanged
		}
	}
	return ObservedQueueIntent{
		Intent: q, ReceiptName: cm.Name,
		ReceiptUID: string(cm.UID), ResourceVersion: cm.ResourceVersion,
	}, nil
}

// RecordQueueOnce makes one Create call and one direct named readback. A lost
// Create response can be resolved by an exact read. A failed/missing read,
// even after a successful Create response, leaves the effect unarmed. An
// AlreadyExists or peer-created matching receipt is evidence only: callers
// still need the original fresh queue-CAS ownership before one CR Create.
func RecordQueueOnce(ctx context.Context, api ConfigMaps, q QueueIntent) (ObservedQueueIntent, error) {
	if api == nil {
		return ObservedQueueIntent{}, fmt.Errorf("%w: direct ConfigMap API is missing", ErrInvalid)
	}
	cm, err := NewQueueConfigMap(q)
	if err != nil {
		return ObservedQueueIntent{}, err
	}
	created, createErr := api.Create(ctx, cm, metav1.CreateOptions{})
	readback, readErr := api.Get(ctx, cm.Name, metav1.GetOptions{})
	if readErr != nil {
		return ObservedQueueIntent{}, fmt.Errorf("%w: Create result %v; direct read: %v", ErrUnconfirmed, createErr, readErr)
	}
	observed, err := QualifyQueue(q, readback)
	if err != nil {
		return ObservedQueueIntent{}, err
	}
	if created != nil {
		createdWitness, err := QualifyQueue(q, created)
		if err != nil || createdWitness.ReceiptUID != observed.ReceiptUID {
			return ObservedQueueIntent{}, ErrChanged
		}
	} else if createErr == nil {
		return ObservedQueueIntent{}, ErrUnconfirmed
	}
	return observed, nil
}

// ObserveQueue directly rereads an existing receipt after a process restart.
// A NotFound/error says nothing about whether an earlier Create can arrive.
func ObserveQueue(ctx context.Context, api ConfigMaps, q QueueIntent) (ObservedQueueIntent, error) {
	if api == nil {
		return ObservedQueueIntent{}, fmt.Errorf("%w: direct ConfigMap API is missing", ErrInvalid)
	}
	if err := q.validate(); err != nil {
		return ObservedQueueIntent{}, err
	}
	readback, err := api.Get(ctx, q.name(), metav1.GetOptions{})
	if err != nil {
		return ObservedQueueIntent{}, fmt.Errorf("%w: direct read: %v", ErrUnconfirmed, err)
	}
	return QualifyQueue(q, readback)
}
