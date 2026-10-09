// Package queueadmission owns the HTTP queue's single compare-and-swap ledger.
// A queue intent authorizes at most one KovaBuild Create call. An uncertain
// Create never frees the intent merely because the CR is not yet visible.
package queueadmission

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/admissioncontract"
	"github.com/cofy-x/kova/internal/admissionjson"
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/recoveryreceipt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConfigMapName            = "kova-service-queue-admission"
	IntentAnnotation         = "kova.cofy.dev/queue-intent"
	ReceiptUIDAnnotation     = "kova.cofy.dev/queue-receipt-uid"
	ReceiptDigestAnnotation  = "kova.cofy.dev/queue-receipt-digest"
	ReceiptSettledAnnotation = "kova.cofy.dev/queue-receipt-settled-uid"
	LedgerDataKey            = "queue.json"
	dataKey                  = LedgerDataKey
	maxLedgerBytes           = 768 * 1024
	maxHeaderBytes           = 4096
	maxIntentBytes           = 736
	maxCASAttempts           = 16
	cleanupRejected          = "rejected"
	cleanupTerminal          = "terminal"
)

var (
	ErrFull     = errors.New("queue admission limit reached")
	ErrBusy     = errors.New("queue admission is busy")
	ErrConflict = errors.New("queue intent has different build parameters")
	ErrDrift    = errors.New("queue admission ledger and KovaBuild disagree")
)

type Intent struct {
	RequesterHash string `json:"requesterHash"`
	RequestDigest string `json:"requestDigest"`
	Nonce         string `json:"nonce"`
	CreatedAtUnix int64  `json:"createdAtUnix"`
	ReceiptUID    string `json:"receiptUID,omitempty"`
	ReceiptDigest string `json:"receiptDigest,omitempty"`
	CleanupKind   string `json:"cleanupKind,omitempty"`
}

type state struct {
	Version        int               `json:"version"`
	GlobalLimit    int               `json:"globalLimit"`
	RequesterLimit int               `json:"requesterLimit"`
	Intents        map[string]Intent `json:"intents"`
}

type Store struct {
	Client         client.Client
	Reader         client.Reader
	Genesis        *admissiongenesis.Guard
	Namespace      string
	GlobalLimit    int
	RequesterLimit int
}

func HashRequester(username string) string {
	sum := sha256.Sum256([]byte(username))
	return hex.EncodeToString(sum[:])
}

// DigestBuild excludes the authentication UID: a username and idempotency key
// identify a retry, while the immutable requested build parameters must match.
func DigestBuild(build *kovav1.KovaBuild) (string, error) {
	spec := build.Spec.DeepCopy()
	spec.Requester.UID = ""
	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s Store) reader() client.Reader {
	if s.Reader != nil {
		return s.Reader
	}
	return s.Client
}

func (s Store) key() client.ObjectKey {
	return client.ObjectKey{Namespace: s.Namespace, Name: ConfigMapName}
}

func (s Store) freshState() state {
	return state{Version: 1, GlobalLimit: s.GlobalLimit, RequesterLimit: s.RequesterLimit, Intents: map[string]Intent{}}
}

// GenesisEmptyQueueData and ValidateQueueLedgerForGenesis reuse the current
// bounded queue schema without letting the bootstrapper create an intent.
func GenesisEmptyQueueData(globalLimit, requesterLimit int) (string, error) {
	initial := (Store{GlobalLimit: globalLimit, RequesterLimit: requesterLimit}).freshState()
	initial.Version = 2
	encoded, err := encodeState(initial)
	return string(encoded), err
}

func ValidateQueueLedgerForGenesis(cm *corev1.ConfigMap, globalLimit, requesterLimit int) error {
	if cm == nil {
		return fmt.Errorf("queue admission ledger is missing")
	}
	if len(cm.Data) != 1 || len(cm.Data[dataKey]) == 0 || len(cm.Data[dataKey]) > maxLedgerBytes || len(cm.BinaryData) != 0 {
		return fmt.Errorf("queue admission ledger has invalid data keys or size")
	}
	var current state
	if err := admissionjson.Decode([]byte(cm.Data[dataKey]), &current, allowedQueueField); err != nil {
		return err
	}
	if current.GlobalLimit != globalLimit || current.RequesterLimit != requesterLimit {
		return fmt.Errorf("queue admission ledger limits differ from receipt")
	}
	if current.Version != 2 {
		return fmt.Errorf("queue admission Genesis requires version 2")
	}
	return validateState(current)
}

// A 253-byte DNS build ID, two 64-byte hashes, one 32-byte nonce, a
// 19-digit positive timestamp, compact 64-byte receipt UID, 71-byte digest,
// typed cleanup disposition, and JSON punctuation fit within 736 bytes. The header reserves
// the largest configured integers. Thus every accepted 1000-intent table
// remains below the 768 KiB data guard.
func validateLimits(global, requester int) error {
	if global < 1 || global > 1000 || requester < 1 || requester > global ||
		maxHeaderBytes+global*maxIntentBytes > maxLedgerBytes {
		return fmt.Errorf("unsupported queue admission capacity")
	}
	return nil
}

func validLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Kubernetes-generated receipt UIDs are UUID-shaped. A noncompact or
// JSON-escaped UID is held as unknown rather than overflowing the queue cap.
func validReceiptUID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, c := range []byte(value) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') &&
			(c < 'A' || c > 'Z') && c != '-' {
			return false
		}
	}
	return true
}

func validateState(current state) error {
	if (current.Version != 1 && current.Version != 2) || current.Intents == nil ||
		validateLimits(current.GlobalLimit, current.RequesterLimit) != nil || len(current.Intents) > current.GlobalLimit {
		return fmt.Errorf("unsupported queue admission state")
	}
	requesterCounts := make(map[string]int)
	for id, entry := range current.Intents {
		if len(id) == 0 || len(id) > 253 || len(validation.IsDNS1123Subdomain(id)) != 0 ||
			!validLowerHex(entry.RequesterHash, 64) || !validLowerHex(entry.RequestDigest, 64) ||
			!validLowerHex(entry.Nonce, 32) || entry.CreatedAtUnix < 1 {
			return fmt.Errorf("queue admission state has an invalid intent")
		}
		if (entry.ReceiptUID == "") != (entry.ReceiptDigest == "") ||
			(current.Version == 1 && (entry.ReceiptUID != "" || entry.CleanupKind != "")) ||
			(entry.ReceiptUID != "" && (!validReceiptUID(entry.ReceiptUID) ||
				!strings.HasPrefix(entry.ReceiptDigest, "sha256:") ||
				!validLowerHex(strings.TrimPrefix(entry.ReceiptDigest, "sha256:"), 64))) ||
			(entry.CleanupKind != "" && (entry.ReceiptUID == "" ||
				(entry.CleanupKind != cleanupRejected && entry.CleanupKind != cleanupTerminal))) {
			return fmt.Errorf("queue admission state has an invalid receipt link")
		}
		requesterCounts[entry.RequesterHash]++
		if requesterCounts[entry.RequesterHash] > current.RequesterLimit {
			return fmt.Errorf("queue admission state exceeds requester capacity")
		}
	}
	return nil
}

func encodeState(current state) ([]byte, error) {
	if err := validateState(current); err != nil {
		return nil, err
	}
	data, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	if len(data) > maxLedgerBytes {
		return nil, fmt.Errorf("queue ledger would exceed %d bytes", maxLedgerBytes)
	}
	return data, nil
}

func allowedQueueField(path []string, key string) bool {
	switch len(path) {
	case 0:
		switch key {
		case "version", "globalLimit", "requesterLimit", "intents":
			return true
		}
	case 1:
		return path[0] == "intents"
	case 2:
		if path[0] == "intents" {
			switch key {
			case "requesterHash", "requestDigest", "nonce", "createdAtUnix", "receiptUID", "receiptDigest", "cleanupKind":
				return true
			}
		}
	}
	return false
}

func (s Store) read(ctx context.Context) (*corev1.ConfigMap, state, error) {
	if s.Genesis != nil {
		if s.Reader == nil {
			return nil, state{}, fmt.Errorf("Genesis queue admission requires a direct API reader")
		}
		if err := s.Genesis.Check(ctx); err != nil {
			return nil, state{}, err
		}
	}
	var cm corev1.ConfigMap
	if err := s.reader().Get(ctx, s.key(), &cm); err != nil {
		return nil, state{}, err
	}
	if s.Genesis != nil {
		if err := s.Genesis.CheckLedger(&cm, admissioncontract.Queue); err != nil {
			return nil, state{}, err
		}
	}
	if len(cm.Data) != 1 || len(cm.Data[dataKey]) == 0 || len(cm.Data[dataKey]) > maxLedgerBytes || len(cm.BinaryData) != 0 {
		return nil, state{}, fmt.Errorf("queue ledger %s has invalid data keys or size", s.key())
	}
	var result state
	if err := admissionjson.Decode([]byte(cm.Data[dataKey]), &result, allowedQueueField); err != nil {
		return nil, state{}, fmt.Errorf("queue ledger %s is invalid: %w", s.key(), err)
	}
	if result.GlobalLimit != s.GlobalLimit || result.RequesterLimit != s.RequesterLimit {
		return nil, state{}, fmt.Errorf("queue ledger %s has invalid state or mismatched replica limits", s.key())
	}
	if (s.Genesis != nil && result.Version != 2) || (s.Genesis == nil && result.Version != 1) {
		return nil, state{}, fmt.Errorf("queue ledger %s has the wrong receipt protocol version", s.key())
	}
	if err := validateState(result); err != nil {
		return nil, state{}, fmt.Errorf("queue ledger %s has unsupported state: %w", s.key(), err)
	}
	return &cm, result, nil
}

// CheckReady validates the durable ledger without creating or repairing it.
// A missing ledger after startup may have held an unknown Create intent, so a
// request path must not silently replace it with an empty one.
func (s Store) CheckReady(ctx context.Context) error {
	_, _, err := s.read(ctx)
	return err
}

// PreflightInitialization validates first-start eligibility without writing.
// Service startup must run this before creating the active ledger, otherwise
// rejecting legacy builds here would leave an unusable half-initialized pair.
func (s Store) PreflightInitialization(ctx context.Context) error {
	if s.Genesis != nil {
		return s.CheckReady(ctx)
	}
	if err := validateLimits(s.GlobalLimit, s.RequesterLimit); err != nil {
		return fmt.Errorf("queue ledger %s has invalid replica limits", s.key())
	}
	if _, _, err := s.read(ctx); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	// A missing ledger is only safe to initialize in an empty runner namespace.
	// Rolling upgrades must drain old builds and move to a fresh namespace.
	var builds kovav1.KovaBuildList
	if err := s.reader().List(ctx, &builds, client.InNamespace(s.Namespace), client.Limit(1)); err != nil {
		return err
	}
	if len(builds.Items) != 0 {
		// Another new Service replica may have created the ledger and then a
		// CR between our first missing-ledger GET and this List. Re-read the
		// authoritative ledger before classifying the CR as legacy state.
		if _, _, err := s.read(ctx); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("queue ledger %s is absent while KovaBuilds exist; drain and migrate before admission", s.key())
	}
	return nil
}

func (s Store) initialize(ctx context.Context) error {
	if err := s.PreflightInitialization(ctx); err != nil {
		return err
	}
	data, err := encodeState(s.freshState())
	if err != nil {
		return err
	}
	cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: s.Namespace}, Data: map[string]string{dataKey: string(data)}}
	return s.Client.Create(ctx, &cm)
}

func (s Store) write(ctx context.Context, cm *corev1.ConfigMap, next state) error {
	if cm == nil || len(cm.Data) != 1 || len(cm.Data[dataKey]) == 0 || len(cm.BinaryData) != 0 {
		return fmt.Errorf("queue ledger has unsupported data keys")
	}
	data, err := encodeState(next)
	if err != nil {
		return err
	}
	if s.Genesis != nil {
		return s.Genesis.PatchLedgerData(ctx, cm, admissioncontract.Queue, string(data))
	}
	copy := cm.DeepCopy()
	copy.Data = map[string]string{dataKey: string(data)}
	return s.Client.Update(ctx, copy)
}

func randomNonce() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func waitCAS(ctx context.Context, attempt int) error {
	delay := time.Duration(1<<min(attempt, 5)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// EnsureInitialized establishes the queue ledger before the HTTP listener is
// made ready. Direct/admin CRs created after startup must not be mistaken for
// legacy state merely because no HTTP submission has happened yet.
func (s Store) EnsureInitialized(ctx context.Context) error {
	if s.Genesis != nil {
		return s.CheckReady(ctx)
	}
	for retry := 0; retry < maxCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, _, err := s.read(ctx); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if err := s.initialize(ctx); err != nil {
			if apierrors.IsAlreadyExists(err) || apierrors.IsTooManyRequests(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			// A missing Create response may hide a committed ledger. Only a
			// validating direct read permits the listener to start.
			if _, _, readErr := s.read(ctx); readErr == nil {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrBusy
}

// Reserve returns fresh=true only to the single caller authorized to issue
// Create. Every duplicate, including a same-key retry after process loss,
// must observe the CR or report a pending intent, never issue another Create.
func (s Store) Reserve(ctx context.Context, build *kovav1.KovaBuild) (Intent, bool, error) {
	digest, err := DigestBuild(build)
	if err != nil {
		return Intent{}, false, err
	}
	nonce, err := randomNonce()
	if err != nil {
		return Intent{}, false, err
	}
	requester := HashRequester(build.Spec.Requester.Username)
	proposed := Intent{RequesterHash: requester, RequestDigest: digest, Nonce: nonce, CreatedAtUnix: time.Now().Unix()}
	for retry := 0; retry < maxCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return Intent{}, false, err
		}
		cm, current, err := s.read(ctx)
		if err != nil {
			return Intent{}, false, err
		}
		if existing, ok := current.Intents[build.Name]; ok {
			if existing.RequesterHash != requester || existing.RequestDigest != digest {
				return Intent{}, false, ErrConflict
			}
			return existing, false, nil
		}
		if len(current.Intents) >= s.GlobalLimit {
			return Intent{}, false, ErrFull
		}
		count := 0
		for _, entry := range current.Intents {
			if entry.RequesterHash == requester {
				count++
			}
		}
		if count >= s.RequesterLimit {
			return Intent{}, false, ErrFull
		}
		current.Intents[build.Name] = proposed
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return Intent{}, false, err
				}
				continue
			}
			// A lost Update response is safe to recover only when a direct GET
			// observes this exact nonce. A later commit after an absent GET
			// remains a held, operator-visible intent; no Create is issued.
			if observed, found, readErr := s.Lookup(ctx, build.Name); readErr == nil && found && observed == proposed {
				return proposed, true, nil
			}
			if apierrors.IsTooManyRequests(err) {
				return Intent{}, false, ErrBusy
			}
			return Intent{}, false, err
		}
		return proposed, true, nil
	}
	return Intent{}, false, ErrBusy
}

func (s Store) Lookup(ctx context.Context, id string) (Intent, bool, error) {
	_, current, err := s.read(ctx)
	if err != nil {
		return Intent{}, false, err
	}
	entry, ok := current.Intents[id]
	return entry, ok, nil
}

// ReceiptIntent derives the independently persisted pre-Create evidence from
// the externally pinned Genesis and the exact winning queue CAS. A caller
// cannot supply its own namespace, ledger UID, or request digest.
func (s Store) ReceiptIntent(build *kovav1.KovaBuild, entry Intent) (recoveryreceipt.QueueIntent, error) {
	if s.Genesis == nil || build == nil || build.Namespace != s.Namespace ||
		build.Name == "" || entry.Nonce == "" {
		return recoveryreceipt.QueueIntent{}, ErrDrift
	}
	receipt := s.Genesis.Bootstrap.Receipt
	binding := s.Genesis.Original
	if receipt.Namespace != s.Namespace || receipt.Contract.NamespaceUID != binding.NamespaceUID ||
		receipt.Contract.ReceiptNamespace == s.Namespace || receipt.Contract.ReceiptNamespace == "" ||
		receipt.Contract.ReceiptNamespaceUID == "" ||
		receipt.GenesisUID != binding.GenesisUID ||
		binding.ActiveLedgerUID == "" || binding.QueueLedgerUID == "" {
		return recoveryreceipt.QueueIntent{}, ErrDrift
	}
	digest, err := DigestBuild(build)
	if err != nil {
		return recoveryreceipt.QueueIntent{}, err
	}
	if entry.RequesterHash != HashRequester(build.Spec.Requester.Username) || entry.RequestDigest != digest {
		return recoveryreceipt.QueueIntent{}, ErrDrift
	}
	intent := recoveryreceipt.QueueIntent{
		Namespace: s.Namespace, NamespaceUID: binding.NamespaceUID,
		ReceiptNamespace:    receipt.Contract.ReceiptNamespace,
		ReceiptNamespaceUID: receipt.Contract.ReceiptNamespaceUID,
		GenesisName:         receipt.GenesisName, GenesisUID: binding.GenesisUID,
		Generation:      receipt.Contract.Generation,
		ActiveLedgerUID: binding.ActiveLedgerUID, QueueLedgerUID: binding.QueueLedgerUID,
		BuildName: build.Name, RequesterName: build.Spec.Requester.Username,
		RequesterUID: build.Spec.Requester.UID, RequesterHash: entry.RequesterHash,
		RequestDigest: entry.RequestDigest, SourceDigest: build.Spec.Source.Digest,
		QueueNonce: entry.Nonce,
	}
	if _, err := recoveryreceipt.NewQueueConfigMap(intent); err != nil {
		return recoveryreceipt.QueueIntent{}, err
	}
	return intent, nil
}

func (s Store) receiptNamespace() string {
	if s.Genesis == nil {
		return ""
	}
	return s.Genesis.Bootstrap.Receipt.Contract.ReceiptNamespace
}

// CheckReceiptNamespace always compares against the externally pinned UID in
// the immutable Genesis receipt. A current same-name Namespace is not enough.
func (s Store) CheckReceiptNamespace(ctx context.Context) error {
	if s.Genesis == nil || s.Genesis.Bootstrap.API == nil {
		return ErrDrift
	}
	ns, err := s.Genesis.Bootstrap.API.GetNamespace(ctx, s.receiptNamespace())
	if err != nil {
		return err
	}
	if err := s.Genesis.Bootstrap.Receipt.QualifyReceiptNamespace(ns); err != nil {
		return fmt.Errorf("%w: %v", ErrDrift, err)
	}
	return nil
}

// PinReceipt retains the exact original ConfigMap UID and data digest in the
// bounded queue slot before the fresh CAS owner may send one KovaBuild Create.
// A lost pin response is recoverable only by direct readback of this link;
// other callers may observe it but never gain Create authority.
func (s Store) PinReceipt(ctx context.Context, build *kovav1.KovaBuild, reserved Intent, witness recoveryreceipt.ObservedQueueIntent) error {
	if err := s.CheckReceiptNamespace(ctx); err != nil {
		return err
	}
	expected, err := s.ReceiptIntent(build, reserved)
	if err != nil {
		return err
	}
	if witness.Intent != expected || !validReceiptUID(witness.ReceiptUID) ||
		!strings.HasPrefix(witness.DataDigest, "sha256:") ||
		!validLowerHex(strings.TrimPrefix(witness.DataDigest, "sha256:"), 64) {
		return ErrDrift
	}
	for retry := 0; retry < maxCASAttempts; retry++ {
		cm, current, err := s.read(ctx)
		if err != nil {
			return err
		}
		entry, found := current.Intents[build.Name]
		if !found || entry.Nonce != reserved.Nonce || entry.RequestDigest != reserved.RequestDigest ||
			entry.RequesterHash != reserved.RequesterHash || entry.CleanupKind != "" {
			return ErrDrift
		}
		if entry.ReceiptUID != "" {
			if entry.ReceiptUID == witness.ReceiptUID && entry.ReceiptDigest == witness.DataDigest {
				return s.CheckReceiptNamespace(ctx)
			}
			return ErrDrift
		}
		entry.ReceiptUID, entry.ReceiptDigest = witness.ReceiptUID, witness.DataDigest
		current.Intents[build.Name] = entry
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if observed, found, readErr := s.Lookup(ctx, build.Name); readErr == nil && found &&
				observed.Nonce == entry.Nonce && observed.ReceiptUID == entry.ReceiptUID &&
				observed.ReceiptDigest == entry.ReceiptDigest && observed.CleanupKind == "" {
				return s.CheckReceiptNamespace(ctx)
			}
			return err
		}
		observed, found, err := s.Lookup(ctx, build.Name)
		if err != nil {
			return err
		}
		if !found || observed.Nonce != entry.Nonce || observed.ReceiptUID != entry.ReceiptUID ||
			observed.ReceiptDigest != entry.ReceiptDigest || observed.CleanupKind != "" {
			return ErrDrift
		}
		return s.CheckReceiptNamespace(ctx)
	}
	return ErrBusy
}

func (s Store) observePinnedReceipt(ctx context.Context, build *kovav1.KovaBuild, entry Intent) error {
	if err := s.CheckReceiptNamespace(ctx); err != nil {
		return err
	}
	if entry.ReceiptUID == "" || entry.ReceiptDigest == "" || entry.CleanupKind != "" ||
		build.Annotations[ReceiptUIDAnnotation] != entry.ReceiptUID ||
		build.Annotations[ReceiptDigestAnnotation] != entry.ReceiptDigest {
		return ErrDrift
	}
	intent, err := s.ReceiptIntent(build, entry)
	if err != nil {
		return err
	}
	expected, err := recoveryreceipt.NewQueueConfigMap(intent)
	if err != nil {
		return err
	}
	var observed corev1.ConfigMap
	if err := s.reader().Get(ctx, client.ObjectKey{Namespace: s.receiptNamespace(), Name: expected.Name}, &observed); err != nil {
		return fmt.Errorf("%w: pinned queue receipt is unavailable: %v", ErrDrift, err)
	}
	witness, err := recoveryreceipt.QualifyQueue(intent, &observed)
	if err != nil || witness.ReceiptUID != entry.ReceiptUID || witness.DataDigest != entry.ReceiptDigest {
		return ErrDrift
	}
	return s.Genesis.Check(ctx)
}

func (s Store) markReceiptCleanupReady(ctx context.Context, id string, expected Intent, kind string) error {
	if kind != cleanupRejected && kind != cleanupTerminal {
		return ErrDrift
	}
	for retry := 0; retry < maxCASAttempts; retry++ {
		cm, current, err := s.read(ctx)
		if err != nil {
			return err
		}
		entry, found := current.Intents[id]
		if !found || entry.Nonce != expected.Nonce || entry.RequestDigest != expected.RequestDigest ||
			entry.RequesterHash != expected.RequesterHash || entry.ReceiptUID != expected.ReceiptUID ||
			entry.ReceiptDigest != expected.ReceiptDigest || entry.ReceiptUID == "" {
			return ErrDrift
		}
		if entry.CleanupKind == kind {
			return nil
		}
		if entry.CleanupKind != "" {
			return ErrDrift
		}
		entry.CleanupKind = kind
		current.Intents[id] = entry
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if observed, found, readErr := s.Lookup(ctx, id); readErr == nil && found &&
				observed.Nonce == entry.Nonce && observed.ReceiptUID == entry.ReceiptUID &&
				observed.ReceiptDigest == entry.ReceiptDigest && observed.CleanupKind == kind {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrBusy
}

// deletePinnedReceipt is called only after the cleanup-kind CAS. That marker
// proves the original receipt was directly observed before any deletion, so
// a restart may accept NotFound without mistaking a never-created receipt
// for a settled one. The original fresh owner made only one Create attempt.
func (s Store) deletePinnedReceipt(ctx context.Context, entry Intent) error {
	if s.Genesis == nil || s.Client == nil || entry.CleanupKind == "" || !validReceiptUID(entry.ReceiptUID) {
		return ErrDrift
	}
	if err := s.CheckReceiptNamespace(ctx); err != nil {
		return err
	}
	name := "kova-admission-intent-" + entry.Nonce
	var current corev1.ConfigMap
	err := s.reader().Get(ctx, client.ObjectKey{Namespace: s.receiptNamespace(), Name: name}, &current)
	if apierrors.IsNotFound(err) {
		return s.Genesis.Check(ctx)
	}
	if err != nil {
		return err
	}
	digest, err := recoveryreceipt.DigestData(current.Data)
	if err != nil || current.Name != name || current.Namespace != s.receiptNamespace() ||
		string(current.UID) != entry.ReceiptUID || digest != entry.ReceiptDigest ||
		current.Immutable == nil || !*current.Immutable || current.DeletionTimestamp != nil ||
		len(current.OwnerReferences) != 0 || len(current.BinaryData) != 0 {
		return ErrDrift
	}
	if err := s.Genesis.Check(ctx); err != nil {
		return err
	}
	uid := current.UID
	deleteErr := s.Client.Delete(ctx, &current, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	var after corev1.ConfigMap
	readErr := s.reader().Get(ctx, client.ObjectKey{Namespace: s.receiptNamespace(), Name: name}, &after)
	if apierrors.IsNotFound(readErr) {
		return s.Genesis.Check(ctx)
	}
	if readErr != nil {
		return readErr
	}
	if after.UID != uid {
		return ErrDrift
	}
	if deleteErr != nil {
		return deleteErr
	}
	return fmt.Errorf("queue receipt %s/%s remains after UID-preconditioned Delete", s.receiptNamespace(), name)
}

func terminalOrDeleting(build *kovav1.KovaBuild) bool {
	if build == nil {
		return false
	}
	if build.DeletionTimestamp != nil {
		return true
	}
	switch build.Status.Phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}

// markReceiptSettled leaves a durable CR witness before the queue entry is
// removed. A crash after that final CAS can then distinguish completed
// receipt cleanup from an absent/unconfirmed pre-effect receipt.
func (s Store) markReceiptSettled(ctx context.Context, build *kovav1.KovaBuild, entry Intent) error {
	for retry := 0; retry < maxCASAttempts; retry++ {
		var current kovav1.KovaBuild
		if err := s.reader().Get(ctx, client.ObjectKeyFromObject(build), &current); err != nil {
			return err
		}
		if current.UID == "" || current.UID != build.UID || !terminalOrDeleting(&current) ||
			current.Annotations[IntentAnnotation] != entry.Nonce ||
			current.Annotations[ReceiptUIDAnnotation] != entry.ReceiptUID ||
			current.Annotations[ReceiptDigestAnnotation] != entry.ReceiptDigest {
			return ErrDrift
		}
		if current.Annotations[ReceiptSettledAnnotation] == entry.ReceiptUID {
			return nil
		}
		if current.Annotations[ReceiptSettledAnnotation] != "" {
			return ErrDrift
		}
		copy := current.DeepCopy()
		copy.Annotations[ReceiptSettledAnnotation] = entry.ReceiptUID
		if err := s.Genesis.Check(ctx); err != nil {
			return err
		}
		writeErr := s.Client.Update(ctx, copy)
		var observed kovav1.KovaBuild
		readErr := s.reader().Get(ctx, client.ObjectKeyFromObject(build), &observed)
		if readErr == nil && observed.UID == build.UID &&
			observed.Annotations[ReceiptSettledAnnotation] == entry.ReceiptUID {
			return s.Genesis.Check(ctx)
		}
		if apierrors.IsConflict(writeErr) {
			if err := waitCAS(ctx, retry); err != nil {
				return err
			}
			continue
		}
		if writeErr != nil {
			return writeErr
		}
		if readErr != nil {
			return readErr
		}
		return ErrDrift
	}
	return ErrBusy
}

// VerifyForBuild rejects a service-created CR whose queue intent has vanished
// or changed before its first active reservation is committed.
func (s Store) VerifyForBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	nonce := build.Annotations[IntentAnnotation]
	if nonce == "" {
		if s.Genesis != nil {
			// Direct/admin CRs do not consume HTTP quota, but they still
			// require the original committed queue ledger before a grant.
			return s.CheckReady(ctx)
		}
		return nil // Direct/admin CRs are outside the HTTP queue quota.
	}
	entry, found, err := s.Lookup(ctx, build.Name)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: queue ledger %s is absent for KovaBuild %s/%s", ErrDrift, s.key(), build.Namespace, build.Name)
	}
	if err != nil {
		return err
	}
	if !found || entry.Nonce != nonce || entry.RequesterHash != HashRequester(build.Spec.Requester.Username) {
		return fmt.Errorf("%w: KovaBuild %s/%s has no matching queue intent", ErrDrift, build.Namespace, build.Name)
	}
	digest, err := DigestBuild(build)
	if err != nil {
		return err
	}
	if entry.RequestDigest != digest {
		return fmt.Errorf("%w: KovaBuild %s/%s request digest differs from queue intent", ErrDrift, build.Namespace, build.Name)
	}
	if s.Genesis != nil {
		if err := s.observePinnedReceipt(ctx, build, entry); err != nil {
			return err
		}
	}
	return nil
}

// ReleaseForBuild runs only after active capacity has been durably committed,
// or when a matching terminal/deleting CR and its Pod cleanup are verified.
func (s Store) ReleaseForBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	if s.Genesis != nil {
		return s.releaseReceiptedBuild(ctx, build)
	}
	nonce := build.Annotations[IntentAnnotation]
	if nonce == "" {
		return nil
	}
	digest, err := DigestBuild(build)
	if err != nil {
		return err
	}
	for retry := 0; retry < maxCASAttempts; retry++ {
		cm, current, err := s.read(ctx)
		if err != nil {
			return err
		}
		entry, found := current.Intents[build.Name]
		if !found {
			return nil
		}
		if entry.Nonce != nonce || entry.RequesterHash != HashRequester(build.Spec.Requester.Username) || entry.RequestDigest != digest {
			return fmt.Errorf("%w: KovaBuild %s/%s has a different queue intent", ErrDrift, build.Namespace, build.Name)
		}
		delete(current.Intents, build.Name)
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if _, found, readErr := s.Lookup(ctx, build.Name); readErr == nil && !found {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrBusy
}

// releaseReceiptedBuild is called only after the controller fenced the grant,
// confirmed its runner Pod gone, and released any active reservation. The
// queue slot remains charged until the exact receipt is deleted and the
// terminal CR records that deletion. An unknown pre-effect Create never enters
// this path because it has no confirmed, terminal CR with the pinned link.
func (s Store) releaseReceiptedBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	if build == nil || build.UID == "" || !terminalOrDeleting(build) {
		return ErrDrift
	}
	nonce := build.Annotations[IntentAnnotation]
	if nonce == "" {
		return nil // direct/admin CRs have no HTTP queue receipt
	}
	digest, err := DigestBuild(build)
	if err != nil {
		return err
	}
	for retry := 0; retry < maxCASAttempts; retry++ {
		cm, current, err := s.read(ctx)
		if err != nil {
			return err
		}
		entry, found := current.Intents[build.Name]
		if !found {
			// Only a prior successful cleanup can remove this link. Require
			// its terminal CR witness, then check the old receipt name remains
			// absent; absence alone is never a no-effect inference.
			var persisted kovav1.KovaBuild
			if err := s.reader().Get(ctx, client.ObjectKeyFromObject(build), &persisted); err != nil {
				return err
			}
			if persisted.UID != build.UID || !terminalOrDeleting(&persisted) ||
				persisted.Annotations[IntentAnnotation] != nonce ||
				persisted.Annotations[ReceiptSettledAnnotation] == "" ||
				persisted.Annotations[ReceiptSettledAnnotation] != persisted.Annotations[ReceiptUIDAnnotation] {
				return ErrDrift
			}
			var receipt corev1.ConfigMap
			if err := s.CheckReceiptNamespace(ctx); err != nil {
				return err
			}
			err := s.reader().Get(ctx, client.ObjectKey{Namespace: s.receiptNamespace(), Name: "kova-admission-intent-" + nonce}, &receipt)
			if !apierrors.IsNotFound(err) {
				if err != nil {
					return err
				}
				return ErrDrift
			}
			return s.Genesis.Check(ctx)
		}
		if entry.Nonce != nonce || entry.RequesterHash != HashRequester(build.Spec.Requester.Username) ||
			entry.RequestDigest != digest || entry.ReceiptUID == "" ||
			build.Annotations[ReceiptUIDAnnotation] != entry.ReceiptUID ||
			build.Annotations[ReceiptDigestAnnotation] != entry.ReceiptDigest {
			return ErrDrift
		}
		if entry.CleanupKind == "" {
			if err := s.observePinnedReceipt(ctx, build, entry); err != nil {
				return err
			}
			if err := s.markReceiptCleanupReady(ctx, build.Name, entry, cleanupTerminal); err != nil {
				return err
			}
			continue
		}
		if entry.CleanupKind != cleanupTerminal {
			return ErrDrift
		}
		if err := s.deletePinnedReceipt(ctx, entry); err != nil {
			return err
		}
		if err := s.markReceiptSettled(ctx, build, entry); err != nil {
			return err
		}
		delete(current.Intents, build.Name)
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if _, found, readErr := s.Lookup(ctx, build.Name); readErr == nil && !found {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrBusy
}

// ReleaseRejected is for a definitive, non-persisted Create result only.
// It must never be called for a timeout, connection error, or server error.
func (s Store) ReleaseRejected(ctx context.Context, id, nonce string) error {
	if s.Genesis != nil {
		return fmt.Errorf("%w: Genesis rejection cleanup requires its pinned receipt", ErrDrift)
	}
	return s.releaseRejected(ctx, id, nonce)
}

// ReleaseRejectedReceipt is only for this fresh queue-CAS owner's definitive
// KovaBuild Create rejection. A timeout, server error, or absent CR never
// qualifies. The immutable receipt is re-observed and cleanup kind is CASed
// before UID deletion; quota is released last.
func (s Store) ReleaseRejectedReceipt(ctx context.Context, build *kovav1.KovaBuild, witness recoveryreceipt.ObservedQueueIntent) error {
	if s.Genesis == nil || build == nil || witness.Intent.BuildName != build.Name {
		return ErrDrift
	}
	entry, found, err := s.Lookup(ctx, build.Name)
	if err != nil {
		return err
	}
	if !found || entry.Nonce != witness.Intent.QueueNonce || entry.ReceiptUID != witness.ReceiptUID ||
		entry.ReceiptDigest != witness.DataDigest || entry.CleanupKind != "" {
		return ErrDrift
	}
	if err := s.observePinnedReceipt(ctx, build, entry); err != nil {
		return err
	}
	if err := s.markReceiptCleanupReady(ctx, build.Name, entry, cleanupRejected); err != nil {
		return err
	}
	entry.CleanupKind = cleanupRejected
	if err := s.deletePinnedReceipt(ctx, entry); err != nil {
		return err
	}
	return s.releaseRejected(ctx, build.Name, entry.Nonce)
}

// ResumeRejectedCleanup is limited to an already-persisted rejected cleanup
// decision. A duplicate request cannot turn an unconfirmed or missing receipt
// into permission to replay the original KovaBuild Create.
func (s Store) ResumeRejectedCleanup(ctx context.Context, id, nonce string) error {
	if s.Genesis == nil {
		return ErrDrift
	}
	entry, found, err := s.Lookup(ctx, id)
	if err != nil {
		return err
	}
	if !found || entry.Nonce != nonce || entry.CleanupKind != cleanupRejected {
		return ErrDrift
	}
	var build kovav1.KovaBuild
	err = s.reader().Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: id}, &build)
	if !apierrors.IsNotFound(err) {
		if err != nil {
			return err
		}
		return ErrDrift
	}
	if err := s.deletePinnedReceipt(ctx, entry); err != nil {
		return err
	}
	return s.releaseRejected(ctx, id, nonce)
}

func (s Store) releaseRejected(ctx context.Context, id, nonce string) error {
	for retry := 0; retry < maxCASAttempts; retry++ {
		cm, current, err := s.read(ctx)
		if err != nil {
			return err
		}
		entry, found := current.Intents[id]
		if !found {
			return nil
		}
		if entry.Nonce != nonce {
			return fmt.Errorf("%w: queue intent %s changed before rejection cleanup", ErrDrift, id)
		}
		if s.Genesis != nil && (entry.CleanupKind != cleanupRejected || entry.ReceiptUID == "") {
			return ErrDrift
		}
		delete(current.Intents, id)
		if err := s.write(ctx, cm, current); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if _, found, readErr := s.Lookup(ctx, id); readErr == nil && !found {
				return nil
			}
			return err
		}
		return nil
	}
	return ErrBusy
}
