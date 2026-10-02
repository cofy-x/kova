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
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/admissionjson"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConfigMapName    = "kova-service-queue-admission"
	IntentAnnotation = "kova.cofy.dev/queue-intent"
	dataKey          = "queue.json"
	maxLedgerBytes   = 768 * 1024
	maxHeaderBytes   = 4096
	maxIntentBytes   = 512
	maxCASAttempts   = 16
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

// A 253-byte DNS build ID, two 64-byte hashes, one 32-byte nonce, a
// 19-digit positive timestamp and all JSON punctuation fit within 512 bytes.
// The header reserves the largest configured integers and field names. Thus
// every accepted 1000-intent table remains below the 768 KiB data guard.
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

func validateState(current state) error {
	if current.Version != 1 || current.Intents == nil ||
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
			case "requesterHash", "requestDigest", "nonce", "createdAtUnix":
				return true
			}
		}
	}
	return false
}

func (s Store) read(ctx context.Context) (*corev1.ConfigMap, state, error) {
	var cm corev1.ConfigMap
	if err := s.reader().Get(ctx, s.key(), &cm); err != nil {
		return nil, state{}, err
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

// VerifyForBuild rejects a service-created CR whose queue intent has vanished
// or changed before its first active reservation is committed.
func (s Store) VerifyForBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	nonce := build.Annotations[IntentAnnotation]
	if nonce == "" {
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
	return nil
}

// ReleaseForBuild runs only after active capacity has been durably committed,
// or when a matching terminal/deleting CR and its Pod cleanup are verified.
func (s Store) ReleaseForBuild(ctx context.Context, build *kovav1.KovaBuild) error {
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

// ReleaseRejected is for a definitive, non-persisted Create result only.
// It must never be called for a timeout, connection error, or server error.
func (s Store) ReleaseRejected(ctx context.Context, id, nonce string) error {
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
