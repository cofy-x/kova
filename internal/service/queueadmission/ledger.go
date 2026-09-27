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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ConfigMapName    = "kova-service-queue-admission"
	IntentAnnotation = "kova.cofy.dev/queue-intent"
	dataKey          = "queue.json"
	maxLedgerBytes   = 768 * 1024
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

func (s Store) read(ctx context.Context) (*corev1.ConfigMap, state, error) {
	var cm corev1.ConfigMap
	if err := s.reader().Get(ctx, s.key(), &cm); err != nil {
		return nil, state{}, err
	}
	if len(cm.Data[dataKey]) > maxLedgerBytes {
		return nil, state{}, fmt.Errorf("queue ledger %s exceeds its size guard", s.key())
	}
	var result state
	if err := json.Unmarshal([]byte(cm.Data[dataKey]), &result); err != nil {
		return nil, state{}, fmt.Errorf("queue ledger %s is invalid: %w", s.key(), err)
	}
	if result.Version != 1 || result.Intents == nil || result.GlobalLimit != s.GlobalLimit || result.RequesterLimit != s.RequesterLimit || s.GlobalLimit < 1 || s.GlobalLimit > 1000 || s.RequesterLimit < 1 || s.RequesterLimit > s.GlobalLimit || len(result.Intents) > s.GlobalLimit {
		return nil, state{}, fmt.Errorf("queue ledger %s has invalid state or mismatched replica limits", s.key())
	}
	requesterCounts := make(map[string]int)
	for id, entry := range result.Intents {
		if id == "" || len(entry.RequesterHash) != 64 || len(entry.RequestDigest) != 64 || len(entry.Nonce) != 32 || entry.CreatedAtUnix < 1 {
			return nil, state{}, fmt.Errorf("queue ledger %s has an invalid intent", s.key())
		}
		requesterCounts[entry.RequesterHash]++
		if requesterCounts[entry.RequesterHash] > s.RequesterLimit {
			return nil, state{}, fmt.Errorf("queue ledger %s exceeds its per-requester limit", s.key())
		}
	}
	return &cm, result, nil
}

func (s Store) initialize(ctx context.Context) error {
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
	data, err := json.Marshal(s.freshState())
	if err != nil {
		return err
	}
	cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: s.Namespace}, Data: map[string]string{dataKey: string(data)}}
	return s.Client.Create(ctx, &cm)
}

func (s Store) write(ctx context.Context, cm *corev1.ConfigMap, next state) error {
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if len(data) > maxLedgerBytes {
		return fmt.Errorf("queue ledger would exceed %d bytes", maxLedgerBytes)
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
		if apierrors.IsNotFound(err) {
			if err := s.initialize(ctx); err != nil && !apierrors.IsAlreadyExists(err) {
				if apierrors.IsTooManyRequests(err) {
					return Intent{}, false, ErrBusy
				}
				return Intent{}, false, err
			}
			continue
		}
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
	if apierrors.IsNotFound(err) {
		return Intent{}, false, nil
	}
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
