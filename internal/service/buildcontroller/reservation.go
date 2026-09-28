package buildcontroller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	AdmissionLedgerName       = "kova-service-admission"
	reservationConfigMap      = AdmissionLedgerName
	reservationDataKey        = "reservations.json"
	bootstrapAttemptKey       = "kova.cofy.dev/admission-bootstrap"
	podCreateAttemptKey       = "kova.cofy.dev/create-attempt"
	maxReservationCASAttempts = 16
)

type activeReservation struct {
	BuildName string   `json:"buildName"`
	Requester string   `json:"requester"`
	Slots     int      `json:"slots"`
	Closing   bool     `json:"closing,omitempty"`
	InFlight  []string `json:"inFlight,omitempty"`
}

type reservationState struct {
	Version                  int                          `json:"version"`
	Fence                    uint64                       `json:"fence"`
	MaxJobs                  int                          `json:"maxJobs"`
	MaxPerRequester          int                          `json:"maxPerRequester"`
	WorkerSlots              int                          `json:"workerSlots"`
	LastGrantedRequesterHash string                       `json:"lastGrantedRequesterHash,omitempty"`
	Active                   map[string]activeReservation `json:"active"`
}

var errAdmissionClosed = errors.New("KovaBuild is no longer eligible for runner admission")

type admissionRecoveryError struct {
	Namespace string
	BuildName string
	Pending   int
}

func (e *admissionRecoveryError) Error() string {
	return fmt.Sprintf("admission recovery required for %s/%s: %d Pod create attempt(s) have unknown outcome", e.Namespace, e.BuildName, e.Pending)
}

func reservationKey(build *kovav1.KovaBuild) string {
	if build.UID != "" {
		return string(build.UID)
	}
	// The API server always sets UID. The name fallback is for fake clients.
	return "name:" + build.Name
}

func (r *KovaBuildReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// A direct CR read must follow the active-ledger read on every CAS attempt.
// Terminal/deletion cleanup first changes that ledger's resourceVersion, so
// a writer that checked an older CR state either commits before cleanup sees
// its grant, or conflicts and rechecks this now-ineligible CR.
func (r *KovaBuildReconciler) ensureLiveAdmissionBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	var current kovav1.KovaBuild
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: build.Namespace, Name: build.Name}, &current); err != nil {
		return err
	}
	if current.UID != build.UID || !current.DeletionTimestamp.IsZero() || cancellationRequested(&current) || (current.Status.Phase != "" && current.Status.Phase != kovav1.PhaseQueued) {
		return fmt.Errorf("%w: %s/%s", errAdmissionClosed, build.Namespace, build.Name)
	}
	return nil
}

func (r *KovaBuildReconciler) readReservations(ctx context.Context, namespace string) (*corev1.ConfigMap, reservationState, error) {
	key := client.ObjectKey{Namespace: namespace, Name: reservationConfigMap}
	var cm corev1.ConfigMap
	if err := r.reader().Get(ctx, key, &cm); err != nil {
		return nil, reservationState{}, err
	}
	state, err := decodeReservations(&cm)
	if err == nil {
		err = r.validateReservationLimits(state)
	}
	return &cm, state, err
}

func (r *KovaBuildReconciler) initializeReservations(ctx context.Context, namespace string) (*corev1.ConfigMap, reservationState, bool, error) {
	var bootstrapNonce string
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, namespace)
		if err == nil {
			return cm, state, false, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, reservationState{}, false, err
		}
		// On a fresh namespace the active ledger is created before the queue
		// ledger. An existing queue ledger proves the active ledger existed
		// before: even an empty CR/Pod List cannot disprove a late Pod Create
		// using an in-flight nonce from that deleted ledger.
		var queueCM corev1.ConfigMap
		queueErr := r.reader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: queueadmission.ConfigMapName}, &queueCM)
		if queueErr == nil {
			// A concurrent first-start replica may have completed both writes
			// after our missing active GET. Re-read before declaring ledger loss.
			if cm, state, err := r.readReservations(ctx, namespace); err == nil {
				return cm, state, false, nil
			} else if !apierrors.IsNotFound(err) {
				return nil, reservationState{}, false, err
			}
			return nil, reservationState{}, false, fmt.Errorf("active admission ledger is absent while queue admission ledger exists in %s; inspect recovery evidence before migration", namespace)
		} else if !apierrors.IsNotFound(queueErr) {
			return nil, reservationState{}, false, queueErr
		}
		if err := PreflightAdmissionLedger(ctx, r.reader(), namespace, r.Cfg); err != nil {
			return nil, reservationState{}, false, err
		}
		state = r.freshReservations()
		data, err := json.Marshal(state)
		if err != nil {
			return nil, reservationState{}, false, err
		}
		if bootstrapNonce == "" {
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return nil, reservationState{}, false, err
			}
			bootstrapNonce = hex.EncodeToString(nonce[:])
		}
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: reservationConfigMap, Namespace: namespace, Annotations: map[string]string{bootstrapAttemptKey: bootstrapNonce}},
			Data:       map[string]string{reservationDataKey: string(data)},
		}
		if err := r.Create(ctx, cm); err != nil {
			if apierrors.IsAlreadyExists(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return nil, reservationState{}, false, err
				}
				continue
			}
			// A lost Create response may hide a committed active ledger. Only
			// a validating direct read of THIS nonce permits queue creation.
			// A peer's committed ledger only permits waiting for its queue.
			if cm, state, readErr := r.readReservations(ctx, namespace); readErr == nil {
				return cm, state, cm.Annotations[bootstrapAttemptKey] == bootstrapNonce, nil
			}
			return nil, reservationState{}, false, err
		}
		return cm, state, true, nil
	}
	return nil, reservationState{}, false, fmt.Errorf("active admission ledger is busy initializing in %s", namespace)
}

// PreflightAdmissionLedger checks first-start eligibility without mutation.
// Existing ledgers are validated in place; absent ledgers require no legacy
// builds or runners. It does not authorize repairing a half-existing pair.
func PreflightAdmissionLedger(ctx context.Context, reader client.Reader, namespace string, cfg config.Config) error {
	r := &KovaBuildReconciler{APIReader: reader, Cfg: cfg}
	if _, _, err := r.readReservations(ctx, namespace); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if cfg.MaxActiveJobs < 1 || cfg.MaxActiveJobsPerRequester < 1 || cfg.MaxActiveJobsPerRequester > cfg.MaxActiveJobs || cfg.WorkerSlots < 1 {
		return fmt.Errorf("active admission ledger has invalid replica limits")
	}
	var builds kovav1.KovaBuildList
	if err := reader.List(ctx, &builds, client.InNamespace(namespace), client.Limit(1)); err != nil {
		return err
	}
	if len(builds.Items) != 0 {
		return fmt.Errorf("active admission ledger is absent while KovaBuilds exist in %s; drain and migrate before admission", namespace)
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range pods.Items {
		if pods.Items[i].Labels["kova.cofy.dev/build-id"] != "" {
			return fmt.Errorf("active admission ledger is absent while runner Pods exist in %s; drain and migrate before admission", namespace)
		}
	}
	return nil
}

// EnsureAdmissionLedger initializes the active ledger before the HTTP listener
// opens. created is true only when this call's Create is observed committed;
// only that caller may finish queue bootstrap. Other callers must wait, never
// repair a missing queue. Subsequent readiness checks are read-only.
func EnsureAdmissionLedger(ctx context.Context, writer client.Client, reader client.Reader, namespace string, cfg config.Config) (created bool, err error) {
	r := &KovaBuildReconciler{Client: writer, APIReader: reader, Cfg: cfg}
	_, _, created, err = r.initializeReservations(ctx, namespace)
	return created, err
}

// CheckAdmissionLedger validates the authoritative active ledger and this
// replica's limits without creating a replacement for missing state.
func CheckAdmissionLedger(ctx context.Context, reader client.Reader, namespace string, cfg config.Config) error {
	var cm corev1.ConfigMap
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: reservationConfigMap}, &cm); err != nil {
		return err
	}
	state, err := decodeReservations(&cm)
	if err != nil {
		return err
	}
	return (&KovaBuildReconciler{Cfg: cfg}).validateReservationLimits(state)
}

func decodeReservations(cm *corev1.ConfigMap) (reservationState, error) {
	var state reservationState
	if err := json.Unmarshal([]byte(cm.Data[reservationDataKey]), &state); err != nil {
		return reservationState{}, fmt.Errorf("admission ledger %s/%s is invalid: %w", cm.Namespace, cm.Name, err)
	}
	if state.Version != 1 || state.Active == nil || state.MaxJobs < 0 || state.MaxPerRequester < 0 || state.WorkerSlots < 0 {
		return reservationState{}, fmt.Errorf("admission ledger %s/%s has an unsupported state", cm.Namespace, cm.Name)
	}
	if cursor := state.LastGrantedRequesterHash; cursor != "" {
		if len(cursor) != 64 || cursor != strings.ToLower(cursor) {
			return reservationState{}, fmt.Errorf("admission ledger %s/%s has an invalid requester cursor", cm.Namespace, cm.Name)
		}
		if _, err := hex.DecodeString(cursor); err != nil {
			return reservationState{}, fmt.Errorf("admission ledger %s/%s has an invalid requester cursor", cm.Namespace, cm.Name)
		}
	}
	for key, entry := range state.Active {
		if key == "" || entry.BuildName == "" || entry.Requester == "" || entry.Slots < 1 {
			return reservationState{}, fmt.Errorf("admission ledger %s/%s has an invalid active reservation", cm.Namespace, cm.Name)
		}
		seen := map[string]bool{}
		for _, attempt := range entry.InFlight {
			if attempt == "" || seen[attempt] {
				return reservationState{}, fmt.Errorf("admission ledger %s/%s has an invalid Pod create attempt", cm.Namespace, cm.Name)
			}
			seen[attempt] = true
		}
	}
	return state, nil
}

func (r *KovaBuildReconciler) validateReservationLimits(state reservationState) error {
	if state.MaxJobs != r.Cfg.MaxActiveJobs || state.MaxPerRequester != r.Cfg.MaxActiveJobsPerRequester || state.WorkerSlots != r.Cfg.WorkerSlots {
		return fmt.Errorf("active admission ledger limits differ from this Service replica")
	}
	return nil
}

func (r *KovaBuildReconciler) freshReservations() reservationState {
	return reservationState{Version: 1, MaxJobs: r.Cfg.MaxActiveJobs, MaxPerRequester: r.Cfg.MaxActiveJobsPerRequester, WorkerSlots: r.Cfg.WorkerSlots, Active: map[string]activeReservation{}}
}

func (r *KovaBuildReconciler) writeReservations(ctx context.Context, cm *corev1.ConfigMap, state reservationState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	copy := cm.DeepCopy()
	copy.Data = map[string]string{reservationDataKey: string(data)}
	return r.Update(ctx, copy)
}

func waitReservationCAS(ctx context.Context, retry int) error {
	delay := time.Duration(1<<min(retry, 5)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// fenceReservation is a bounded global RV fence, not an unbounded per-build
// tombstone. It also closes any current grant before Pod inspection: no new
// Pod Create nonce may be appended during cleanup. It must CAS even when no
// grant exists, so a paused old leader's pre-fence admission CAS conflicts.
func (r *KovaBuildReconciler) fenceReservation(ctx context.Context, build *kovav1.KovaBuild) error {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return err
		}
		if state.Fence == ^uint64(0) {
			return fmt.Errorf("active admission fence counter is exhausted in %s", build.Namespace)
		}
		state.Fence++
		if entry, ok := state.Active[reservationKey(build)]; ok {
			if entry.BuildName != build.Name {
				return fmt.Errorf("admission reservation for %s/%s has mismatched build name %q", build.Namespace, build.Name, entry.BuildName)
			}
			entry.Closing = true
			state.Active[reservationKey(build)] = entry
		}
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("active admission ledger is busy fencing %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) releaseReservation(ctx context.Context, build *kovav1.KovaBuild) error {
	key := client.ObjectKey{Namespace: build.Namespace, Name: reservationConfigMap}
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var cm corev1.ConfigMap
		if err := r.reader().Get(ctx, key, &cm); err != nil {
			return client.IgnoreNotFound(err)
		}
		state, err := decodeReservations(&cm)
		if err != nil {
			return err
		}
		if err := r.validateReservationLimits(state); err != nil {
			return err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok {
			return nil
		}
		if entry.BuildName != build.Name {
			return fmt.Errorf("admission reservation for %s/%s has mismatched build name %q", build.Namespace, build.Name, entry.BuildName)
		}
		if !entry.Closing {
			return fmt.Errorf("admission reservation for %s/%s cannot be released before its cleanup fence", build.Namespace, build.Name)
		}
		if len(entry.InFlight) != 0 {
			return &admissionRecoveryError{Namespace: build.Namespace, BuildName: build.Name, Pending: len(entry.InFlight)}
		}
		if pod, err := r.getOwnedPod(ctx, build); err != nil {
			return err
		} else if pod != nil {
			return fmt.Errorf("runner Pod %s/%s still exists while releasing active capacity", pod.Namespace, pod.Name)
		}
		delete(state.Active, reservationKey(build))
		if err := r.writeReservations(ctx, &cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("active admission ledger is busy releasing %s/%s", build.Namespace, build.Name)
}

func newPodCreateAttempt() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// beginPodCreate is a durable in-flight fence. A replacement leader cannot
// release the capacity while this API Create might still reach the apiserver.
func (r *KovaBuildReconciler) beginPodCreate(ctx context.Context, build *kovav1.KovaBuild) (string, error) {
	attempt, err := newPodCreateAttempt()
	if err != nil {
		return "", err
	}
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return "", err
		}
		if err := r.ensureLiveAdmissionBuild(ctx, build); err != nil {
			return "", err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || entry.BuildName != build.Name {
			return "", fmt.Errorf("KovaBuild %s/%s has no matching active reservation", build.Namespace, build.Name)
		}
		if entry.Closing {
			return "", fmt.Errorf("%w: %s/%s grant is closing", errAdmissionClosed, build.Namespace, build.Name)
		}
		if len(entry.InFlight) != 0 {
			return "", &admissionRecoveryError{Namespace: build.Namespace, BuildName: build.Name, Pending: len(entry.InFlight)}
		}
		entry.InFlight = append(entry.InFlight, attempt)
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return "", err
				}
				continue
			}
			// A lost Update response is not proof that the nonce was absent.
			// If the authoritative read sees this exact nonce, its fence is
			// committed and this caller may safely issue its Pod Create.
			if recorded, readErr := r.podCreateAttemptRecorded(ctx, build, attempt); readErr == nil && recorded {
				return attempt, nil
			}
			return "", err
		}
		return attempt, nil
	}
	return "", fmt.Errorf("active admission ledger is busy starting Pod create for %s/%s", build.Namespace, build.Name)
}

// completePodCreate is called only after definitive Create success/rejection,
// AlreadyExists, or observation of the Pod with this exact nonce.
func (r *KovaBuildReconciler) completePodCreate(ctx context.Context, build *kovav1.KovaBuild, attempt string) error {
	for retry := 0; retry < maxReservationCASAttempts; retry++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cm, state, err := r.readReservations(ctx, build.Namespace)
		if err != nil {
			return err
		}
		entry, ok := state.Active[reservationKey(build)]
		if !ok || entry.BuildName != build.Name {
			return fmt.Errorf("KovaBuild %s/%s lost its active reservation while completing Pod create", build.Namespace, build.Name)
		}
		index := -1
		for i, value := range entry.InFlight {
			if value == attempt {
				index = i
				break
			}
		}
		if index < 0 {
			return nil
		}
		entry.InFlight = append(entry.InFlight[:index], entry.InFlight[index+1:]...)
		state.Active[reservationKey(build)] = entry
		if err := r.writeReservations(ctx, cm, state); err != nil {
			if apierrors.IsConflict(err) {
				if err := waitReservationCAS(ctx, retry); err != nil {
					return err
				}
				continue
			}
			if recorded, readErr := r.podCreateAttemptRecorded(ctx, build, attempt); readErr == nil && !recorded {
				return nil
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("active admission ledger is busy completing Pod create for %s/%s", build.Namespace, build.Name)
}

func (r *KovaBuildReconciler) podCreateAttemptRecorded(ctx context.Context, build *kovav1.KovaBuild, attempt string) (bool, error) {
	_, state, err := r.readReservations(ctx, build.Namespace)
	if err != nil {
		return false, err
	}
	entry, ok := state.Active[reservationKey(build)]
	if !ok || entry.BuildName != build.Name {
		return false, fmt.Errorf("KovaBuild %s/%s has no matching active reservation", build.Namespace, build.Name)
	}
	for _, value := range entry.InFlight {
		if value == attempt {
			return true, nil
		}
	}
	return false, nil
}

func podOwnedByBuild(pod *corev1.Pod, build *kovav1.KovaBuild) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == kovav1.Group+"/"+kovav1.Version && owner.Kind == "KovaBuild" && owner.Name == build.Name && owner.UID == build.UID {
			return true
		}
	}
	return false
}

func (r *KovaBuildReconciler) ensureNoUnreservedRunner(ctx context.Context, namespace string, state reservationState, builds []kovav1.KovaBuild) error {
	byName := make(map[string]*kovav1.KovaBuild, len(builds))
	for i := range builds {
		build := &builds[i]
		byName[build.Name] = build
		if build.Status.Phase == kovav1.PhaseStarting || build.Status.Phase == kovav1.PhaseRunning || build.Status.Phase == kovav1.PhaseVerifying || build.Status.Phase == kovav1.PhaseFailedVerifying {
			covered, err := r.reservationCovered(ctx, namespace, state, build)
			if err != nil {
				return err
			}
			if !covered {
				return fmt.Errorf("active KovaBuild %s/%s has no admission reservation", namespace, build.Name)
			}
		}
	}
	var pods corev1.PodList
	if err := r.reader().List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		name := pod.Labels["kova.cofy.dev/build-id"]
		if name == "" {
			continue
		}
		build := byName[name]
		if build == nil || !podOwnedByBuild(pod, build) {
			return fmt.Errorf("runner Pod %s/%s has no matching KovaBuild owner", namespace, pod.Name)
		}
		covered, err := r.reservationCovered(ctx, namespace, state, build)
		if err != nil {
			return err
		}
		if !covered {
			return fmt.Errorf("runner Pod %s/%s has no admission reservation", namespace, pod.Name)
		}
	}
	return nil
}

func (r *KovaBuildReconciler) reservationCovered(ctx context.Context, namespace string, state reservationState, build *kovav1.KovaBuild) (bool, error) {
	if entry, ok := state.Active[reservationKey(build)]; ok {
		return entry.BuildName == build.Name, nil
	}
	// The Pod/status LIST may be newer than our ledger GET. Re-read the
	// authoritative ledger before declaring drift; a concurrent grant must
	// have committed before the Pod/status became visible.
	var latest corev1.ConfigMap
	if err := r.reader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: reservationConfigMap}, &latest); err != nil {
		return false, err
	}
	current, err := decodeReservations(&latest)
	if err != nil {
		return false, err
	}
	entry, ok := current.Active[reservationKey(build)]
	return ok && entry.BuildName == build.Name, nil
}

func (r *KovaBuildReconciler) getOwnedPod(ctx context.Context, build *kovav1.KovaBuild) (*corev1.Pod, error) {
	var pod corev1.Pod
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: buildPodName(build.Name)}, &pod)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !podOwnedByBuild(&pod, build) {
		return nil, fmt.Errorf("runner Pod %s/%s is not owned by KovaBuild UID %s", pod.Namespace, pod.Name, build.UID)
	}
	return &pod, nil
}
