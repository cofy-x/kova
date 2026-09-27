package buildcontroller

import (
	"context"
	"fmt"
	"sort"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerOptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	admissionAuditRequestName  = "kova-service-admission-audit"
	admissionAuditInterval     = 30 * time.Second
	admissionAuditGrantBatch   = 32
	admissionAuditContinuation = 4 * time.Second
	admissionWakeTimeout       = time.Second
)

// AdmissionPump turns durable capacity changes into one targeted KovaBuild
// wakeup. It never grants capacity itself: the woken build still performs the
// direct CR/Pod checks and commits its active grant and fairness cursor in one
// resourceVersion CAS. No individual queued build needs a capacity timer.
type AdmissionPump struct {
	Reader client.Reader
	Cfg    config.Config
	wake   chan event.GenericEvent
	kick   chan event.GenericEvent
	// Repeated CR Create/status events can all point at the same fair head.
	// This is a leader-local traffic hint, never the source of grant truth.
	lastWakeUID      types.UID
	lastWakeLedgerRV string
	lastWakeAt       time.Time
	// At most one bounded slice of active grants is inspected per audit. The
	// cursor is advisory; the next leader starts from the beginning again.
	auditCursor string
	// On leader startup or a lost-event audit, continue the bounded slices
	// promptly until a complete pass over the initial active set is done.
	auditRemaining int
}

func NewAdmissionPump(reader client.Reader, cfg config.Config) *AdmissionPump {
	return &AdmissionPump{
		Reader: reader, Cfg: cfg,
		wake: make(chan event.GenericEvent, 64),
		kick: make(chan event.GenericEvent, 1),
	}
}

func (p *AdmissionPump) WakeEvents() <-chan event.GenericEvent { return p.wake }

func (p *AdmissionPump) SetupWithManager(mgr ctrl.Manager) error {
	if p.Reader == nil || p.Cfg.Namespace == "" {
		return fmt.Errorf("admission pump requires an API reader and namespace")
	}
	requestForBuild := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []ctrl.Request {
		if obj.GetNamespace() != p.Cfg.Namespace {
			return nil
		}
		return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: p.Cfg.Namespace, Name: reservationConfigMap}}}
	})
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("kova_admission_pump").
		For(&corev1.ConfigMap{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			return obj.GetNamespace() == p.Cfg.Namespace && obj.GetName() == reservationConfigMap
		}))).
		Watches(&kovav1.KovaBuild{}, requestForBuild, builder.WithPredicates(admissionBuildEvents(p.Cfg.Namespace))).
		WatchesRawSource(source.Channel(p.kick, &handler.EnqueueRequestForObject{})).
		WithOptions(controllerOptions.Options{MaxConcurrentReconciles: 1}).
		Complete(p); err != nil {
		return err
	}
	return mgr.Add(&admissionAuditTicker{namespace: p.Cfg.Namespace, kick: p.kick})
}

func admissionBuildEvents(namespace string) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object != nil && e.Object.GetNamespace() == namespace
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, oldOK := e.ObjectOld.(*kovav1.KovaBuild)
			current, currentOK := e.ObjectNew.(*kovav1.KovaBuild)
			if !oldOK || !currentOK || current.Namespace != namespace {
				return false
			}
			queuedTransition := old.Status.Phase != current.Status.Phase &&
				(old.Status.Phase == "" || old.Status.Phase == kovav1.PhaseQueued || current.Status.Phase == kovav1.PhaseQueued)
			return queuedTransition ||
				old.DeletionTimestamp.IsZero() != current.DeletionTimestamp.IsZero() ||
				cancellationRequested(old) != cancellationRequested(current)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object != nil && e.Object.GetNamespace() == namespace
		},
	}
}

func (p *AdmissionPump) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Namespace != p.Cfg.Namespace || (req.Name != reservationConfigMap && req.Name != admissionAuditRequestName) {
		return ctrl.Result{}, nil
	}
	// A missing or changed ledger is not an invitation to reconstruct it from
	// cache. Both ledgers remain authoritative across leader transitions.
	r := KovaBuildReconciler{APIReader: p.Reader, Cfg: p.Cfg}
	ledger, state, err := r.readReservations(ctx, p.Cfg.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	result := ctrl.Result{}
	if req.Name == admissionAuditRequestName {
		if p.auditRemaining == 0 {
			p.auditRemaining = len(state.Active)
		}
		inspected, err := p.wakeGrantedBuilds(ctx, state, p.auditRemaining)
		p.auditRemaining -= inspected
		if err != nil {
			return ctrl.Result{}, err
		}
		if len(state.Active) == 0 {
			p.auditRemaining = 0
		}
		if p.auditRemaining > 0 {
			result.RequeueAfter = admissionAuditContinuation
		}
	}
	if activeCapacityExhausted(state.Active, p.Cfg.MaxActiveJobs, p.Cfg.WorkerSlots) {
		// A capacity event cannot grant while the active ledger is full.
		// Do not fetch and decode the potentially 1000-entry queue ledger on
		// every new CR/status event. HTTP admission and /readyz still check it;
		// the pump validates it before any possible grant.
		return result, nil
	}
	queue := queueadmission.Store{Reader: p.Reader, Namespace: p.Cfg.Namespace,
		GlobalLimit: p.Cfg.MaxQueuedJobs, RequesterLimit: p.Cfg.MaxQueuedJobsPerRequester}
	if err := queue.CheckReady(ctx); err != nil {
		return ctrl.Result{}, err
	}
	var builds kovav1.KovaBuildList
	if err := p.Reader.List(ctx, &builds, client.InNamespace(p.Cfg.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	// The pump is an optimization of who wakes, never a relaxation of the
	// existing full-scan orphan and active-reservation safety check.
	if err := r.ensureNoUnreservedRunner(ctx, p.Cfg.Namespace, state, builds.Items); err != nil {
		return ctrl.Result{}, err
	}
	candidate := nextAdmissionCandidate(builds.Items, state, p.Cfg)
	if candidate == nil {
		return result, nil
	}
	var current kovav1.KovaBuild
	if err := p.Reader.Get(ctx, client.ObjectKeyFromObject(candidate), &current); err != nil {
		return ctrl.Result{}, err
	}
	if current.UID != candidate.UID || !current.DeletionTimestamp.IsZero() || cancellationRequested(&current) ||
		(current.Status.Phase != "" && current.Status.Phase != kovav1.PhaseQueued) {
		// A concurrent update invalidated this selection. Its watch event
		// normally retries the pump; the bounded audit covers a lost event.
		return result, nil
	}
	if err := queue.VerifyForBuild(ctx, &current); err != nil {
		// Let the build record AdmissionRecoveryRequired, but do not wake a
		// later fair-share candidate or grant around an unexplained drift.
		if wakeErr := p.wakeBuild(ctx, &current); wakeErr != nil {
			return ctrl.Result{}, wakeErr
		}
		return ctrl.Result{}, err
	}
	if p.lastWakeUID == current.UID && p.lastWakeLedgerRV == ledger.ResourceVersion && time.Since(p.lastWakeAt) < admissionAuditInterval {
		return result, nil
	}
	if err := p.wakeBuild(ctx, &current); err != nil {
		return ctrl.Result{}, err
	}
	p.lastWakeUID, p.lastWakeLedgerRV, p.lastWakeAt = current.UID, ledger.ResourceVersion, time.Now()
	return result, nil
}

func activeCapacityExhausted(active map[string]activeReservation, maxJobs, workerSlots int) bool {
	if maxJobs > 0 && len(active) >= maxJobs {
		return true
	}
	used := 0
	for _, entry := range active {
		used += entry.Slots
	}
	return workerSlots > 0 && used >= workerSlots
}

func nextAdmissionCandidate(builds []kovav1.KovaBuild, state reservationState, cfg config.Config) *kovav1.KovaBuild {
	if activeCapacityExhausted(state.Active, cfg.MaxActiveJobs, cfg.WorkerSlots) {
		return nil
	}
	activeByRequester := make(map[string]int)
	for _, entry := range state.Active {
		activeByRequester[entry.Requester]++
	}
	for _, candidate := range fairQueue(queuedAdmissionCandidates(builds, state.Active), activeByRequester, state.LastGrantedRequesterHash) {
		if cfg.MaxActiveJobsPerRequester > 0 && activeByRequester[requesterKey(candidate)] >= cfg.MaxActiveJobsPerRequester {
			continue
		}
		return candidate
	}
	return nil
}

func (p *AdmissionPump) wakeGrantedBuilds(ctx context.Context, state reservationState, remaining int) (int, error) {
	keys := make([]string, 0, len(state.Active))
	for key := range state.Active {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		p.auditCursor = ""
		return 0, nil
	}
	sort.Strings(keys)
	start := sort.Search(len(keys), func(i int) bool { return keys[i] > p.auditCursor })
	if start == len(keys) {
		start = 0
	}
	inspected := 0
	for offset := 0; offset < min(len(keys), admissionAuditGrantBatch, remaining); offset++ {
		key := keys[(start+offset)%len(keys)]
		entry := state.Active[key]
		var build kovav1.KovaBuild
		if err := p.Reader.Get(ctx, types.NamespacedName{Namespace: p.Cfg.Namespace, Name: entry.BuildName}, &build); err != nil {
			return inspected, fmt.Errorf("active admission grant %q has no readable KovaBuild: %w", key, err)
		}
		if reservationKey(&build) != key || requesterKey(&build) != entry.Requester {
			return inspected, fmt.Errorf("active admission grant %q does not match its KovaBuild identity", key)
		}
		// Only an unfinished grant (or a fenced cleanup) needs a wake after
		// leader change. Running builds keep their own lifecycle reconciliation.
		if build.Status.Phase == "" || build.Status.Phase == kovav1.PhaseQueued ||
			entry.Closing || len(entry.InFlight) > 0 {
			if err := p.wakeBuild(ctx, &build); err != nil {
				return inspected, err
			}
		}
		p.auditCursor = key
		inspected++
	}
	return inspected, nil
}

func (p *AdmissionPump) wakeBuild(ctx context.Context, build *kovav1.KovaBuild) error {
	timer := time.NewTimer(admissionWakeTimeout)
	defer timer.Stop()
	e := event.GenericEvent{Object: &kovav1.KovaBuild{ObjectMeta: build.ObjectMeta}}
	select {
	case p.wake <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("targeted admission wake channel is full")
	}
}

type admissionAuditTicker struct {
	namespace string
	kick      chan<- event.GenericEvent
}

func (*admissionAuditTicker) NeedLeaderElection() bool { return true }

func (t *admissionAuditTicker) Start(ctx context.Context) error {
	ticker := time.NewTicker(admissionAuditInterval)
	defer ticker.Stop()
	for {
		e := event.GenericEvent{Object: &corev1.ConfigMap{}}
		e.Object.SetNamespace(t.namespace)
		e.Object.SetName(admissionAuditRequestName)
		// A pending audit is enough: controller-runtime de-duplicates the
		// synthetic key, and the next tick recovers a lost watch event.
		select {
		case t.kick <- e:
		default:
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
