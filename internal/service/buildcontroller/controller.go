package buildcontroller

import (
	"context"
	"fmt"
	"sync"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/recoverydrain"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerOptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const cleanupFinalizer = kovav1.CleanupFinalizer

var (
	jobQueueLatency = observability.DurationHistogram("kova.service.job.queue.duration", "Time from job creation to runner admission")
	jobDuration     = observability.DurationHistogram("kova.service.job.duration", "Time from runner admission to terminal state")
	jobCompletions  = observability.Int64Counter("kova.service.job.completions", "Terminal service jobs")
	capacityWaits   = observability.Int64Counter("kova.service.capacity.waits", "Reconciliations delayed by capacity admission")
)

type KovaBuildReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Kube     kube.API
	Cfg      config.Config
	Recorder record.EventRecorder
	Genesis  *admissiongenesis.Guard
	// RecoveryReceipts is a direct typed client scoped to the immutable
	// dedicated receipt Namespace; it is not the manager's cached client.
	RecoveryReceipts ReceiptConfigMaps
	// APIReader bypasses the manager cache for capacity and recovery reads.
	APIReader         client.Reader
	verificationOnce  sync.Once
	verificationSlots chan struct{}
}

func (r *KovaBuildReconciler) queueStoreForNamespace(namespace string) queueadmission.Store {
	return queueadmission.Store{
		Client: r.Client, Reader: r.reader(), Genesis: r.Genesis, Namespace: namespace,
		GlobalLimit: r.Cfg.MaxQueuedJobs, RequesterLimit: r.Cfg.MaxQueuedJobsPerRequester,
	}
}

func (r *KovaBuildReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var build kovav1.KovaBuild
	if err := r.Get(ctx, req.NamespacedName, &build); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A recovery tombstone only occupies an old, pre-receipted name. It must
	// never acquire a runner, finalizer workflow, admission grant, or status.
	if recoverydrain.IsInertBuildTombstone(&build) {
		return ctrl.Result{}, nil
	}
	if !build.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &build)
	}
	if controllerutil.AddFinalizer(&build, cleanupFinalizer) {
		if r.Genesis != nil {
			if err := r.Genesis.Check(ctx); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.Update(ctx, &build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	// A persisted stop decision survives the UID delete and a lost terminal
	// status response. Resume it before any runner GET or fresh cancellation.
	if r.Genesis != nil && build.Status.AdmissionGenesisStopIntent != nil && !isTerminalPhase(build.Status.Phase) {
		return r.resumeGenesisStop(ctx, &build)
	}
	// Once the runner reported failure, cancellation cannot rewrite that
	// outcome; only bounded partial-receipt verification remains.
	if cancellationRequested(&build) && !isTerminalPhase(build.Status.Phase) && build.Status.Phase != kovav1.PhaseFailedVerifying {
		if r.Genesis != nil && build.Status.AdmissionGenesisWitness != nil {
			switch build.Status.Phase {
			case kovav1.PhaseStarting, kovav1.PhaseRunning, kovav1.PhaseVerifying:
				return r.reconcileGenesisCancellation(ctx, &build)
			}
		}
		return r.cancelBuild(ctx, &build)
	}
	if isTerminalPhase(build.Status.Phase) {
		return r.reconcileTerminal(ctx, &build)
	}
	switch build.Status.Phase {
	case "", kovav1.PhaseQueued:
		if r.Genesis != nil {
			if err := r.Genesis.Check(ctx); err != nil {
				return ctrl.Result{}, err
			}
		}
		if _, err := buildcontract.NormalizeTargetSpecs(contractTargets(build.Spec.Targets)); err != nil {
			return ctrl.Result{}, r.finish(ctx, &build, kovav1.PhaseFailed, "InvalidTargets", err.Error())
		}
		if err := buildcontract.ValidateConcurrency(requestedConcurrency(&build), len(build.Spec.Targets)); err != nil {
			return ctrl.Result{}, r.finish(ctx, &build, kovav1.PhaseFailed, "InvalidTargets", err.Error())
		}
		for _, target := range build.Spec.Targets {
			if _, ok := r.Cfg.BuildkitPlatformAddrs[target.Platform]; !ok {
				return ctrl.Result{}, r.finish(ctx, &build, kovav1.PhaseFailed, "WorkerPlatformUnavailable", "no BuildKit worker pool is configured for platform "+target.Platform)
			}
		}
		return r.startBuild(ctx, &build)
	case kovav1.PhaseStarting:
		return r.submitWhenReady(ctx, &build)
	case kovav1.PhaseRunning:
		return r.pollBuild(ctx, &build)
	case kovav1.PhaseVerifying:
		return r.reconcileVerifying(ctx, &build)
	case kovav1.PhaseFailedVerifying:
		return r.reconcileFailedVerifying(ctx, &build)
	default:
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
}

func contractTargets(values []kovav1.KovaBuildTargetSpec) []buildcontract.TargetSpec {
	targets := make([]buildcontract.TargetSpec, 0, len(values))
	for _, value := range values {
		targets = append(targets, buildcontract.TargetSpec{Target: value.Target, Platform: value.Platform})
	}
	return targets
}

func (r *KovaBuildReconciler) SetupWithManager(mgr ctrl.Manager, admissionWake <-chan event.GenericEvent) error {
	concurrency := r.Cfg.ControllerConcurrency
	if concurrency <= 0 {
		concurrency = buildcontract.DefaultControllerConcurrency
	}
	if admissionWake == nil {
		return fmt.Errorf("admission wake source is required for event-driven queued builds")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&kovav1.KovaBuild{}).
		Owns(&corev1.Pod{}).
		WatchesRawSource(source.Channel(admissionWake, &handler.EnqueueRequestForObject{})).
		WithOptions(controllerOptions.Options{MaxConcurrentReconciles: concurrency}).
		Complete(r)
}
