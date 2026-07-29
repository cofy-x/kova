package buildcontroller

import (
	"context"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/service/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerOptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const cleanupFinalizer = "kova.cofy.dev/cleanup"

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
	Store    artifactstore.Store
	Cfg      config.Config
	Recorder record.EventRecorder
}

func (r *KovaBuildReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var build kovav1.KovaBuild
	if err := r.Get(ctx, req.NamespacedName, &build); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !build.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &build)
	}
	if controllerutil.AddFinalizer(&build, cleanupFinalizer) {
		if err := r.Update(ctx, &build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if cancellationRequested(&build) && !isTerminalPhase(build.Status.Phase) {
		return r.cancelBuild(ctx, &build)
	}
	if isTerminalPhase(build.Status.Phase) {
		return r.reconcileTerminal(ctx, &build)
	}
	switch build.Status.Phase {
	case "", kovav1.PhaseQueued:
		return r.startBuild(ctx, &build)
	case kovav1.PhaseStarting:
		return r.submitWhenReady(ctx, &build)
	case kovav1.PhaseRunning:
		return r.pollBuild(ctx, &build)
	default:
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
}

func (r *KovaBuildReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kovav1.KovaBuild{}).
		Owns(&corev1.Pod{}).
		WithOptions(controllerOptions.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
