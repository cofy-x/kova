package buildcontroller

import (
	"context"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/kube"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/config"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/service/sourcestore"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const cleanupFinalizer = "kova.cofy.dev/cleanup"

type KovaBuildReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Kube   kube.API
	Cfg    config.Config
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
	if isTerminalPhase(build.Status.Phase) {
		return r.reconcileTerminal(ctx, &build)
	}
	if !build.Spec.Source.Ready {
		return ctrl.Result{RequeueAfter: time.Second}, nil
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
		Complete(r)
}

func (r *KovaBuildReconciler) startBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	podName := buildPodName(build.Name)
	pod := runner.PreparePod(runner.ManifestOptions{
		PodName:         podName,
		Namespace:       build.Namespace,
		Image:           r.Cfg.RunnerImage,
		ImagePullPolicy: r.Cfg.RunnerImagePullPolicy,
		ImagePullSecret: r.Cfg.RunnerImagePullSecret,
		BuildkitAddr:    r.Cfg.BuildkitAddr,
		NodeSelector:    r.Cfg.RunnerNodeSelector,
		SourcePVCClaim:  build.Spec.Source.PVC.ClaimName,
		SourceMountPath: r.sourceMountPath(build),
		SourceReadOnly:  true,
		Labels: map[string]string{
			"kova.cofy.dev/build-id": build.Name,
		},
	})
	if err := ctrl.SetControllerReference(build, &pod, r.Scheme); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	if err := r.Create(ctx, &pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	now := metav1.Now()
	build.Status.Phase = kovav1.PhaseStarting
	syncRequestStatus(build)
	build.Status.RunnerPodName = podName
	build.Status.StartedAt = &now
	build.Status.Message = ""
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *KovaBuildReconciler) submitWhenReady(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Status.RunnerPodName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return r.startBuild(ctx, build)
		}
		return ctrl.Result{}, err
	}
	if !podReady(&pod) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if err := (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).SubmitBuild(ctx, build, r.sourceMountPath(build)); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	build.Status.Phase = kovav1.PhaseRunning
	syncRequestStatus(build)
	build.Status.Message = ""
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
}

func (r *KovaBuildReconciler) pollBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	state, err := (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).BuildStatus(ctx, build)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	done, success, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, err.Error())
	}
	if !done {
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	if success {
		build.Status.Phase = kovav1.PhaseSucceeded
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
		if !buildresult.AllSucceeded(build.Status.Results) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "one or more build results could not be verified")
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseSucceeded, "")
	}
	if state.Status == "cancelled" {
		build.Status.Phase = kovav1.PhaseCancelled
		build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, state.Error)
	}
	build.Status.Phase = kovav1.PhaseFailed
	build.Status.Results = buildresult.Resolve(ctx, runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}, build)
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, state.Error)
}

func (r *KovaBuildReconciler) reconcileTerminal(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.FinishedAt == nil || r.Cfg.JobTTL <= 0 {
		return ctrl.Result{}, nil
	}
	remaining := time.Until(build.Status.FinishedAt.Add(r.Cfg.JobTTL))
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := r.Delete(ctx, build); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) reconcileDelete(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.RunnerPodName != "" {
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := sourcestore.Remove(r.Cfg.SourceRoot, build.Name); err != nil {
		return ctrl.Result{}, err
	}
	if controllerutil.RemoveFinalizer(build, cleanupFinalizer) {
		if err := r.Update(ctx, build); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) finish(ctx context.Context, build *kovav1.KovaBuild, phase string, message string) error {
	now := metav1.Now()
	build.Status.Phase = phase
	syncRequestStatus(build)
	build.Status.Message = message
	build.Status.FinishedAt = &now
	return r.Status().Update(ctx, build)
}

func syncRequestStatus(build *kovav1.KovaBuild) {
	build.Status.SourceDigest = build.Spec.SourceDigest
	build.Status.IdempotencyKey = build.Spec.IdempotencyKey
}

func (r *KovaBuildReconciler) sourceMountPath(build *kovav1.KovaBuild) string {
	if build.Spec.Source.PVC.MountPath != "" {
		return build.Spec.Source.PVC.MountPath
	}
	if r.Cfg.SourceMountPath != "" {
		return r.Cfg.SourceMountPath
	}
	return sourcestore.DefaultRoot
}

func buildPodName(name string) string {
	return "kova-job-" + name
}

func isTerminalPhase(phase string) bool {
	switch phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
