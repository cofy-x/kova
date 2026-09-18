package buildcontroller

import (
	"context"
	"fmt"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *KovaBuildReconciler) startBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Cfg.RunnerImage == "" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ConfigurationInvalid", "runner image is required")
	}
	decision, err := r.admission(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !decision.Admitted {
		capacityWaits.Add(ctx, 1)
		build.Status.Phase = kovav1.PhaseQueued
		setPhaseCondition(build, kovav1.PhaseQueued, "WaitingForCapacity", decision.Message)
		build.Status.ObservedGeneration = build.Generation
		if err := r.Status().Update(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	podName := buildPodName(build.Name)
	pod := runner.PreparePod(runner.ManifestOptions{
		PodName:           podName,
		Namespace:         build.Namespace,
		Image:             r.Cfg.RunnerImage,
		ImagePullPolicy:   r.Cfg.RunnerImagePullPolicy,
		ImagePullSecret:   r.Cfg.RunnerImagePullSecret,
		BuildkitAddr:      r.Cfg.BuildkitAddr,
		NodeSelector:      r.Cfg.RunnerNodeSelector,
		Env:               r.Cfg.RunnerEnv,
		SourceURI:         build.Spec.Source.URI,
		SourceDigest:      build.Spec.Source.Digest,
		RegistryPlainHTTP: r.Cfg.RegistryPlainHTTP,
		Labels: map[string]string{
			"kova.cofy.dev/build-id": build.Name,
		},
	})
	if err := ctrl.SetControllerReference(build, &pod, r.Scheme); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateFailed", err.Error())
	}
	if err := r.Create(ctx, &pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateFailed", err.Error())
	}
	now := metav1.Now()
	jobQueueLatency.RecordDuration(ctx, time.Since(build.CreationTimestamp.Time))
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.ObservedGeneration = build.Generation
	build.Status.AllocatedConcurrency = int32(decision.Allocation)
	build.Status.RunnerPodName = podName
	build.Status.StartedAt = &now
	build.Status.Message = ""
	setPhaseCondition(build, kovav1.PhaseStarting, "RunnerCreated", "runner Pod was created")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *KovaBuildReconciler) cancelBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.Phase == kovav1.PhaseRunning && build.Status.RunnerPodName != "" {
		// Cancellation remains effective when the daemon is already unavailable;
		// deleting the runner Pod is the authoritative stop operation.
		_ = (runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}).CancelBuild(ctx, build)
	}
	if build.Status.RunnerPodName != "" {
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", "build was cancelled")
}

func (r *KovaBuildReconciler) submitWhenReady(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Status.RunnerPodName}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", "runner Pod disappeared before the build was submitted")
		}
		return ctrl.Result{}, err
	}
	if !podReady(&pod) {
		if message, failed := sourceFetchFailure(&pod); failed {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidSource", message)
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod terminated before becoming ready: %s", pod.Status.Phase))
		}
		if build.Status.StartedAt != nil && r.Cfg.WaitTimeout > 0 && time.Since(build.Status.StartedAt.Time) >= r.Cfg.WaitTimeout {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod did not become ready within %s", r.Cfg.WaitTimeout))
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	client := runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}
	sourceTargets, err := client.SourceTargets(ctx, build, sourcePath(build))
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidSource", err.Error())
	}
	if !buildcontract.EqualTargetSets(build.Spec.Targets, sourceTargets) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidTargets", "source targets do not exactly match requested targets")
	}
	if err := client.SubmitBuild(ctx, build, sourcePath(build)); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildSubmissionFailed", err.Error())
	}
	build.Status.Phase = kovav1.PhaseRunning
	build.Status.ObservedGeneration = build.Generation
	build.Status.Message = ""
	setPhaseCondition(build, kovav1.PhaseRunning, "BuildSubmitted", "build was submitted to the runner")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
}

func sourceFetchFailure(pod *corev1.Pod) (string, bool) {
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name != "source-fetch" || status.State.Terminated == nil || status.State.Terminated.ExitCode == 0 {
			continue
		}
		message := status.State.Terminated.Message
		if message == "" {
			message = status.State.Terminated.Reason
		}
		if message == "" {
			message = fmt.Sprintf("source fetch exited with code %d", status.State.Terminated.ExitCode)
		}
		return message, true
	}
	return "", false
}

func (r *KovaBuildReconciler) pollBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	client := runnerexec.Client{Kube: r.Kube, BuildkitAddr: r.Cfg.BuildkitAddr}
	state, err := client.BuildStatus(ctx, build)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", err.Error())
	}
	done, success, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
	}
	if !done {
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	if success {
		build.Status.Phase = kovav1.PhaseSucceeded
	} else if state.Status == "cancelled" {
		build.Status.Phase = kovav1.PhaseCancelled
	} else {
		build.Status.Phase = kovav1.PhaseFailed
	}
	resolved := buildresult.Resolve(ctx, client, build, r.Cfg.RegistryPlainHTTP)
	build.Status.Outputs = buildresult.Outputs(resolved)
	if success && !buildresult.AllSucceeded(resolved) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ResultVerificationFailed", "one or more build results could not be verified")
	}
	if success {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseSucceeded, "Completed", "")
	}
	if state.Status == "cancelled" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", state.Error)
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildFailed", state.Error)
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
	if controllerutil.RemoveFinalizer(build, cleanupFinalizer) {
		if err := r.Update(ctx, build); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}
