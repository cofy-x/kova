package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const terminalObservationTimeout = 5 * time.Second

func (r *KovaBuildReconciler) startBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Cfg.RunnerImage == "" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ConfigurationInvalid", "runner image is required")
	}
	// Reconcile's object may be stale. A direct read also gives Status.Update
	// the latest resourceVersion after a previous uncertain write.
	var current kovav1.KovaBuild
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Name}, &current); err != nil {
		return ctrl.Result{}, err
	}
	if current.UID != build.UID || !current.DeletionTimestamp.IsZero() || cancellationRequested(&current) || (current.Status.Phase != "" && current.Status.Phase != kovav1.PhaseQueued) {
		return ctrl.Result{Requeue: true}, nil
	}
	build = &current
	decision, err := r.admission(ctx, build)
	if err != nil {
		if errors.Is(err, queueadmission.ErrDrift) {
			if statusErr := r.markAdmissionRecoveryReason(ctx, build, "QueueIntentDrift", "queue intent does not match the build; active admission is blocked"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if !decision.Admitted {
		capacityWaits.Add(ctx, 1)
		build.Status.Phase = kovav1.PhaseQueued
		apiMeta.RemoveStatusCondition(&build.Status.Conditions, admissionRecoveryCondition)
		setPhaseCondition(build, kovav1.PhaseQueued, "WaitingForCapacity", decision.Message)
		build.Status.ObservedGeneration = build.Generation
		if err := r.Status().Update(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.Cfg.PollInterval}, nil
	}
	// The active grant is already durable. Removing its matching queued
	// intent afterward can temporarily double-count capacity, never overbook.
	if err := r.queueStoreForNamespace(build.Namespace).ReleaseForBuild(ctx, build); err != nil {
		if errors.Is(err, queueadmission.ErrDrift) {
			if statusErr := r.markAdmissionRecoveryReason(ctx, build, "QueueIntentDrift", "queue intent does not match the build; release is blocked"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	podName := buildPodName(build.Name)
	pod := runner.PreparePod(runner.ManifestOptions{
		PodName:               podName,
		Namespace:             build.Namespace,
		Image:                 r.Cfg.RunnerImage,
		ImagePullPolicy:       r.Cfg.RunnerImagePullPolicy,
		ImagePullSecret:       r.Cfg.RunnerImagePullSecret,
		NodeSelector:          r.Cfg.RunnerNodeSelector,
		Env:                   r.Cfg.RunnerEnv,
		RunnerResources:       r.Cfg.RunnerResources,
		SourceFetchResources:  r.Cfg.SourceFetchResources,
		SourceVolumeSizeLimit: r.Cfg.SourceVolumeSizeLimit,
		SourceURI:             build.Spec.Source.URI,
		SourceDigest:          build.Spec.Source.Digest,
		RegistryPlainHTTP:     r.Cfg.RegistryPlainHTTP,
		Labels: map[string]string{
			"kova.cofy.dev/build-id": build.Name,
		},
	})
	if r.Cfg.MaxBuildDuration > 0 {
		podDuration := r.Cfg.MaxBuildDuration + r.verificationWindow()
		seconds := int64(podDuration / time.Second)
		if podDuration%time.Second != 0 {
			seconds++
		}
		pod.Spec.ActiveDeadlineSeconds = &seconds
	}
	if err := ctrl.SetControllerReference(build, &pod, r.Scheme); err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateFailed", err.Error())
	}
	owned, err := r.getOwnedPod(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if owned != nil {
		// A previous Create may have persisted before its response or the
		// Starting status update was lost. The stamped nonce proves which
		// attempt reached storage; no second Create is needed.
		if observed := owned.Annotations[podCreateAttemptKey]; observed != "" {
			if err := r.completePodCreate(ctx, build, observed); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		attempt, err := r.beginPodCreate(ctx, build)
		if err != nil {
			var recovery *admissionRecoveryError
			if errors.As(err, &recovery) {
				if statusErr := r.markAdmissionRecovery(ctx, build, recovery.Pending); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
			}
			return ctrl.Result{}, err
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[podCreateAttemptKey] = attempt
		if err := r.Create(ctx, &pod); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateOutcomeUnknown", err.Error())
			}
			owned, getErr := r.getOwnedPod(ctx, build)
			if getErr != nil {
				return ctrl.Result{}, getErr
			}
			if owned == nil {
				return ctrl.Result{}, fmt.Errorf("runner Pod %s/%s disappeared after AlreadyExists", build.Namespace, podName)
			}
			if err := r.completePodCreate(ctx, build, attempt); err != nil {
				return ctrl.Result{}, err
			}
			if observed := owned.Annotations[podCreateAttemptKey]; observed != "" {
				if err := r.completePodCreate(ctx, build, observed); err != nil {
					return ctrl.Result{}, err
				}
			}
		} else if err := r.completePodCreate(ctx, build, attempt); err != nil {
			return ctrl.Result{}, err
		}
	}
	now := metav1.Now()
	jobQueueLatency.RecordDuration(ctx, time.Since(build.CreationTimestamp.Time))
	build.Status.Phase = kovav1.PhaseStarting
	build.Status.ObservedGeneration = build.Generation
	build.Status.AllocatedConcurrency = int32(decision.Allocation)
	build.Status.RunnerPodName = podName
	build.Status.StartedAt = &now
	build.Status.Message = ""
	apiMeta.RemoveStatusCondition(&build.Status.Conditions, admissionRecoveryCondition)
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
		cancelCtx, stop := context.WithTimeout(ctx, r.verificationAttemptTimeout())
		_ = (runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}).CancelBuild(cancelCtx, build)
		stop()
	}
	if err := r.deleteRunnerAndConfirm(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", "build was cancelled")
}

func (r *KovaBuildReconciler) submitWhenReady(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	client := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}
	if expired, result, err := r.reconcileExpiredBuild(ctx, build, client); expired {
		return result, err
	}
	var pod corev1.Pod
	if err := r.getRunnerPod(ctx, build, &pod); err != nil {
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
		return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, time.Second)}, nil
	}
	state, err := r.observeBuildStatus(ctx, client, build)
	if err != nil {
		if errors.Is(err, runnerexec.ErrInvalidBuildStatus) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
		}
		return r.retryStatusObservation(ctx, build, err)
	}
	if state.Status != "idle" {
		return r.markSubmitted(ctx, build, state)
	}
	if !state.SupportsIdempotentBuildRequest() {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolIncompatible", "idle runner does not advertise idempotent build submission; drain Starting jobs and replace the legacy runner before upgrading the controller")
	}
	if err := r.clearPollFailure(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	operationCtx := ctx
	cancelOperation := func() {}
	if r.Cfg.MaxBuildDuration > 0 && build.Status.StartedAt != nil {
		operationCtx, cancelOperation = context.WithDeadline(ctx, build.Status.StartedAt.Add(r.Cfg.MaxBuildDuration))
	}
	defer cancelOperation()
	sourceTargets, err := client.SourceTargets(operationCtx, build, sourcePath(build))
	if err != nil {
		if errors.Is(err, runnerexec.ErrSourceInspectTransport) {
			return r.retryStatusObservation(ctx, build, err)
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidSource", err.Error())
	}
	if !buildcontract.EqualTargetSpecSets(contractTargets(build.Spec.Targets), sourceTargets) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidTargets", "source targets do not exactly match requested targets")
	}
	if expired, result, err := r.reconcileExpiredBuild(ctx, build, client); expired {
		return result, err
	}
	if err := client.SubmitBuild(operationCtx, build, sourcePath(build)); err != nil {
		// The POST can be accepted even when exec loses its response. The
		// runner's request ID makes the next submission safe if observation
		// is also unavailable.
		state, statusErr := r.observeBuildStatus(ctx, client, build)
		if statusErr != nil {
			if errors.Is(statusErr, runnerexec.ErrInvalidBuildStatus) {
				return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", statusErr.Error())
			}
			return r.retryStatusObservation(ctx, build, statusErr)
		}
		if state.Status == "idle" {
			return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, time.Second)}, nil
		}
		return r.markSubmitted(ctx, build, state)
	}
	return r.markSubmitted(ctx, build, runner.BuildState{Status: "running"})
}

func (r *KovaBuildReconciler) markSubmitted(ctx context.Context, build *kovav1.KovaBuild, state runner.BuildState) (ctrl.Result, error) {
	if state.RequestID != "" && state.RequestID != runnerexec.RequestID(build) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", "runner reported a different build request")
	}
	done, _, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
	}
	if done {
		return r.finishObservedBuild(ctx, build, runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}, state)
	}
	build.Status.Phase = kovav1.PhaseRunning
	build.Status.ObservedGeneration = build.Generation
	build.Status.Message = ""
	build.Status.PollFailureSince = nil
	build.Status.PollFailureCount = 0
	setPhaseCondition(build, kovav1.PhaseRunning, "BuildSubmitted", "build was submitted to the runner")
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, r.Cfg.PollInterval)}, nil
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
	client := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}
	if expired, result, err := r.reconcileExpiredBuild(ctx, build, client); expired {
		return result, err
	}
	state, err := r.observeBuildStatus(ctx, client, build)
	if err != nil {
		if errors.Is(err, runnerexec.ErrInvalidBuildStatus) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
		}
		return r.retryStatusObservation(ctx, build, err)
	}
	if state.RequestID != "" && state.RequestID != runnerexec.RequestID(build) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", "runner reported a different build request")
	}
	done, _, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
	}
	if !done {
		if err := r.clearPollFailure(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, r.Cfg.PollInterval)}, nil
	}
	return r.finishObservedBuild(ctx, build, client, state)
}

func (r *KovaBuildReconciler) finishObservedBuild(ctx context.Context, build *kovav1.KovaBuild, client runnerexec.Client, state runner.BuildState) (ctrl.Result, error) {
	_, success, _ := runner.WaitDecision(state.Status)
	build.Status.PollFailureSince = nil
	build.Status.PollFailureCount = 0
	if success {
		return r.beginVerification(ctx, build)
	}
	if state.Status == "cancelled" {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseCancelled, "Cancelled", state.Error)
	}
	// A failed runner is already terminal. Keep its best-effort partial outputs,
	// but never let export/registry I/O hold a reconciler indefinitely.
	resolveCtx, cancel := context.WithTimeout(ctx, r.verificationAttemptTimeout())
	defer cancel()
	build.Status.Phase = kovav1.PhaseFailed
	resolved := buildresult.Resolve(resolveCtx, client, build, r.Cfg.RegistryPlainHTTP)
	build.Status.Outputs = buildresult.Outputs(resolved)
	return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildFailed", state.Error)
}

func (r *KovaBuildReconciler) observeBuildStatus(ctx context.Context, client runnerexec.Client, build *kovav1.KovaBuild) (runner.BuildState, error) {
	statusCtx, cancel := context.WithTimeout(ctx, terminalObservationTimeout)
	defer cancel()
	return client.BuildStatus(statusCtx, build)
}

func (r *KovaBuildReconciler) reconcileExpiredBuild(ctx context.Context, build *kovav1.KovaBuild, client runnerexec.Client) (bool, ctrl.Result, error) {
	if r.Cfg.MaxBuildDuration <= 0 || build.Status.StartedAt == nil || time.Since(build.Status.StartedAt.Time) < r.Cfg.MaxBuildDuration {
		return false, ctrl.Result{}, nil
	}
	state, err := r.observeBuildStatus(ctx, client, build)
	if err == nil && (state.RequestID == "" || state.RequestID == runnerexec.RequestID(build)) {
		if done, _, decisionErr := runner.WaitDecision(state.Status); decisionErr == nil && done {
			result, finishErr := r.finishObservedBuild(ctx, build, client, state)
			return true, result, finishErr
		}
	}
	return r.expireActiveBuild(ctx, build)
}

func (r *KovaBuildReconciler) clearPollFailure(ctx context.Context, build *kovav1.KovaBuild) error {
	if build.Status.PollFailureSince == nil && build.Status.PollFailureCount == 0 {
		return nil
	}
	build.Status.PollFailureSince = nil
	build.Status.PollFailureCount = 0
	return r.Status().Update(ctx, build)
}

func (r *KovaBuildReconciler) retryStatusObservation(ctx context.Context, build *kovav1.KovaBuild, observationErr error) (ctrl.Result, error) {
	var pod corev1.Pod
	err := r.getRunnerPod(ctx, build, &pod)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", "runner Pod disappeared while build status was unavailable")
	}
	if err == nil && (pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod terminated while build status was unavailable: %s", pod.Status.Phase))
	}
	now := metav1.Now()
	if build.Status.PollFailureSince == nil {
		build.Status.PollFailureSince = &now
	}
	if r.Cfg.PollRetryWindow > 0 && time.Since(build.Status.PollFailureSince.Time) >= r.Cfg.PollRetryWindow {
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner status unavailable for %s: %v", r.Cfg.PollRetryWindow, observationErr))
	}
	if build.Status.PollFailureCount < 5 {
		build.Status.PollFailureCount++
	}
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	delay := time.Second << (build.Status.PollFailureCount - 1)
	if delay > 10*time.Second {
		delay = 10 * time.Second
	}
	if r.Cfg.PollRetryWindow > 0 {
		remaining := time.Until(build.Status.PollFailureSince.Add(r.Cfg.PollRetryWindow))
		if remaining < delay {
			delay = max(remaining, time.Millisecond)
		}
	}
	return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, delay)}, nil
}

func (r *KovaBuildReconciler) getRunnerPod(ctx context.Context, build *kovav1.KovaBuild, pod *corev1.Pod) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	return reader.Get(ctx, types.NamespacedName{Namespace: build.Namespace, Name: build.Status.RunnerPodName}, pod)
}

func (r *KovaBuildReconciler) activeRequeueAfter(build *kovav1.KovaBuild, delay time.Duration) time.Duration {
	if delay <= 0 {
		delay = time.Second
	}
	if r.Cfg.MaxBuildDuration <= 0 || build.Status.StartedAt == nil {
		return delay
	}
	remaining := time.Until(build.Status.StartedAt.Add(r.Cfg.MaxBuildDuration))
	if remaining < delay {
		return max(remaining, time.Millisecond)
	}
	return delay
}

func (r *KovaBuildReconciler) expireActiveBuild(ctx context.Context, build *kovav1.KovaBuild) (bool, ctrl.Result, error) {
	if r.Cfg.MaxBuildDuration <= 0 || build.Status.StartedAt == nil || time.Since(build.Status.StartedAt.Time) < r.Cfg.MaxBuildDuration {
		return false, ctrl.Result{}, nil
	}
	if build.Status.RunnerPodName != "" {
		cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = (runnerexec.Client{Kube: r.Kube}).CancelBuild(cancelCtx, build)
		cancel()
		if err := r.Kube.DeletePod(ctx, build.Namespace, build.Status.RunnerPodName); err != nil {
			return true, ctrl.Result{}, err
		}
	}
	return true, ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildTimedOut", fmt.Sprintf("build exceeded the service maximum duration of %s", r.Cfg.MaxBuildDuration))
}

func (r *KovaBuildReconciler) reconcileTerminal(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	// The terminal status and verified outputs live on the KovaBuild until JobTTL.
	// Keeping its runner Pod for the same duration would consume scheduler Pod
	// capacity long after the build has released its active worker slot.
	if err := r.deleteRunnerAndConfirm(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.releaseReservation(ctx, build); err != nil {
		var recovery *admissionRecoveryError
		if errors.As(err, &recovery) {
			if statusErr := r.markAdmissionRecovery(ctx, build, recovery.Pending); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if err := r.queueStoreForNamespace(build.Namespace).ReleaseForBuild(ctx, build); err != nil {
		if errors.Is(err, queueadmission.ErrDrift) {
			if statusErr := r.markAdmissionRecoveryReason(ctx, build, "QueueIntentDrift", "queue intent does not match the build; release is blocked"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if err := r.clearAdmissionRecovery(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
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
	if err := r.deleteRunnerAndConfirm(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.releaseReservation(ctx, build); err != nil {
		var recovery *admissionRecoveryError
		if errors.As(err, &recovery) {
			if statusErr := r.markAdmissionRecovery(ctx, build, recovery.Pending); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if err := r.queueStoreForNamespace(build.Namespace).ReleaseForBuild(ctx, build); err != nil {
		if errors.Is(err, queueadmission.ErrDrift) {
			if statusErr := r.markAdmissionRecoveryReason(ctx, build, "QueueIntentDrift", "queue intent does not match the build; release is blocked"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if controllerutil.RemoveFinalizer(build, cleanupFinalizer) {
		if err := r.Update(ctx, build); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) deleteRunnerAndConfirm(ctx context.Context, build *kovav1.KovaBuild) error {
	pod, err := r.getOwnedPod(ctx, build)
	if err != nil || pod == nil {
		return err
	}
	if observed := pod.Annotations[podCreateAttemptKey]; observed != "" {
		if err := r.completePodCreate(ctx, build, observed); err != nil {
			return err
		}
	}
	deleteCtx, cancel := context.WithTimeout(ctx, r.verificationAttemptTimeout())
	defer cancel()
	if err := r.Kube.DeletePod(deleteCtx, build.Namespace, buildPodName(build.Name)); err != nil {
		return err
	}
	stillPresent, err := r.getOwnedPod(ctx, build)
	if err != nil {
		return err
	}
	if stillPresent != nil {
		return fmt.Errorf("runner Pod %s/%s still exists after deletion", build.Namespace, buildPodName(build.Name))
	}
	return nil
}
