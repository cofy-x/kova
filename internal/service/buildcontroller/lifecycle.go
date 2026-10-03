package buildcontroller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/runner"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	"github.com/cofy-x/kova/internal/service/runnerexec"
	"github.com/cofy-x/kova/internal/sourcebundle"

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
	admissionStarted := time.Now()
	decision, err := r.admission(ctx, build)
	recordServiceStage(ctx, "admission", admissionStarted, err)
	if err != nil {
		if errors.Is(err, errAdmissionClosed) || apierrors.IsNotFound(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		if errors.Is(err, queueadmission.ErrDrift) {
			if statusErr := r.markAdmissionRecoveryReason(ctx, build, "QueueIntentDrift", "queue intent does not match the build; active admission is blocked"); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{}, err
	}
	if !decision.Admitted {
		capacityWaits.Add(ctx, 1)
		previousStatus := build.Status.DeepCopy()
		build.Status.Phase = kovav1.PhaseQueued
		apiMeta.RemoveStatusCondition(&build.Status.Conditions, admissionRecoveryCondition)
		setPhaseCondition(build, kovav1.PhaseQueued, "WaitingForCapacity", decision.Message)
		build.Status.ObservedGeneration = build.Generation
		if !reflect.DeepEqual(previousStatus, &build.Status) {
			if err := r.Status().Update(ctx, build); err != nil {
				return ctrl.Result{}, err
			}
		}
		// Capacity changes and leader startup wake only the next eligible queued
		// build. An unchanged queue item must not generate its own 5s API loop.
		return ctrl.Result{}, nil
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
		if r.Genesis != nil {
			if _, err := r.witnessFromPod(build, owned); err != nil {
				return ctrl.Result{}, err
			}
		}
		if observed := owned.Annotations[podCreateAttemptKey]; observed != "" {
			if err := r.completePodCreate(ctx, build, observed); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		attempt, err := r.beginPodCreate(ctx, build)
		if err != nil {
			if errors.Is(err, errAdmissionClosed) || apierrors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			}
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
		if err := r.stampGenesisPod(ctx, build, &pod, attempt); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &pod); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				if definitivePodCreateRejection(err) {
					if clearErr := r.completePodCreate(ctx, build, attempt); clearErr != nil {
						return ctrl.Result{}, clearErr
					}
					return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateRejected", err.Error())
				}
				return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerCreateOutcomeUnknown", err.Error())
			}
			owned, getErr := r.getOwnedPod(ctx, build)
			if getErr != nil {
				return ctrl.Result{}, getErr
			}
			if owned == nil {
				return ctrl.Result{}, fmt.Errorf("runner Pod %s/%s disappeared after AlreadyExists", build.Namespace, podName)
			}
			if r.Genesis != nil {
				if _, err := r.witnessFromPod(build, owned); err != nil {
					return ctrl.Result{}, err
				}
				if owned.Annotations[podCreateAttemptKey] != attempt {
					return ctrl.Result{}, fmt.Errorf("Genesis runner Pod Create was won by another attempt")
				}
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
	if r.Genesis != nil {
		// The API-assigned Pod UID is not known until a direct post-Create
		// observation. Persist it and the request identity in Starting status
		// before any path may submit work to the runner.
		observed, err := r.getOwnedPod(ctx, build)
		if err != nil {
			return ctrl.Result{}, err
		}
		witness, err := r.witnessFromPod(build, observed)
		if err != nil {
			return ctrl.Result{}, err
		}
		build.Status.AdmissionGenesisWitness = witness
	}
	build.Status.StartedAt = &now
	build.Status.Message = ""
	apiMeta.RemoveStatusCondition(&build.Status.Conditions, admissionRecoveryCondition)
	setPhaseCondition(build, kovav1.PhaseStarting, "RunnerCreated", "runner Pod was created")
	if r.Genesis != nil {
		if err := r.Genesis.Check(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func definitivePodCreateRejection(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsUnauthorized(err)
}

// A cancellation request is not proof that an accepted runner has stopped.
// Observe its exact pre-loss Pod/CR/request witness before any fence or stop:
// a terminal response moves into receipt verification, an uncertain read
// preserves the charge, and only a still-live nonterminal response may take
// the UID-safe cancellation path while the original pair remains healthy.
func (r *KovaBuildReconciler) reconcileGenesisCancellation(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	pairErr := r.Genesis.Check(ctx)
	if pairErr != nil && !apierrors.IsNotFound(pairErr) && !errors.Is(pairErr, admissiongenesis.ErrChanged) {
		return ctrl.Result{}, pairErr
	}
	current, _, err := r.directGenesisWitness(ctx, build)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pairErr == nil {
		if _, err := r.directGenesisCharge(ctx, current); err != nil {
			return ctrl.Result{}, err
		}
	}
	if current.Status.Phase == kovav1.PhaseVerifying {
		return r.reconcileVerifying(ctx, current)
	}
	if current.Status.Phase != kovav1.PhaseStarting && current.Status.Phase != kovav1.PhaseRunning {
		return ctrl.Result{}, fmt.Errorf("Genesis cancellation has no observable active phase")
	}
	client := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}
	state, err := r.observeBuildStatus(ctx, client, current)
	if err != nil {
		return ctrl.Result{}, err
	}
	if state.Status == "idle" {
		if current.Status.Phase != kovav1.PhaseStarting {
			return ctrl.Result{}, fmt.Errorf("Genesis Running runner reported idle; accepted request outcome is uncertain")
		}
		return r.stopGenesisRunner(ctx, current, "Cancelled")
	}
	done, _, err := runner.WaitDecision(state.Status)
	if err != nil {
		return ctrl.Result{}, err
	}
	if done {
		return r.finishObservedBuild(ctx, current, client, state)
	}
	if pairErr != nil {
		return ctrl.Result{}, pairErr
	}
	return r.stopGenesisRunner(ctx, current, "Cancelled")
}

func (r *KovaBuildReconciler) cancelBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Genesis != nil && build.Status.AdmissionGenesisWitness != nil {
		return ctrl.Result{}, fmt.Errorf("Genesis accepted runner cancellation requires exact terminal observation before stop")
	}
	if err := r.fenceReservation(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if err := validateRunnerPodStatusName(build); err != nil {
		return ctrl.Result{}, err
	}
	if r.Genesis == nil && build.Status.Phase == kovav1.PhaseRunning && build.Status.RunnerPodName != "" {
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
	client := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs, RegistryPlainHTTP: r.Cfg.RegistryPlainHTTP}
	var pod corev1.Pod
	if r.Genesis != nil {
		current, witnessedPod, err := r.directGenesisGrant(ctx, build)
		if err != nil {
			return r.observeExistingGenesisSubmission(ctx, build, client, err)
		}
		build, pod = current, *witnessedPod
	}
	if expired, result, err := r.reconcileExpiredBuild(ctx, build, client); expired {
		return result, err
	}
	if r.Genesis == nil {
		if err := r.getRunnerPod(ctx, build, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", "runner Pod disappeared before the build was submitted")
			}
			return ctrl.Result{}, err
		}
	}
	if !podReady(&pod) {
		if reason, message, failed := sourceFetchFailure(&pod); failed {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, reason, message)
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
		return r.markSubmittedWithTimings(ctx, build, state, &pod)
	}
	if !state.SupportsIdempotentBuildRequest() {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolIncompatible", "idle runner does not advertise idempotent build submission; drain Starting jobs and replace the legacy runner before upgrading the controller")
	}
	if err := r.clearPollFailure(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if r.Genesis != nil {
		current, witnessedPod, err := r.directGenesisGrant(ctx, build)
		if err != nil {
			return r.observeExistingGenesisSubmission(ctx, build, client, err)
		}
		build, pod = current, *witnessedPod
	}
	operationCtx := ctx
	cancelOperation := func() {}
	if r.Cfg.MaxBuildDuration > 0 && build.Status.StartedAt != nil {
		operationCtx, cancelOperation = context.WithDeadline(ctx, build.Status.StartedAt.Add(r.Cfg.MaxBuildDuration))
	}
	defer cancelOperation()
	inspectStarted := time.Now()
	sourceTargets, err := client.SourceTargets(operationCtx, build, sourcePath(build))
	recordServiceStage(ctx, "source_inspect", inspectStarted, err)
	if err != nil {
		if errors.Is(err, runnerexec.ErrRunnerResponseTooLarge) || errors.Is(err, runnerexec.ErrSourceInspectProtocol) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerProtocolError", err.Error())
		}
		if errors.Is(err, runnerexec.ErrSourceInspectTransport) {
			return r.retryStatusObservation(ctx, build, err)
		}
		if errors.Is(err, runnerexec.ErrSourceInspectInvalid) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidSource", err.Error())
		}
		if errors.Is(err, runnerexec.ErrSourceInspectResourceExhausted) {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "SourceInspectResourceExhausted", err.Error())
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "SourceInspectUnavailable", err.Error())
	}
	if !buildcontract.EqualTargetSpecSets(contractTargets(build.Spec.Targets), sourceTargets) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "InvalidTargets", "source targets do not exactly match requested targets")
	}
	if r.Genesis != nil {
		current, witnessedPod, err := r.directGenesisGrant(ctx, build)
		if err != nil {
			return r.observeExistingGenesisSubmission(ctx, build, client, err)
		}
		build, pod = current, *witnessedPod
	}
	if expired, result, err := r.reconcileExpiredBuild(ctx, build, client); expired {
		return result, err
	}
	if r.Genesis != nil {
		current, witnessedPod, err := r.directGenesisGrant(ctx, build)
		if err != nil {
			return r.observeExistingGenesisSubmission(ctx, build, client, err)
		}
		build, pod = current, *witnessedPod
	}
	submitStarted := time.Now()
	submitErr := client.SubmitBuild(operationCtx, build, sourcePath(build))
	recordServiceStage(ctx, "submit", submitStarted, submitErr)
	if r.Genesis != nil {
		current, after, err := r.directGenesisWitness(ctx, build)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !samePodUID(&pod, after) {
			return ctrl.Result{}, fmt.Errorf("Genesis runner Pod UID changed across build submission")
		}
		build = current
		state, statusErr := r.observeBuildStatus(ctx, client, build)
		if statusErr != nil {
			return r.retryStatusObservation(ctx, build, statusErr)
		}
		if state.Status == "idle" {
			return ctrl.Result{RequeueAfter: r.activeRequeueAfter(build, time.Second)}, nil
		}
		return r.markSubmittedWithTimings(ctx, build, state, after)
	}
	if submitErr != nil {
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
		return r.markSubmittedWithTimings(ctx, build, state, &pod)
	}
	return r.markSubmittedWithTimings(ctx, build, runner.BuildState{Status: "running"}, &pod)
}

// A pre-loss POST may have been accepted even though its reply or the active
// ledger later vanished. This path performs only a witness-bound status GET;
// it never retries SubmitBuild without the original live grant.
func (r *KovaBuildReconciler) observeExistingGenesisSubmission(ctx context.Context, build *kovav1.KovaBuild, exec runnerexec.Client, grantErr error) (ctrl.Result, error) {
	if err := r.Genesis.Check(ctx); err == nil ||
		(!apierrors.IsNotFound(err) && !errors.Is(err, admissiongenesis.ErrChanged)) {
		// A healthy pair with no live grant is a bypass or cleanup race;
		// an unavailable API is not proof of committed ledger loss. Neither
		// grants permission to contact the runner.
		return ctrl.Result{}, grantErr
	}
	current, pod, err := r.directGenesisWitness(ctx, build)
	if err != nil {
		return ctrl.Result{}, grantErr
	}
	state, err := r.observeBuildStatus(ctx, exec, current)
	if err != nil || state.Status == "idle" {
		return ctrl.Result{}, grantErr
	}
	return r.markSubmittedWithTimings(ctx, current, state, pod)
}

func (r *KovaBuildReconciler) markSubmitted(ctx context.Context, build *kovav1.KovaBuild, state runner.BuildState) (ctrl.Result, error) {
	if r.Genesis != nil && (build.Status.AdmissionGenesisWitness == nil ||
		state.RequestID != build.Status.AdmissionGenesisWitness.RunnerRequestID) {
		return ctrl.Result{}, fmt.Errorf("Genesis runner acceptance lacks exact request witness")
	}
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

func sourceFetchFailure(pod *corev1.Pod) (string, string, bool) {
	if pod.Status.Reason == "Evicted" {
		return "RunnerResourceExhausted", "runner Pod was evicted before build submission", true
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name != "source-fetch" || status.State.Terminated == nil || status.State.Terminated.ExitCode == 0 {
			continue
		}
		terminated := status.State.Terminated
		message := terminated.Message
		if message == "" {
			message = terminated.Reason
		}
		if message == "" {
			message = fmt.Sprintf("source fetch exited with code %d", terminated.ExitCode)
		}
		if terminated.Reason == "OOMKilled" || terminated.ExitCode == sourcebundle.FetchExitCodeResourceExhausted {
			return "SourceFetchResourceExhausted", message, true
		}
		if terminated.ExitCode == sourcebundle.FetchExitCodeInvalidSource {
			return "InvalidSource", message, true
		}
		return "SourceFetchUnavailable", message, true
	}
	return "", "", false
}

func (r *KovaBuildReconciler) pollBuild(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Genesis != nil {
		current, _, err := r.directGenesisWitness(ctx, build)
		if err != nil {
			return ctrl.Result{}, err
		}
		build = current
	}
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
	// The runner's failure is immutable, but a preceding output may already
	// have reached its registry. Persist this outcome before bounded receipt
	// collection so a leader restart never re-submits or reports success.
	return r.beginFailedVerification(ctx, build, state.Error)
}

func (r *KovaBuildReconciler) observeBuildStatus(ctx context.Context, client runnerexec.Client, build *kovav1.KovaBuild) (runner.BuildState, error) {
	var before *corev1.Pod
	if r.Genesis != nil {
		_, witnessed, err := r.directGenesisWitness(ctx, build)
		if err != nil {
			return runner.BuildState{}, err
		}
		before = witnessed
	}
	statusCtx, cancel := context.WithTimeout(ctx, terminalObservationTimeout)
	defer cancel()
	state, err := client.BuildStatus(statusCtx, build)
	if err != nil || r.Genesis == nil {
		return state, err
	}
	_, after, err := r.directGenesisWitness(ctx, build)
	if err != nil {
		return runner.BuildState{}, err
	}
	if !samePodUID(before, after) ||
		(state.Status != "idle" && state.RequestID != build.Status.AdmissionGenesisWitness.RunnerRequestID) ||
		(state.Status == "idle" && state.RequestID != "") {
		return runner.BuildState{}, fmt.Errorf("Genesis runner status lacks exact Pod and request identity")
	}
	return state, nil
}

func (r *KovaBuildReconciler) reconcileExpiredBuild(ctx context.Context, build *kovav1.KovaBuild, client runnerexec.Client) (bool, ctrl.Result, error) {
	if r.Cfg.MaxBuildDuration <= 0 || build.Status.StartedAt == nil || time.Since(build.Status.StartedAt.Time) < r.Cfg.MaxBuildDuration {
		return false, ctrl.Result{}, nil
	}
	state, err := r.observeBuildStatus(ctx, client, build)
	if r.Genesis != nil {
		if err != nil {
			// An uncertain status is not permission to erase an accepted result.
			return true, ctrl.Result{}, err
		}
		if state.Status == "idle" {
			if build.Status.Phase != kovav1.PhaseStarting {
				return true, ctrl.Result{}, fmt.Errorf("Genesis Running runner reported idle at expiry; accepted request outcome is uncertain")
			}
			result, stopErr := r.stopGenesisRunner(ctx, build, "BuildTimedOut")
			return true, result, stopErr
		}
		done, _, decisionErr := runner.WaitDecision(state.Status)
		if decisionErr != nil {
			return true, ctrl.Result{}, decisionErr
		}
		if done {
			result, finishErr := r.finishObservedBuild(ctx, build, client, state)
			return true, result, finishErr
		}
		result, stopErr := r.stopGenesisRunner(ctx, build, "BuildTimedOut")
		return true, result, stopErr
	}
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
	if r.Genesis != nil && build.Status.AdmissionGenesisWitness != nil {
		// Poll-window expiry must not convert a transport uncertainty into a
		// forced stop without a witnessed nonterminal runner response.
		return ctrl.Result{}, observationErr
	}
	if build.Status.RunnerPodName == "" {
		return ctrl.Result{}, fmt.Errorf("running KovaBuild %s/%s has no runner Pod name", build.Namespace, build.Name)
	}
	if err := validateRunnerPodStatusName(build); err != nil {
		return ctrl.Result{}, err
	}
	var pod corev1.Pod
	err := r.getRunnerPod(ctx, build, &pod)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", "runner Pod disappeared while build status was unavailable")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !podOwnedByBuild(&pod, build) {
		return ctrl.Result{}, fmt.Errorf("runner Pod %s/%s is not owned by KovaBuild UID %s", pod.Namespace, pod.Name, build.UID)
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "RunnerUnavailable", fmt.Sprintf("runner Pod terminated while build status was unavailable: %s", pod.Status.Phase))
	}
	now := metav1.Now()
	if build.Status.PollFailureSince == nil {
		build.Status.PollFailureSince = &now
	}
	if r.Cfg.PollRetryWindow > 0 && time.Since(build.Status.PollFailureSince.Time) >= r.Cfg.PollRetryWindow {
		if err := r.requireUIDSafePodStop(); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.deleteOwnedRunnerPodAndConfirm(ctx, build, &pod); err != nil {
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
	if r.Genesis != nil {
		return true, ctrl.Result{}, fmt.Errorf("Genesis expiry requires exact nonterminal runner observation before forced stop")
	}
	if build.Status.RunnerPodName != "" {
		if err := validateRunnerPodStatusName(build); err != nil {
			return true, ctrl.Result{}, err
		}
		if err := r.requireUIDSafePodStop(); err != nil {
			return true, ctrl.Result{}, err
		}
		cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = (runnerexec.Client{Kube: r.Kube}).CancelBuild(cancelCtx, build)
		cancel()
		if err := r.deleteRunnerAndConfirm(ctx, build); err != nil {
			return true, ctrl.Result{}, err
		}
	}
	return true, ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildTimedOut", fmt.Sprintf("build exceeded the service maximum duration of %s", r.Cfg.MaxBuildDuration))
}

func (r *KovaBuildReconciler) reconcileTerminal(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if err := r.fenceReservation(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
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
	if build.UID == "" {
		return ctrl.Result{}, fmt.Errorf("refusing to delete terminal KovaBuild %s/%s without a UID", build.Namespace, build.Name)
	}
	if err := r.Delete(ctx, build, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &build.UID}}); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *KovaBuildReconciler) reconcileDelete(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if r.Genesis != nil && build.Status.AdmissionGenesisWitness != nil && !isTerminalPhase(build.Status.Phase) {
		if build.Status.AdmissionGenesisStopIntent != nil {
			return r.resumeGenesisStop(ctx, build)
		}
		// A CR deletion explicitly discards the build result. Unlike the
		// cancellation annotation, it is not a request to retain a completed
		// result for verification; still, its Pod stop needs a durable intent.
		return r.stopGenesisRunner(ctx, build, "Deleted")
	}
	if err := r.fenceReservation(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.deleteRunnerAndConfirm(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	// The deleting CR can remain visible until its finalizer is removed. Make
	// its terminal status durable before releasing active capacity, otherwise
	// admission can observe a Starting/Running CR with no reservation and
	// mistake routine cleanup for an orphaned runner.
	if !isTerminalPhase(build.Status.Phase) {
		if err := r.finish(ctx, build, kovav1.PhaseCancelled, "Deleted", "build was deleted"); err != nil {
			return ctrl.Result{}, err
		}
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
	if err := validateRunnerPodStatusName(build); err != nil {
		return err
	}
	if r.Genesis != nil {
		if err := r.Genesis.Check(ctx); err != nil {
			return err
		}
		if r.APIReader == nil {
			return fmt.Errorf("Genesis cleanup requires a direct API reader")
		}
		if build.Status.AdmissionGenesisWitness == nil {
			pod, err := r.getOwnedPod(ctx, build)
			if err != nil || pod == nil {
				return err
			}
			return fmt.Errorf("Genesis cleanup refuses a runner Pod without durable witness")
		}
		current, pod, err := r.directGenesisStopState(ctx, build)
		if err != nil {
			return err
		}
		if pod == nil {
			if !isTerminalPhase(current.Status.Phase) {
				return fmt.Errorf("Genesis cleanup cannot infer a nonterminal result from Pod absence")
			}
			return nil
		}
		if !isTerminalPhase(current.Status.Phase) && !validGenesisStopIntent(current) {
			return fmt.Errorf("Genesis cleanup lacks a terminal result or durable stop intent")
		}
		return r.deleteOwnedRunnerPodAndConfirm(ctx, current, pod)
	}
	pod, err := r.getOwnedPod(ctx, build)
	if err != nil || pod == nil {
		return err
	}
	return r.deleteOwnedRunnerPodAndConfirm(ctx, build, pod)
}

func (r *KovaBuildReconciler) deleteOwnedRunnerPodAndConfirm(ctx context.Context, build *kovav1.KovaBuild, pod *corev1.Pod) error {
	if pod.Namespace != build.Namespace || pod.Name != buildPodName(build.Name) || !podOwnedByBuild(pod, build) {
		return fmt.Errorf("refusing to delete runner Pod %s/%s not matching KovaBuild UID %s", pod.Namespace, pod.Name, build.UID)
	}
	if pod.UID == "" {
		return fmt.Errorf("refusing to delete runner Pod %s/%s without a UID", pod.Namespace, pod.Name)
	}
	if r.Genesis != nil {
		if err := r.Genesis.Check(ctx); err != nil {
			return err
		}
		current, observed, err := r.directGenesisStopState(ctx, build)
		if err != nil {
			return err
		}
		if observed == nil || observed.UID != pod.UID ||
			(!isTerminalPhase(current.Status.Phase) && !validGenesisStopIntent(current)) {
			return fmt.Errorf("Genesis Pod deletion lacks exact durable terminal or stop witness")
		}
		entry, err := r.directGenesisCharge(ctx, current)
		if err != nil {
			return err
		}
		if !entry.Closing {
			return fmt.Errorf("Genesis Pod deletion lacks fenced active charge")
		}
		build, pod = current, observed
	}
	if observed := pod.Annotations[podCreateAttemptKey]; observed != "" {
		if err := r.completePodCreate(ctx, build, observed); err != nil {
			return err
		}
	}
	deleteCtx, cancel := context.WithTimeout(ctx, r.verificationAttemptTimeout())
	defer cancel()
	if err := r.Kube.DeletePodWithUID(deleteCtx, pod.Namespace, pod.Name, pod.UID); err != nil {
		return err
	}
	var stillPresent corev1.Pod
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, &stillPresent)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if stillPresent.UID != pod.UID {
		return apierrors.NewConflict(corev1.Resource("pods"), pod.Name,
			fmt.Errorf("runner Pod %s/%s was replaced after deletion: expected UID %s, observed UID %s", pod.Namespace, pod.Name, pod.UID, stillPresent.UID))
	}
	return fmt.Errorf("runner Pod %s/%s UID %s still exists after deletion", pod.Namespace, pod.Name, pod.UID)
}

func validateRunnerPodStatusName(build *kovav1.KovaBuild) error {
	if build.Status.RunnerPodName != "" && build.Status.RunnerPodName != buildPodName(build.Name) {
		return fmt.Errorf("KovaBuild %s/%s references unexpected runner Pod %q", build.Namespace, build.Name, build.Status.RunnerPodName)
	}
	return nil
}
