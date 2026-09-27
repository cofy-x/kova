package buildcontroller

import (
	"context"
	"fmt"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/buildresult"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	defaultVerificationAttemptTimeout = 10 * time.Second
	defaultVerificationWindow         = 5 * time.Minute
	verificationBatchSize             = 16
)

func (r *KovaBuildReconciler) verificationAttemptTimeout() time.Duration {
	if r.Cfg.VerificationAttemptTimeout > 0 {
		return r.Cfg.VerificationAttemptTimeout
	}
	return defaultVerificationAttemptTimeout
}

func (r *KovaBuildReconciler) verificationWindow() time.Duration {
	if r.Cfg.VerificationWindow > 0 {
		return r.Cfg.VerificationWindow
	}
	return defaultVerificationWindow
}

func (r *KovaBuildReconciler) beginVerification(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	return r.beginVerificationPhase(ctx, build, kovav1.PhaseVerifying, "RunnerCompleted", "verifying digest-pinned build outputs", r.verificationWindow())
}

func (r *KovaBuildReconciler) beginFailedVerification(ctx context.Context, build *kovav1.KovaBuild, runnerError string) (ctrl.Result, error) {
	// A failed runner must never become Succeeded, even if every earlier push
	// can be verified. This distinct phase also fails closed under an older
	// controller, which only knows the successful Verifying phase.
	window := min(r.verificationWindow(), defaultVerificationWindow)
	return r.beginVerificationPhase(ctx, build, kovav1.PhaseFailedVerifying, "BuildFailed", runnerError, window)
}

func (r *KovaBuildReconciler) beginVerificationPhase(ctx context.Context, build *kovav1.KovaBuild, phase, reason, message string, window time.Duration) (ctrl.Result, error) {
	pending := buildresult.Pending(build)
	if len(pending) == 0 || len(pending) > kovav1.MaxConcreteOutputs {
		if phase == kovav1.PhaseFailedVerifying {
			return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "BuildFailed", message)
		}
		return ctrl.Result{}, r.finish(ctx, build, kovav1.PhaseFailed, "ResultVerificationFailed", "invalid expected output set")
	}
	now := metav1.Now()
	deadlineTime := now.Add(window)
	if r.Cfg.MaxBuildDuration > 0 && build.Status.StartedAt != nil {
		podBudgetDeadline := build.Status.StartedAt.Add(r.Cfg.MaxBuildDuration + window)
		if podBudgetDeadline.Before(deadlineTime) {
			deadlineTime = podBudgetDeadline
		}
	}
	deadline := metav1.NewTime(deadlineTime)
	build.Status.Phase = phase
	build.Status.ObservedGeneration = build.Generation
	build.Status.Reason = truncate(reason, 128)
	build.Status.Message = truncate(message, 2048)
	build.Status.VerificationStartedAt = &now
	build.Status.VerificationDeadlineAt = &deadline
	build.Status.VerificationNextAttemptAt = &now
	build.Status.VerificationAttempts = 0
	build.Status.VerificationLastError = ""
	build.Status.VerificationResults = make([]kovav1.BuildVerificationResult, 0, len(pending))
	for _, result := range pending {
		build.Status.VerificationResults = append(build.Status.VerificationResults, kovav1.BuildVerificationResult{
			Format: result.Format, Image: result.Repository, Platform: result.Platform, State: "pending",
		})
	}
	setPhaseCondition(build, phase, build.Status.Reason, build.Status.Message)
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Millisecond}, nil
}

func (r *KovaBuildReconciler) verificationSlot() chan struct{} {
	r.verificationOnce.Do(func() {
		concurrency := r.Cfg.ControllerConcurrency
		if concurrency <= 0 {
			concurrency = 4
		}
		limit := max(1, min(2, concurrency-1))
		r.verificationSlots = make(chan struct{}, limit)
	})
	return r.verificationSlots
}

func (r *KovaBuildReconciler) reconcileVerifying(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.VerificationStartedAt == nil || build.Status.VerificationDeadlineAt == nil ||
		!validVerificationResults(build) {
		return r.failVerification(ctx, build, "verification state is missing or inconsistent")
	}
	// A prior attempt may have persisted every result but lost the subsequent
	// terminal status write. Its verified receipts win over the deadline on
	// restart; runner cleanup belongs to reconcileTerminal after that write.
	done, failed := buildresult.VerificationDone(build.Status.VerificationResults)
	if failed {
		return r.failVerification(ctx, build, firstVerificationError(build.Status.VerificationResults))
	}
	if done {
		build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
		build.Status.VerificationLastError = ""
		build.Status.VerificationNextAttemptAt = nil
		if err := r.finish(ctx, build, kovav1.PhaseSucceeded, "Completed", ""); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	remaining := time.Until(build.Status.VerificationDeadlineAt.Time)
	if remaining <= 0 {
		message := "result verification exceeded its deadline"
		if build.Status.VerificationLastError != "" {
			message += ": " + build.Status.VerificationLastError
		}
		return r.failVerification(ctx, build, message)
	}
	if next := build.Status.VerificationNextAttemptAt; next != nil && time.Now().Before(next.Time) {
		return ctrl.Result{RequeueAfter: min(time.Until(next.Time), remaining)}, nil
	}
	slot := r.verificationSlot()
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		return ctrl.Result{RequeueAfter: min(250*time.Millisecond, remaining)}, nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, min(r.verificationAttemptTimeout(), remaining))
	defer cancel()
	build.Status.VerificationAttempts++
	exporter := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}
	transient, hardFailure := buildresult.CollectReceipts(attemptCtx, exporter, build, build.Status.VerificationResults)
	// Persist exact push receipts before any registry lookup. On leader handoff,
	// pending digest-bearing outputs resume here without another runner POST.
	boundVerificationErrors(build.Status.VerificationResults)
	build.Status.VerificationLastError = truncate(transient, 2048)
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if !hardFailure && attemptCtx.Err() == nil {
		registryTransient, registryHardFailure := buildresult.VerifyRemoteReceipts(attemptCtx, build.Status.VerificationResults, r.Cfg.RegistryPlainHTTP, verificationBatchSize)
		if registryTransient != "" {
			transient = registryTransient
		}
		hardFailure = registryHardFailure
	}
	if attemptCtx.Err() != nil && transient == "" {
		transient = attemptCtx.Err().Error()
	}
	boundVerificationErrors(build.Status.VerificationResults)
	build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
	done, failed = buildresult.VerificationDone(build.Status.VerificationResults)
	if hardFailure || failed {
		build.Status.VerificationLastError = firstVerificationError(build.Status.VerificationResults)
		return r.failVerification(ctx, build, build.Status.VerificationLastError)
	}
	if done {
		build.Status.VerificationLastError = ""
		build.Status.VerificationNextAttemptAt = nil
		// Durable receipts precede the terminal transition. A status-write
		// ambiguity can then be recovered even if the deadline has elapsed.
		if err := r.Status().Update(ctx, build); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.finish(ctx, build, kovav1.PhaseSucceeded, "Completed", ""); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	if transient == "" {
		transient = "verification batch remains pending"
	}
	build.Status.VerificationLastError = truncate(transient, 2048)
	delay := verificationBackoff(build.Status.VerificationAttempts)
	if transient == "verification batch remains pending" {
		delay = 100 * time.Millisecond
	}
	remaining = time.Until(build.Status.VerificationDeadlineAt.Time)
	delay = min(delay, max(remaining, time.Millisecond))
	next := metav1.NewTime(time.Now().Add(delay))
	build.Status.VerificationNextAttemptAt = &next
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}

// reconcileFailedVerifying retains the runner's failed outcome while it
// independently verifies any outputs pushed before the failure. Unlike a
// successful build, one definitive failed output does not prevent another
// output's exact pushed digest from being collected and checked.
func (r *KovaBuildReconciler) reconcileFailedVerifying(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	if build.Status.VerificationStartedAt == nil || build.Status.VerificationDeadlineAt == nil || !validVerificationResults(build) {
		// Discard malformed receipt state rather than repeatedly trying to
		// update an object the CRD schema cannot accept.
		build.Status.VerificationResults = nil
		build.Status.Outputs = nil
		build.Status.VerificationLastError = "failed build verification state is missing or inconsistent"
		return r.finishFailedVerification(ctx, build)
	}
	done, _ := buildresult.VerificationDone(build.Status.VerificationResults)
	// A previous leader can persist the exact receipts but lose the terminal
	// write. Complete a fully verified receipt even after the deadline.
	if done {
		build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
		build.Status.VerificationLastError = firstFailedResultError(build.Status.VerificationResults)
		return r.finishFailedVerification(ctx, build)
	}
	remaining := time.Until(build.Status.VerificationDeadlineAt.Time)
	if remaining <= 0 {
		return r.expireFailedVerification(ctx, build)
	}
	if next := build.Status.VerificationNextAttemptAt; next != nil && time.Now().Before(next.Time) {
		return ctrl.Result{RequeueAfter: min(time.Until(next.Time), remaining)}, nil
	}
	slot := r.verificationSlot()
	select {
	case slot <- struct{}{}:
		defer func() { <-slot }()
	default:
		return ctrl.Result{RequeueAfter: min(250*time.Millisecond, remaining)}, nil
	}
	attemptCtx, cancel := context.WithTimeout(ctx, min(r.verificationAttemptTimeout(), remaining))
	defer cancel()
	build.Status.VerificationAttempts++
	exporter := runnerexec.Client{Kube: r.Kube, BuildkitPlatformAddrs: r.Cfg.BuildkitPlatformAddrs}
	transient, _ := buildresult.CollectReceipts(attemptCtx, exporter, build, build.Status.VerificationResults)
	// Persist each exact runner push receipt before registry I/O. No later
	// mutable tag lookup may substitute for a missing digest.
	boundVerificationErrors(build.Status.VerificationResults)
	build.Status.VerificationLastError = truncate(transient, 2048)
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if attemptCtx.Err() == nil {
		registryTransient, _ := buildresult.VerifyRemoteReceipts(attemptCtx, build.Status.VerificationResults, r.Cfg.RegistryPlainHTTP, verificationBatchSize)
		if registryTransient != "" {
			transient = registryTransient
		}
	}
	if attemptCtx.Err() != nil && transient == "" {
		transient = attemptCtx.Err().Error()
	}
	boundVerificationErrors(build.Status.VerificationResults)
	build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
	done, _ = buildresult.VerificationDone(build.Status.VerificationResults)
	if done {
		build.Status.VerificationLastError = firstFailedResultError(build.Status.VerificationResults)
		return r.finishFailedVerification(ctx, build)
	}
	if transient == "" {
		transient = "verification batch remains pending"
	}
	build.Status.VerificationLastError = truncate(transient, 2048)
	delay := verificationBackoff(build.Status.VerificationAttempts)
	if transient == "verification batch remains pending" {
		delay = 100 * time.Millisecond
	}
	remaining = time.Until(build.Status.VerificationDeadlineAt.Time)
	if remaining <= 0 {
		return r.expireFailedVerification(ctx, build)
	}
	delay = min(delay, remaining)
	next := metav1.NewTime(time.Now().Add(delay))
	build.Status.VerificationNextAttemptAt = &next
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}

func firstFailedResultError(results []kovav1.BuildVerificationResult) string {
	for _, result := range results {
		if result.State == "failed" && result.Error != "" {
			return truncate(fmt.Sprintf("%s %s: %s", result.Format, result.Image, result.Error), 2048)
		}
	}
	return ""
}

func (r *KovaBuildReconciler) expireFailedVerification(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	message := "partial result verification exceeded its deadline"
	if build.Status.VerificationLastError != "" {
		message += ": " + build.Status.VerificationLastError
	}
	build.Status.VerificationLastError = truncate(message, 2048)
	for i := range build.Status.VerificationResults {
		if build.Status.VerificationResults[i].State == "pending" {
			build.Status.VerificationResults[i].State = "failed"
			build.Status.VerificationResults[i].Error = truncate(message, 2048)
		}
	}
	build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
	return r.finishFailedVerification(ctx, build)
}

func (r *KovaBuildReconciler) finishFailedVerification(ctx context.Context, build *kovav1.KovaBuild) (ctrl.Result, error) {
	build.Status.VerificationNextAttemptAt = nil
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	message := build.Status.Message
	if message == "" {
		message = "runner reported a failed build"
	}
	if err := r.finish(ctx, build, kovav1.PhaseFailed, "BuildFailed", message); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Millisecond}, nil
}

func validVerificationResults(build *kovav1.KovaBuild) bool {
	expected := buildresult.Pending(build)
	results := build.Status.VerificationResults
	if len(expected) == 0 || len(expected) != len(results) {
		return false
	}
	for i, result := range results {
		if result.Format != expected[i].Format || result.Image != expected[i].Repository || result.Platform != expected[i].Platform {
			return false
		}
		switch result.State {
		case "pending", "succeeded", "failed":
		default:
			return false
		}
		if result.State == "succeeded" && result.PushedDigest == "" {
			return false
		}
	}
	return true
}

func verificationBackoff(attempt int32) time.Duration {
	shift := min(max(attempt-1, 0), 3)
	return time.Second << shift
}

func boundVerificationErrors(results []kovav1.BuildVerificationResult) {
	for i := range results {
		results[i].Error = truncate(results[i].Error, 2048)
	}
}

func firstVerificationError(results []kovav1.BuildVerificationResult) string {
	for _, result := range results {
		if result.State == "failed" && result.Error != "" {
			return truncate(fmt.Sprintf("%s %s: %s", result.Format, result.Image, result.Error), 2048)
		}
	}
	return "one or more build results could not be verified"
}

func (r *KovaBuildReconciler) failVerification(ctx context.Context, build *kovav1.KovaBuild, message string) (ctrl.Result, error) {
	build.Status.VerificationLastError = truncate(message, 2048)
	build.Status.VerificationNextAttemptAt = nil
	for i := range build.Status.VerificationResults {
		if build.Status.VerificationResults[i].State == "pending" {
			build.Status.VerificationResults[i].State = "failed"
			build.Status.VerificationResults[i].Error = truncate(message, 2048)
		}
	}
	build.Status.Outputs = buildresult.VerificationOutputs(build.Status.VerificationResults)
	if err := r.Status().Update(ctx, build); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.finish(ctx, build, kovav1.PhaseFailed, "ResultVerificationFailed", message); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Millisecond}, nil
}
