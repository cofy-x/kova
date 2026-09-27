package httpapi

import (
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
)

func buildJobFromCR(build *kovav1.KovaBuild, cfg config.Config) apiv1.BuildJob {
	job := apiv1.BuildJob{
		ID:                    build.Name,
		Status:                httpStatus(build.Status.Phase),
		CreatedAt:             build.CreationTimestamp.Time,
		SourceDigest:          build.Spec.Source.Digest,
		SourceURI:             build.Spec.Source.URI,
		IdempotencyKey:        build.Spec.IdempotencyKey,
		Requester:             build.Spec.Requester.Username,
		CancellationRequested: build.Annotations[kovav1.CancellationRequestedAnnotation] != "",
		RecoveryRequired:      apiMeta.IsStatusConditionTrue(build.Status.Conditions, "AdmissionRecoveryRequired"),
		RequestedConcurrency:  requestedConcurrency(build),
		AllocatedConcurrency:  build.Status.AllocatedConcurrency,
	}
	if build.Status.Phase == kovav1.PhaseFailed || build.Status.Phase == kovav1.PhaseCancelled {
		job.FailureCode = publicBuildFailureCode(build.Status.Reason)
		job.Error = publicBuildError(build.Status.Reason)
	}
	if build.Status.StartedAt != nil {
		t := build.Status.StartedAt.Time
		job.StartedAt = &t
	}
	if build.Status.FinishedAt != nil {
		t := build.Status.FinishedAt.Time
		job.FinishedAt = &t
		expires := t.Add(cfg.JobTTL)
		job.ExpiresAt = &expires
	}
	if build.Status.VerificationStartedAt != nil {
		t := build.Status.VerificationStartedAt.Time
		job.VerificationStartedAt = &t
	}
	if build.Status.VerificationDeadlineAt != nil {
		t := build.Status.VerificationDeadlineAt.Time
		job.VerificationDeadlineAt = &t
	}
	if build.Status.VerificationNextAttemptAt != nil && (build.Status.Phase == kovav1.PhaseVerifying || build.Status.Phase == kovav1.PhaseFailedVerifying) {
		t := build.Status.VerificationNextAttemptAt.Time
		job.VerificationNextAttemptAt = &t
	}
	job.VerificationAttempts = build.Status.VerificationAttempts
	job.VerificationLastError = build.Status.VerificationLastError
	for _, result := range build.Status.VerificationResults {
		switch result.State {
		case "pending":
			job.VerificationPending++
		case "succeeded":
			job.VerificationSucceeded++
		case "failed":
			job.VerificationFailed++
		}
	}
	return job
}

func requestedConcurrency(build *kovav1.KovaBuild) int {
	if build.Spec.Build.Concurrency > 0 {
		return build.Spec.Build.Concurrency
	}
	return 1
}

func isTerminalPhase(phase string) bool {
	switch phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}

func publicBuildFailureCode(reason string) apiv1.BuildFailureCode {
	switch reason {
	case "InvalidSource":
		return apiv1.BuildFailureInvalidSource
	case "InvalidTargets":
		return apiv1.BuildFailureInvalidTargets
	case "WorkerPlatformUnavailable":
		return apiv1.BuildFailureWorkerPlatformUnavailable
	case "RunnerCreateFailed", "RunnerUnavailable":
		return apiv1.BuildFailureRunnerUnavailable
	case "BuildSubmissionFailed":
		return apiv1.BuildFailureSubmissionFailed
	case "ResultVerificationFailed":
		return apiv1.BuildFailureVerificationFailed
	case "Cancelled":
		return apiv1.BuildFailureCancelled
	default:
		return apiv1.BuildFailureExecutionFailed
	}
}

func httpStatus(phase string) apiv1.JobStatus {
	switch phase {
	case kovav1.PhaseStarting:
		return apiv1.JobStatusStarting
	case kovav1.PhaseRunning:
		return apiv1.JobStatusRunning
	case kovav1.PhaseVerifying, kovav1.PhaseFailedVerifying:
		return apiv1.JobStatusVerifying
	case kovav1.PhaseSucceeded:
		return apiv1.JobStatusSucceeded
	case kovav1.PhaseFailed:
		return apiv1.JobStatusFailed
	case kovav1.PhaseCancelled:
		return apiv1.JobStatusCancelled
	default:
		return apiv1.JobStatusQueued
	}
}

func publicBuildError(reason string) string {
	switch reason {
	case "InvalidSource":
		return "immutable source validation failed"
	case "InvalidTargets":
		return "source targets do not exactly match requested targets"
	case "WorkerPlatformUnavailable":
		return "no BuildKit worker capacity is configured for a requested platform"
	case "RunnerCreateFailed", "RunnerUnavailable":
		return "build runner is unavailable"
	case "BuildSubmissionFailed":
		return "build submission failed"
	case "ResultVerificationFailed":
		return "one or more build results could not be verified"
	case "Cancelled":
		return "build was cancelled"
	case "BuildTimedOut":
		return "build exceeded the service maximum duration"
	default:
		return "build execution failed"
	}
}
