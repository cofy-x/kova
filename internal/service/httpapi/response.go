package httpapi

import (
	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/config"
)

func buildJobFromCR(build *kovav1.KovaBuild, cfg config.Config) BuildJob {
	job := BuildJob{
		ID:             build.Name,
		Status:         httpStatus(build.Status.Phase),
		PodName:        build.Status.RunnerPodName,
		Namespace:      build.Namespace,
		Error:          build.Status.Message,
		CreatedAt:      build.CreationTimestamp.Time,
		BuildkitAddr:   cfg.BuildkitAddr,
		SourceDigest:   build.Spec.Source.Digest,
		IdempotencyKey: build.Spec.IdempotencyKey,
	}
	if job.PodName == "" {
		job.PodName = buildPodName(build.Name)
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
	return job
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

func httpStatus(phase string) string {
	switch phase {
	case kovav1.PhaseStarting:
		return JobStatusStarting
	case kovav1.PhaseRunning:
		return JobStatusRunning
	case kovav1.PhaseSucceeded:
		return JobStatusSucceeded
	case kovav1.PhaseFailed:
		return JobStatusFailed
	case kovav1.PhaseCancelled:
		return JobStatusCancelled
	default:
		return JobStatusQueued
	}
}
