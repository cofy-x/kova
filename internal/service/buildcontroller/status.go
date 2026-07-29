package buildcontroller

import (
	"context"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *KovaBuildReconciler) finish(ctx context.Context, build *kovav1.KovaBuild, phase string, reason string, message string) error {
	r.persistLogs(ctx, build)
	now := metav1.Now()
	build.Status.Phase = phase
	build.Status.ObservedGeneration = build.Generation
	build.Status.Reason = reason
	build.Status.Message = message
	build.Status.FinishedAt = &now
	if build.Status.ResultSummary.Total == 0 {
		build.Status.ResultSummary = summarize(build.Status.Results)
	}
	setPhaseCondition(build, phase, reason, message)
	if r.Recorder != nil {
		eventType := corev1.EventTypeNormal
		if phase == kovav1.PhaseFailed {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(build, eventType, reason, defaultMessage(message, phase))
	}
	jobCompletions.Add(ctx, 1, attribute.String("kova.phase", phase))
	if build.Status.StartedAt != nil {
		jobDuration.RecordDuration(ctx, time.Since(build.Status.StartedAt.Time), attribute.String("kova.phase", phase))
	}
	return r.Status().Update(ctx, build)
}

func defaultMessage(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

func setPhaseCondition(build *kovav1.KovaBuild, phase, reason, message string) {
	status := metav1.ConditionUnknown
	if phase == kovav1.PhaseSucceeded {
		status = metav1.ConditionTrue
	} else if phase == kovav1.PhaseFailed || phase == kovav1.PhaseCancelled {
		status = metav1.ConditionFalse
	}
	if message == "" {
		message = phase
	}
	apiMeta.SetStatusCondition(&build.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: status, Reason: reason, Message: message,
		ObservedGeneration: build.Generation,
	})
}

func summarize(results []kovav1.BuildResult) kovav1.BuildResultSummary {
	summary := kovav1.BuildResultSummary{Total: int32(len(results))}
	for _, result := range results {
		if result.Status == "succeeded" {
			summary.Succeeded++
		} else {
			summary.Failed++
		}
	}
	return summary
}

func cancellationRequested(build *kovav1.KovaBuild) bool {
	return build.Annotations[kovav1.CancellationRequestedAnnotation] != ""
}

func isTerminalPhase(phase string) bool {
	switch phase {
	case kovav1.PhaseSucceeded, kovav1.PhaseFailed, kovav1.PhaseCancelled:
		return true
	default:
		return false
	}
}
