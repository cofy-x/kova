package buildcontroller

import (
	"context"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/observability"
	"github.com/cofy-x/kova/internal/runner"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	serviceStageDuration  = observability.DurationHistogram("kova.service.stage.duration", "Service operation attempt duration, including Kubernetes API wait")
	runnerStartupDuration = observability.DurationHistogram("kova.service.runner.startup.duration", "Observed runner startup segment duration from Kubernetes timestamps")
)

// Stage names are a fixed vocabulary, not job IDs, targets, requesters or errors.
// These are attempt durations: a retry is another observation, not a job total.
func recordServiceStage(ctx context.Context, stage string, started time.Time, err error) {
	result := observability.ResultOK
	if err != nil {
		result = observability.ResultError
	}
	duration := time.Since(started)
	serviceStageDuration.RecordDuration(ctx, duration,
		attribute.String("kova.stage", stage), attribute.String(observability.AttrResult, result))
	ctrl.LoggerFrom(ctx).Info("Service stage timing", "stage", stage, "result", result, "duration_ms", duration.Milliseconds())
}

type startupSegment struct {
	stage    string
	duration time.Duration
}

// Missing or backwards timestamps are omitted, never synthesized as zero.
// source_wait includes image pull / sandbox setup; source_fetch is init-container
// wall time. pod_startup overlaps the finer segments and must not be summed.
func runnerStartupSegments(build *kovav1.KovaBuild, pod *corev1.Pod) []startupSegment {
	var scheduled, ready, sourceStart, sourceEnd time.Time
	for _, condition := range pod.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case corev1.PodScheduled:
			scheduled = condition.LastTransitionTime.Time
		case corev1.PodReady:
			ready = condition.LastTransitionTime.Time
		}
	}
	for _, container := range pod.Status.InitContainerStatuses {
		if container.Name == "source-fetch" && container.State.Terminated != nil && container.State.Terminated.ExitCode == 0 {
			sourceStart = container.State.Terminated.StartedAt.Time
			sourceEnd = container.State.Terminated.FinishedAt.Time
		}
	}
	segments := []startupSegment{}
	add := func(stage string, start, end time.Time) {
		if !start.IsZero() && !end.IsZero() && !end.Before(start) {
			segments = append(segments, startupSegment{stage: stage, duration: end.Sub(start)})
		}
	}
	add("queue_to_pod", build.CreationTimestamp.Time, pod.CreationTimestamp.Time)
	add("pod_schedule", pod.CreationTimestamp.Time, scheduled)
	add("source_wait", scheduled, sourceStart)
	add("source_fetch", sourceStart, sourceEnd)
	add("runner_ready", sourceEnd, ready)
	add("pod_startup", pod.CreationTimestamp.Time, ready)
	return segments
}

func (r *KovaBuildReconciler) markSubmittedWithTimings(ctx context.Context, build *kovav1.KovaBuild, state runner.BuildState, pod *corev1.Pod) (ctrl.Result, error) {
	previousPhase := build.Status.Phase
	result, err := r.markSubmitted(ctx, build, state)
	if err != nil || previousPhase != kovav1.PhaseStarting {
		return result, err
	}
	// Record only after the Starting transition has persisted successfully.
	// Telemetry is best effort, not an exactly-once durable billing receipt.
	for _, segment := range runnerStartupSegments(build, pod) {
		runnerStartupDuration.RecordDuration(ctx, segment.duration, attribute.String("kova.stage", segment.stage))
		ctrl.LoggerFrom(ctx).Info("Runner startup timing", "build", build.Name, "stage", segment.stage, "duration_ms", segment.duration.Milliseconds())
	}
	return result, nil
}
