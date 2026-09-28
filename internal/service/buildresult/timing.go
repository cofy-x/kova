package buildresult

import (
	"context"
	"time"

	"github.com/cofy-x/kova/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	ctrl "sigs.k8s.io/controller-runtime"
)

var targetExecutionDuration = observability.DurationHistogram("kova.service.target.execution.duration", "Successful target execution/export/push duration from the runner receipt")

func targetExecutionTime(started, finished string) (time.Duration, bool) {
	start, err := time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return 0, false
	}
	end, err := time.Parse(time.RFC3339Nano, finished)
	if err != nil || end.Before(start) {
		return 0, false
	}
	return end.Sub(start), true
}

func recordTargetExecution(ctx context.Context, buildName, format string, index int, started, finished string) {
	duration, ok := targetExecutionTime(started, finished)
	if !ok {
		return
	}
	// Copy only fixed format and timing. Untrusted target/reason/log payloads
	// never become labels or logs here. Failed outputs are not success samples.
	targetExecutionDuration.RecordDuration(ctx, duration, attribute.String("kova.mode", format))
	ctrl.LoggerFrom(ctx).Info("Target execution timing", "build", buildName, "format", format, "output_index", index, "duration_ms", duration.Milliseconds())
}
