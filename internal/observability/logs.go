package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

func EmitLog(ctx context.Context, level string, message string) {
	if !Active().Enabled() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record := log.Record{}
	now := time.Now()
	record.SetTimestamp(now)
	record.SetObservedTimestamp(now)
	record.SetSeverity(severity(level))
	record.SetSeverityText(level)
	record.SetBody(log.StringValue(SanitizeLogBody(message)))
	global.Logger(instrumentationName).Emit(ctx, record)
}

func severity(level string) log.Severity {
	switch level {
	case "ERROR":
		return log.SeverityError
	case "WARN":
		return log.SeverityWarn
	case "DEBUG":
		return log.SeverityDebug
	default:
		return log.SeverityInfo
	}
}
