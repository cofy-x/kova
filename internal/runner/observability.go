package runner

import (
	"context"

	"github.com/cofy-x/kova/internal/observability"

	"go.opentelemetry.io/otel/attribute"
)

func runnerOperation(name string, cfg Config) (context.Context, *observability.Operation) {
	return observability.StartOperation(context.Background(), observability.OperationConfig{
		Name: "kova.runner." + name,
		SpanAttrs: []attribute.KeyValue{
			attribute.String(observability.AttrOperation, name),
			observability.StringAttr(observability.AttrNamespace, cfg.Namespace),
			observability.StringAttr(observability.AttrPod, cfg.PodName),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.String(observability.AttrOperation, name),
		},
		Counter:  observability.Instrument{Name: "kova_runner_operations_total", Description: "Runner client operations"},
		Duration: observability.Instrument{Name: "kova_runner_operation_duration_seconds", Description: "Runner client operation duration"},
	})
}
