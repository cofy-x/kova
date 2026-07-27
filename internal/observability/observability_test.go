package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestConfigFromEnvDefaultDisabled(t *testing.T) {
	t.Setenv(EnvEnabled, "")
	cfg := ConfigFromEnv(WithServiceName("svc"), WithComponent("component"))
	if cfg.Enabled {
		t.Fatal("ConfigFromEnv().Enabled = true, want false")
	}
	if cfg.ServiceName != "svc" || cfg.Component != "component" {
		t.Fatalf("ConfigFromEnv() = %#v", cfg)
	}
	if !cfg.TracesEnabled || !cfg.MetricsEnabled || !cfg.LogsEnabled {
		t.Fatalf("signals should default enabled when observability is enabled later: %#v", cfg)
	}
}

func TestConfigFromEnvEnabledAndSanitizedAttrs(t *testing.T) {
	t.Setenv(EnvEnabled, "true")
	t.Setenv("OTEL_SERVICE_NAME", "override")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=kind,token=secret")
	t.Setenv("KOVA_OTEL_METRIC_INTERVAL", "3s")
	cfg := ConfigFromEnv(WithServiceName("svc"))
	if !cfg.Enabled {
		t.Fatal("ConfigFromEnv().Enabled = false, want true")
	}
	if cfg.ServiceName != "override" || cfg.OTLPEndpointURL != "otel-collector:4317" || !cfg.OTLPInsecure {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if cfg.MetricInterval != 3*time.Second {
		t.Fatalf("MetricInterval = %s, want 3s", cfg.MetricInterval)
	}
	if len(cfg.ResourceAttrs) != 2 || cfg.ResourceAttrs[1].Value.AsString() != "[redacted]" {
		t.Fatalf("ResourceAttrs = %#v", cfg.ResourceAttrs)
	}
}

func TestSensitiveAttributeSanitizer(t *testing.T) {
	got := StringAttr("registry_password", "secret")
	if got.Value.AsString() != "[redacted]" {
		t.Fatalf("sanitized value = %q, want [redacted]", got.Value.AsString())
	}
	if got := SanitizeLogBody("token=secret"); got != "[redacted]" {
		t.Fatalf("SanitizeLogBody() = %q", got)
	}
}

func TestDisabledInitReturnsNoop(t *testing.T) {
	handle, err := Init(context.Background(), Config{})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if handle.Enabled() {
		t.Fatal("disabled handle should not be enabled")
	}
	_, span := handle.Start(context.Background(), "test")
	defer span.End()
	if span.IsRecording() {
		t.Fatal("noop span should not record")
	}
}

func TestOperationRecordsErrorStatus(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handle := &Handle{
		enabled: true,
		tracer:  provider.Tracer("test"),
		meter:   noop.NewMeterProvider().Meter("test"),
	}

	_, op := handle.StartOperation(context.Background(), OperationConfig{
		Name:      "test.operation",
		SpanAttrs: []attribute.KeyValue{attribute.String(AttrOperation, "test")},
	})
	op.SetErrorClass("boom")
	op.End(errors.New("boom"))

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	if ended[0].Status().Code != codes.Error {
		t.Fatalf("span status = %v, want error", ended[0].Status())
	}
}

func TestOperationClassifiesDeadlineExceededAsTimeout(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handle := &Handle{
		enabled: true,
		tracer:  provider.Tracer("test"),
		meter:   noop.NewMeterProvider().Meter("test"),
	}

	_, op := handle.StartOperation(context.Background(), OperationConfig{
		Name: "test.timeout",
	})
	op.End(context.DeadlineExceeded)

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	attrs := ended[0].Attributes()
	for _, attr := range attrs {
		if attr.Key == AttrResult && attr.Value.AsString() == ResultTimeout {
			return
		}
	}
	t.Fatalf("span attrs = %#v, want %s=%s", attrs, AttrResult, ResultTimeout)
}
