package observability

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/cofy-x/kova/internal/observability"

type Handle struct {
	enabled bool
	tracer  trace.Tracer
	meter   metric.Meter
	tp      *sdktrace.TracerProvider
	mp      *sdkmetric.MeterProvider
	lp      *sdklog.LoggerProvider
}

var active atomic.Pointer[Handle]

func Init(ctx context.Context, cfg Config) (*Handle, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if !cfg.Enabled {
		handle := noopHandle()
		active.Store(handle)
		return handle, nil
	}

	resourceAttrs := []attribute.KeyValue{
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.version", cfg.ServiceVersion),
		attribute.String(AttrComponent, cfg.Component),
	}
	resourceAttrs = append(resourceAttrs, cfg.ResourceAttrs...)
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(SafeAttrs(resourceAttrs...)...))
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	handle := &Handle{
		enabled: true,
		tracer:  otel.Tracer(instrumentationName),
		meter:   otel.Meter(instrumentationName),
	}

	if cfg.TracesEnabled {
		traceOptions := traceExporterOptions(cfg)
		if cfg.OTLPInsecure {
			traceOptions = append(traceOptions, otlptracegrpc.WithInsecure())
		}
		traceExporter, err := otlptracegrpc.New(ctx, traceOptions...)
		if err != nil {
			return nil, fmt.Errorf("create trace exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter))
		otel.SetTracerProvider(tp)
		handle.tp = tp
		handle.tracer = tp.Tracer(instrumentationName)
	}

	if cfg.MetricsEnabled {
		metricOptions := metricExporterOptions(cfg)
		if cfg.OTLPInsecure {
			metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
		}
		metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
		if err != nil {
			return nil, fmt.Errorf("create metric exporter: %w", err)
		}
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(cfg.MetricInterval))),
		)
		otel.SetMeterProvider(mp)
		handle.mp = mp
		handle.meter = mp.Meter(instrumentationName)
	}

	if cfg.LogsEnabled {
		logOptions := logExporterOptions(cfg)
		if cfg.OTLPInsecure {
			logOptions = append(logOptions, otlploggrpc.WithInsecure())
		}
		logExporter, err := otlploggrpc.New(ctx, logOptions...)
		if err != nil {
			return nil, fmt.Errorf("create log exporter: %w", err)
		}
		lp := sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		)
		global.SetLoggerProvider(lp)
		handle.lp = lp
	}

	active.Store(handle)
	return handle, nil
}

func Active() *Handle {
	if handle := active.Load(); handle != nil {
		return handle
	}
	return noopHandle()
}

func noopHandle() *Handle {
	return &Handle{
		tracer: otel.Tracer(instrumentationName),
		meter:  otel.Meter(instrumentationName),
	}
}

func traceExporterOptions(cfg Config) []otlptracegrpc.Option {
	endpoint := strings.TrimSpace(cfg.OTLPEndpointURL)
	if endpoint == "" {
		return nil
	}
	if strings.Contains(endpoint, "://") {
		return []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(endpoint)}
	}
	return []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
}

func metricExporterOptions(cfg Config) []otlpmetricgrpc.Option {
	endpoint := strings.TrimSpace(cfg.OTLPEndpointURL)
	if endpoint == "" {
		return nil
	}
	if strings.Contains(endpoint, "://") {
		return []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(endpoint)}
	}
	return []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
}

func logExporterOptions(cfg Config) []otlploggrpc.Option {
	endpoint := strings.TrimSpace(cfg.OTLPEndpointURL)
	if endpoint == "" {
		return nil
	}
	if strings.Contains(endpoint, "://") {
		return []otlploggrpc.Option{otlploggrpc.WithEndpointURL(endpoint)}
	}
	return []otlploggrpc.Option{otlploggrpc.WithEndpoint(endpoint)}
}

func (h *Handle) Enabled() bool {
	return h != nil && h.enabled
}

func (h *Handle) Shutdown(ctx context.Context) error {
	if h == nil || !h.enabled {
		return nil
	}
	var first error
	if h.lp != nil {
		if err := h.lp.Shutdown(ctx); err != nil {
			first = err
		}
	}
	if h.mp != nil {
		if err := h.mp.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if h.tp != nil {
		if err := h.tp.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (h *Handle) Tracer() trace.Tracer {
	if h == nil || h.tracer == nil {
		return otel.Tracer(instrumentationName)
	}
	return h.tracer
}

func (h *Handle) Meter() metric.Meter {
	if h == nil || h.meter == nil {
		return otel.Meter(instrumentationName)
	}
	return h.meter
}

func (h *Handle) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return h.Tracer().Start(ctx, name, trace.WithAttributes(SafeAttrs(attrs...)...))
}

func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Active().Start(ctx, name, attrs...)
}
