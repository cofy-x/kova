package observability

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Counter struct {
	counter metric.Int64Counter
}

type Histogram struct {
	histogram metric.Float64Histogram
}

var (
	counterCache   sync.Map
	histogramCache sync.Map
)

func Int64Counter(name, description string) Counter {
	key := name + "\x00" + description
	if cached, ok := counterCache.Load(key); ok {
		return cached.(Counter)
	}
	counter, _ := otel.Meter(instrumentationName).Int64Counter(name, metric.WithDescription(description))
	wrapped := Counter{counter: counter}
	actual, _ := counterCache.LoadOrStore(key, wrapped)
	return actual.(Counter)
}

func DurationHistogram(name, description string) Histogram {
	key := name + "\x00" + description
	if cached, ok := histogramCache.Load(key); ok {
		return cached.(Histogram)
	}
	histogram, _ := otel.Meter(instrumentationName).Float64Histogram(name, metric.WithDescription(description), metric.WithUnit("s"))
	wrapped := Histogram{histogram: histogram}
	actual, _ := histogramCache.LoadOrStore(key, wrapped)
	return actual.(Histogram)
}

func (c Counter) Add(ctx context.Context, value int64, attrs ...attribute.KeyValue) {
	if c.counter == nil {
		return
	}
	c.counter.Add(ctx, value, metric.WithAttributes(SafeAttrs(attrs...)...))
}

func (h Histogram) RecordDuration(ctx context.Context, value time.Duration, attrs ...attribute.KeyValue) {
	if h.histogram == nil {
		return
	}
	h.histogram.Record(ctx, value.Seconds(), metric.WithAttributes(SafeAttrs(attrs...)...))
}
