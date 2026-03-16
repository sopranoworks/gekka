/*
 * otel_provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package telemetry

// OtelProvider implements Provider using the go.opentelemetry.io API packages.
//
// It delegates to the global TracerProvider and MeterProvider registered by
// the application's SDK setup (e.g. OTLP, Jaeger, Prometheus exporters).
// The application is responsible for configuring and installing the SDK;
// gekka only calls the stable OTEL API.
//
// Typical setup:
//
//	// 1. Configure OTEL SDK (application-specific)
//	tp := otelsdktrace.NewTracerProvider(
//	    otelsdktrace.WithBatcher(otlptracehttp.New(ctx)),
//	)
//	otel.SetTracerProvider(tp)
//	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
//	    propagation.TraceContext{},
//	    propagation.Baggage{},
//	))
//
//	// 2. Register with gekka
//	telemetry.SetProvider(telemetry.NewOtelProvider())

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// ── OtelProvider ──────────────────────────────────────────────────────────────

// OtelProvider wraps the global OTEL TracerProvider and MeterProvider.
type OtelProvider struct{}

// NewOtelProvider returns an OtelProvider that reads from:
//   - otel.GetTracerProvider() for tracing
//   - otel.GetMeterProvider() for metrics
//   - otel.GetTextMapPropagator() for W3C trace-context propagation
func NewOtelProvider() *OtelProvider { return &OtelProvider{} }

func (OtelProvider) Tracer(name string) Tracer {
	return &otelTracer{
		inner:      otel.GetTracerProvider().Tracer(name),
		propagator: otel.GetTextMapPropagator(),
	}
}

func (OtelProvider) Meter(name string) Meter {
	return &otelMeter{inner: otel.GetMeterProvider().Meter(name)}
}

// ── otelTracer ────────────────────────────────────────────────────────────────

type otelTracer struct {
	inner      oteltrace.Tracer
	propagator propagation.TextMapPropagator
}

func (t *otelTracer) Start(ctx context.Context, spanName string) (context.Context, Span) {
	ctx, span := t.inner.Start(ctx, spanName)
	return ctx, &otelSpan{inner: span}
}

func (t *otelTracer) Inject(ctx context.Context, carrier map[string]string) {
	t.propagator.Inject(ctx, propagation.MapCarrier(carrier))
}

func (t *otelTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return t.propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

// ── otelSpan ─────────────────────────────────────────────────────────────────

type otelSpan struct{ inner oteltrace.Span }

func (s *otelSpan) End() { s.inner.End() }

func (s *otelSpan) SetAttribute(key string, value any) {
	s.inner.SetAttributes(toKV(Attribute{Key: key, Value: value}))
}

func (s *otelSpan) RecordError(err error) { s.inner.RecordError(err) }
func (s *otelSpan) IsRecording() bool     { return s.inner.IsRecording() }

// ── otelMeter ─────────────────────────────────────────────────────────────────

type otelMeter struct{ inner otelmetric.Meter }

func (m *otelMeter) Counter(name, description, unit string) Counter {
	c, err := m.inner.Int64Counter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return NoopCounter{}
	}
	return &otelCounter{inner: c}
}

func (m *otelMeter) UpDownCounter(name, description, unit string) UpDownCounter {
	c, err := m.inner.Int64UpDownCounter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return NoopUpDownCounter{}
	}
	return &otelUpDownCounter{inner: c}
}

func (m *otelMeter) Histogram(name, description, unit string) Histogram {
	h, err := m.inner.Float64Histogram(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return NoopHistogram{}
	}
	return &otelHistogram{inner: h}
}

// ── otel instruments ──────────────────────────────────────────────────────────

type otelCounter struct{ inner otelmetric.Int64Counter }

func (c *otelCounter) Add(ctx context.Context, delta int64, attrs ...Attribute) {
	c.inner.Add(ctx, delta, otelmetric.WithAttributes(toKVs(attrs)...))
}

type otelUpDownCounter struct{ inner otelmetric.Int64UpDownCounter }

func (c *otelUpDownCounter) Add(ctx context.Context, delta int64, attrs ...Attribute) {
	c.inner.Add(ctx, delta, otelmetric.WithAttributes(toKVs(attrs)...))
}

type otelHistogram struct{ inner otelmetric.Float64Histogram }

func (h *otelHistogram) Record(ctx context.Context, value float64, attrs ...Attribute) {
	h.inner.Record(ctx, value, otelmetric.WithAttributes(toKVs(attrs)...))
}

// ── attribute conversion helpers ──────────────────────────────────────────────

func toKV(a Attribute) attribute.KeyValue {
	switch v := a.Value.(type) {
	case string:
		return attribute.String(a.Key, v)
	case int:
		return attribute.Int64(a.Key, int64(v))
	case int64:
		return attribute.Int64(a.Key, v)
	case float64:
		return attribute.Float64(a.Key, v)
	case bool:
		return attribute.Bool(a.Key, v)
	default:
		return attribute.String(a.Key, "?")
	}
}

func toKVs(attrs []Attribute) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	kvs := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		kvs[i] = toKV(a)
	}
	return kvs
}
