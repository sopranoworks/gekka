/*
 * provider.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package otel provides an OpenTelemetry-backed implementation of the
// gekka telemetry interfaces.
//
// Import this package — and only this package — when you want real OTEL
// instrumentation. The root telemetry package contains only interfaces and
// no-op defaults, so code that never imports telemetry/otel is never linked
// against the go.opentelemetry.io SDK.
//
// Typical setup in your application entry point:
//
//	import (
//	    "go.opentelemetry.io/otel"
//	    "go.opentelemetry.io/otel/propagation"
//	    sdktrace "go.opentelemetry.io/otel/sdk/trace"
//
//	    "github.com/sopranoworks/gekka/telemetry"
//	    "github.com/sopranoworks/gekka/telemetry/otel"
//	)
//
//	func initTelemetry(ctx context.Context) {
//	    tp := sdktrace.NewTracerProvider(...)
//	    otelapi.SetTracerProvider(tp)
//	    otelapi.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
//	        propagation.TraceContext{},
//	        propagation.Baggage{},
//	    ))
//	    telemetry.SetProvider(otel.NewProvider())
//	}
package otel

import (
	"context"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/sopranoworks/gekka/telemetry"
)

// ── Provider ──────────────────────────────────────────────────────────────────

// Provider wraps the global OTEL TracerProvider and MeterProvider.
// It satisfies the telemetry.Provider interface.
//
// Register it once before starting actors:
//
//	telemetry.SetProvider(otel.NewProvider())
type Provider struct{}

// NewProvider returns a Provider that delegates to:
//   - otel.GetTracerProvider() for tracing
//   - otel.GetMeterProvider() for metrics
//   - otel.GetTextMapPropagator() for W3C trace-context propagation
//
// Configure the global OTEL SDK before calling NewProvider so the correct
// exporter is in place.
func NewProvider() *Provider { return &Provider{} }

func (Provider) Tracer(name string) telemetry.Tracer {
	return &otelTracer{
		inner:      otelapi.GetTracerProvider().Tracer(name),
		propagator: otelapi.GetTextMapPropagator(),
	}
}

func (Provider) Meter(name string) telemetry.Meter {
	return &otelMeter{inner: otelapi.GetMeterProvider().Meter(name)}
}

// ── otelTracer ────────────────────────────────────────────────────────────────

type otelTracer struct {
	inner      oteltrace.Tracer
	propagator propagation.TextMapPropagator
}

func (t *otelTracer) Start(ctx context.Context, spanName string) (context.Context, telemetry.Span) {
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
	s.inner.SetAttributes(toKV(telemetry.Attribute{Key: key, Value: value}))
}

func (s *otelSpan) RecordError(err error) { s.inner.RecordError(err) }
func (s *otelSpan) IsRecording() bool     { return s.inner.IsRecording() }

// ── otelMeter ─────────────────────────────────────────────────────────────────

type otelMeter struct{ inner otelmetric.Meter }

func (m *otelMeter) Counter(name, description, unit string) telemetry.Counter {
	c, err := m.inner.Int64Counter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return telemetry.NoopCounter{}
	}
	return &otelCounter{inner: c}
}

func (m *otelMeter) UpDownCounter(name, description, unit string) telemetry.UpDownCounter {
	c, err := m.inner.Int64UpDownCounter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return telemetry.NoopUpDownCounter{}
	}
	return &otelUpDownCounter{inner: c}
}

func (m *otelMeter) Histogram(name, description, unit string) telemetry.Histogram {
	h, err := m.inner.Float64Histogram(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		return telemetry.NoopHistogram{}
	}
	return &otelHistogram{inner: h}
}

// ── otel instruments ──────────────────────────────────────────────────────────

type otelCounter struct{ inner otelmetric.Int64Counter }

func (c *otelCounter) Add(ctx context.Context, delta int64, attrs ...telemetry.Attribute) {
	c.inner.Add(ctx, delta, otelmetric.WithAttributes(toKVs(attrs)...))
}

type otelUpDownCounter struct{ inner otelmetric.Int64UpDownCounter }

func (c *otelUpDownCounter) Add(ctx context.Context, delta int64, attrs ...telemetry.Attribute) {
	c.inner.Add(ctx, delta, otelmetric.WithAttributes(toKVs(attrs)...))
}

type otelHistogram struct{ inner otelmetric.Float64Histogram }

func (h *otelHistogram) Record(ctx context.Context, value float64, attrs ...telemetry.Attribute) {
	h.inner.Record(ctx, value, otelmetric.WithAttributes(toKVs(attrs)...))
}

// ── attribute conversion helpers ──────────────────────────────────────────────

func toKV(a telemetry.Attribute) attribute.KeyValue {
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

func toKVs(attrs []telemetry.Attribute) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	kvs := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		kvs[i] = toKV(a)
	}
	return kvs
}
