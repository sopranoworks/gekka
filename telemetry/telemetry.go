/*
 * telemetry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package telemetry defines the observability abstractions used by gekka.
//
// All interfaces default to no-op implementations so that telemetry has zero
// overhead when not configured.  To enable real instrumentation register a
// Provider before starting actors:
//
//	import gekkaotel "github.com/sopranoworks/gekka/telemetry/otel"
//
//	telemetry.SetProvider(gekkaotel.NewProvider())
//
// The package is intentionally dependency-free; the OTEL-backed implementation
// lives in the sub-package telemetry/otel and is only linked when explicitly
// imported.
package telemetry

import "context"

// ── Provider ─────────────────────────────────────────────────────────────────

// Provider is the root telemetry factory that bundles a Tracer and a Meter.
// Obtain one from telemetry/otel.NewProvider or supply your own implementation.
type Provider interface {
	// Tracer returns the Tracer for the given instrumentation scope name.
	// The name is typically the fully-qualified package path
	// (e.g. "github.com/sopranoworks/gekka/actor").
	Tracer(instrumentationName string) Tracer

	// Meter returns the Meter for the given instrumentation scope name.
	Meter(instrumentationName string) Meter
}

// ── Tracing ───────────────────────────────────────────────────────────────────

// Tracer creates and propagates trace spans.
type Tracer interface {
	// Start creates a child span of any span found in ctx.
	// The returned context carries the new span; pass it to downstream
	// operations so they are recorded as children.
	// Always call span.End() when done, typically via defer.
	Start(ctx context.Context, spanName string) (context.Context, Span)

	// Inject writes W3C trace-context headers (traceparent, tracestate)
	// derived from the active span in ctx into carrier.
	// Use this before placing a message into an actor mailbox or sending
	// it over the wire so the receiver can reconstruct the trace chain.
	Inject(ctx context.Context, carrier map[string]string)

	// Extract reads W3C trace-context headers from carrier and returns a
	// context that, when passed to Start, makes the remote span a logical
	// child of the originating trace.
	Extract(ctx context.Context, carrier map[string]string) context.Context
}

// Span represents an active, sampledtrace span.  Obtain one from Tracer.Start.
type Span interface {
	// End marks the span as finished. Must be called exactly once.
	End()

	// SetAttribute records a typed key-value attribute on this span.
	SetAttribute(key string, value any)

	// RecordError adds an error event to the span and (by convention)
	// marks the span status as Error.
	RecordError(err error)

	// IsRecording reports whether this span is currently capturing data.
	// Noop spans always return false; check this before expensive attribute
	// construction.
	IsRecording() bool
}

// ── Metrics ───────────────────────────────────────────────────────────────────

// Meter creates and manages metric instruments.
type Meter interface {
	// Counter returns a monotonically increasing cumulative instrument.
	// name should follow OpenTelemetry naming conventions
	// (e.g. "gekka.actor.messages.total").
	Counter(name, description, unit string) Counter

	// UpDownCounter returns a sum instrument that can increase or decrease.
	// Suitable for values like queue depth or active connection count.
	UpDownCounter(name, description, unit string) UpDownCounter

	// Histogram returns a value-distribution instrument.
	// Suitable for latency, payload size, etc.
	Histogram(name, description, unit string) Histogram
}

// Counter is a monotonically increasing cumulative instrument.
type Counter interface {
	Add(ctx context.Context, delta int64, attrs ...Attribute)
}

// UpDownCounter is a sum instrument that can both increase and decrease.
type UpDownCounter interface {
	Add(ctx context.Context, delta int64, attrs ...Attribute)
}

// Histogram records the distribution of measured values.
type Histogram interface {
	Record(ctx context.Context, value float64, attrs ...Attribute)
}

// Attribute is a typed key-value pair attached to metrics observations or
// span attributes.
type Attribute struct {
	Key   string
	Value any // string | int64 | float64 | bool
}

// StringAttr is a convenience constructor for a string-valued Attribute.
func StringAttr(key, value string) Attribute { return Attribute{Key: key, Value: value} }

// Int64Attr is a convenience constructor for an int64-valued Attribute.
func Int64Attr(key string, value int64) Attribute { return Attribute{Key: key, Value: value} }

// Float64Attr is a convenience constructor for a float64-valued Attribute.
func Float64Attr(key string, value float64) Attribute { return Attribute{Key: key, Value: value} }

// BoolAttr is a convenience constructor for a bool-valued Attribute.
func BoolAttr(key string, value bool) Attribute { return Attribute{Key: key, Value: value} }

// ── Global provider ───────────────────────────────────────────────────────────

var global Provider = NoopProvider{}

// SetProvider installs p as the process-wide telemetry provider.
//
// Call this once during application initialisation, before any actors are
// started. SetProvider is not goroutine-safe; do not call it concurrently
// with actor operations.
func SetProvider(p Provider) {
	if p == nil {
		p = NoopProvider{}
	}
	global = p
}

// Global returns the current process-wide Provider.
// When no provider has been set, this returns a NoopProvider.
func Global() Provider { return global }

// GetTracer is a convenience wrapper that returns a Tracer from the global
// provider for the given instrumentation scope name.
func GetTracer(instrumentationName string) Tracer {
	return global.Tracer(instrumentationName)
}

// GetMeter is a convenience wrapper that returns a Meter from the global
// provider for the given instrumentation scope name.
func GetMeter(instrumentationName string) Meter {
	return global.Meter(instrumentationName)
}
