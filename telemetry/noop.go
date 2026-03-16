/*
 * noop.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package telemetry

import "context"

// ── NoopProvider ──────────────────────────────────────────────────────────────

// NoopProvider is the default Provider. All operations are no-ops with
// zero allocations (tracer/meter/span/counter singletons are returned by value).
type NoopProvider struct{}

func (NoopProvider) Tracer(_ string) Tracer { return NoopTracer{} }
func (NoopProvider) Meter(_ string) Meter   { return NoopMeter{} }

// ── NoopTracer ────────────────────────────────────────────────────────────────

// NoopTracer is a Tracer that creates zero-cost no-op spans.
type NoopTracer struct{}

func (NoopTracer) Start(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, NoopSpan{}
}

func (NoopTracer) Inject(_ context.Context, _ map[string]string)                          {}
func (NoopTracer) Extract(ctx context.Context, _ map[string]string) context.Context       { return ctx }

// ── NoopSpan ──────────────────────────────────────────────────────────────────

// NoopSpan is a Span that discards all data.
type NoopSpan struct{}

func (NoopSpan) End()                         {}
func (NoopSpan) SetAttribute(_ string, _ any) {}
func (NoopSpan) RecordError(_ error)          {}
func (NoopSpan) IsRecording() bool            { return false }

// ── NoopMeter ─────────────────────────────────────────────────────────────────

// NoopMeter is a Meter that creates zero-cost no-op instruments.
type NoopMeter struct{}

func (NoopMeter) Counter(_, _, _ string) Counter             { return NoopCounter{} }
func (NoopMeter) UpDownCounter(_, _, _ string) UpDownCounter { return NoopUpDownCounter{} }
func (NoopMeter) Histogram(_, _, _ string) Histogram         { return NoopHistogram{} }

// ── Noop instruments ──────────────────────────────────────────────────────────

type NoopCounter struct{}

func (NoopCounter) Add(_ context.Context, _ int64, _ ...Attribute) {}

type NoopUpDownCounter struct{}

func (NoopUpDownCounter) Add(_ context.Context, _ int64, _ ...Attribute) {}

type NoopHistogram struct{}

func (NoopHistogram) Record(_ context.Context, _ float64, _ ...Attribute) {}
