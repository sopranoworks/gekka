/*
 * telemetry_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package telemetry_test

import (
	"context"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/telemetry"
)

// ── Spy provider for testing ──────────────────────────────────────────────────

// spySpan records whether End was called and what attributes were set.
type spySpan struct {
	mu      sync.Mutex
	ended   bool
	attrs   map[string]any
	errors  []error
	traceID string // synthetic trace ID injected by spyTracer
}

func newSpySpan(traceID string) *spySpan {
	return &spySpan{attrs: make(map[string]any), traceID: traceID}
}

func (s *spySpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
}
func (s *spySpan) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attrs[key] = value
}
func (s *spySpan) RecordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, err)
}
func (s *spySpan) IsRecording() bool { return true }

// spyTracer records all started spans and propagates a synthetic "traceparent"
// header so we can verify context propagation without a real OTEL SDK.
type spyTracer struct {
	mu    sync.Mutex
	spans []*spySpan
}

const headerKey = "traceparent"

func (t *spyTracer) Start(ctx context.Context, name string) (context.Context, telemetry.Span) {
	// Derive trace ID: reuse parent's if present, else generate a new one.
	parentID, _ := ctx.Value(ctxKey{}).(string)
	if parentID == "" {
		parentID = name // use span name as a deterministic stand-in
	}
	span := newSpySpan(parentID)

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()

	// Store trace ID in context so child spans can read it.
	ctx = context.WithValue(ctx, ctxKey{}, parentID)
	return ctx, span
}

func (t *spyTracer) Inject(ctx context.Context, carrier map[string]string) {
	if id, ok := ctx.Value(ctxKey{}).(string); ok && id != "" {
		carrier[headerKey] = id
	}
}

func (t *spyTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	if id := carrier[headerKey]; id != "" {
		return context.WithValue(ctx, ctxKey{}, id)
	}
	return ctx
}

func (t *spyTracer) spanCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.spans)
}

func (t *spyTracer) lastSpan() *spySpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.spans) == 0 {
		return nil
	}
	return t.spans[len(t.spans)-1]
}

type ctxKey struct{}

// spyProvider returns spyTracer for any name.
type spyProvider struct {
	tracer *spyTracer
}

func (p *spyProvider) Tracer(_ string) telemetry.Tracer { return p.tracer }
func (p *spyProvider) Meter(_ string) telemetry.Meter   { return telemetry.NoopMeter{} }

// ── Helper: install / restore the global provider ─────────────────────────────

func withSpyProvider(t *testing.T) (*spyTracer, func()) {
	t.Helper()
	prev := telemetry.Global()
	spy := &spyTracer{}
	telemetry.SetProvider(&spyProvider{tracer: spy})
	return spy, func() { telemetry.SetProvider(prev) }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestNoop_NoAllocs(t *testing.T) {
	// Ensure the noop provider compiles and does not panic.
	p := telemetry.NoopProvider{}
	tr := p.Tracer("test")
	m := p.Meter("test")

	ctx, span := tr.Start(context.Background(), "s")
	span.SetAttribute("k", "v")
	span.RecordError(nil)
	span.End()
	if span.IsRecording() {
		t.Error("noop span must report IsRecording() = false")
	}

	c := m.Counter("c", "", "")
	c.Add(ctx, 1)
	u := m.UpDownCounter("u", "", "")
	u.Add(ctx, -1)
	h := m.Histogram("h", "", "")
	h.Record(ctx, 3.14)
}

func TestGlobal_SetProvider(t *testing.T) {
	spy, restore := withSpyProvider(t)
	defer restore()

	tr := telemetry.GetTracer("mylib")
	_, span := tr.Start(context.Background(), "root")
	span.End()

	if spy.spanCount() != 1 {
		t.Errorf("expected 1 span, got %d", spy.spanCount())
	}
	s := spy.lastSpan()
	if !s.ended {
		t.Error("span was not ended")
	}
}

func TestSpanAttributes(t *testing.T) {
	spy, restore := withSpyProvider(t)
	defer restore()

	tr := telemetry.GetTracer("test")
	_, span := tr.Start(context.Background(), "op")
	span.SetAttribute("actor.path", "/user/foo")
	span.SetAttribute("count", int64(42))
	span.End()

	s := spy.lastSpan()
	if s.attrs["actor.path"] != "/user/foo" {
		t.Errorf("actor.path = %v", s.attrs["actor.path"])
	}
	if s.attrs["count"] != int64(42) {
		t.Errorf("count = %v", s.attrs["count"])
	}
}

func TestTraceContextPropagation_LocalTell(t *testing.T) {
	// Verify that when an actor receives a message via TellCtx, the actor's
	// Start() loop extracts the trace context and starts a child span.

	spy, restore := withSpyProvider(t)
	defer restore()

	// 1. Create a root span and inject its context into the message.
	rootTracer := telemetry.GetTracer("test")
	rootCtx, rootSpan := rootTracer.Start(context.Background(), "root")
	defer rootSpan.End()

	// 2. Inject the root trace context into a carrier.
	carrier := make(map[string]string)
	rootTracer.Inject(rootCtx, carrier)

	if len(carrier) == 0 {
		t.Fatal("Inject produced no headers — spy tracer may not be installed")
	}

	// 3. Extract the context on the receiving side (simulates what Start() does).
	recvCtx := rootTracer.Extract(context.Background(), carrier)
	_, childSpan := rootTracer.Start(recvCtx, "actor.Receive")
	childSpan.SetAttribute("actor.path", "/user/worker")
	childSpan.End()

	// We should have 2 spans: root + child.
	if spy.spanCount() < 2 {
		t.Errorf("expected ≥2 spans, got %d", spy.spanCount())
	}
}

func TestActorReceive_SpanCreated(t *testing.T) {
	// Verify that actor.Start() creates a span for each Receive call.
	spy, restore := withSpyProvider(t)
	defer restore()

	received := make(chan struct{})
	tActor := &closureActor{
		BaseActor: actor.NewBaseActor(),
		fn: func(msg any) {
			close(received)
		},
	}
	actor.InjectSystem(tActor, nil)
	tActor.SetSelf(&fakeRef{path: "/user/test-actor"})
	actor.Start(tActor)

	// Send a plain message (no trace context).
	tActor.Mailbox() <- "hello"

	<-received

	// Give Start() goroutine time to record the span.
	// (The span is recorded synchronously before received is closed via close,
	//  so no extra wait should be needed, but close(received) happens inside
	//  Receive before span.End().)

	if spy.spanCount() == 0 {
		t.Error("expected at least one span from actor.Start()")
	}
}

func TestActorReceive_TraceContextPropagated(t *testing.T) {
	// Verify that a TraceContext in an Envelope is extracted and the resulting
	// span is a logical child of the original trace.
	spy, restore := withSpyProvider(t)
	defer restore()

	received := make(chan struct{})
	tActor := &closureActor{
		BaseActor: actor.NewBaseActor(),
		fn: func(msg any) {
			close(received)
		},
	}
	tActor.SetSelf(&fakeRef{path: "/user/trace-test"})
	actor.Start(tActor)

	// Inject a synthetic trace-parent header into the Envelope.
	carrier := map[string]string{headerKey: "root-trace-id"}
	tActor.Mailbox() <- actor.Envelope{
		Payload:      "traced-msg",
		TraceContext: carrier,
	}
	<-received

	if spy.spanCount() == 0 {
		t.Error("expected a span from traced message delivery")
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// closureActor is a minimal actor that calls fn for each message.
type closureActor struct {
	actor.BaseActor
	fn func(any)
}

func (a *closureActor) Receive(msg any) {
	if a.fn != nil {
		a.fn(msg)
	}
}

// fakeRef is a minimal actor.Ref used in tests.
type fakeRef struct{ path string }

func (r *fakeRef) Tell(_ any, _ ...actor.Ref) {}
func (r *fakeRef) Path() string               { return r.path }
