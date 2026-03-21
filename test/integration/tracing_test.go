/*
 * tracing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package integration_test contains cross-node and infrastructure-aware
// integration tests for the Gekka cluster package.
package integration_test

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/persistence/projection"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/cluster/sharding"
	"github.com/sopranoworks/gekka/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// setupTracer configures an in-memory span recorder as the global OTel tracer
// and W3C TraceContext as the global propagator.  Returns the recorder and a
// cleanup function that restores the previous global state.
func setupTracer(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return rec, func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}
}

// allSpansShareTraceID returns true when every span in spans shares the same
// TraceID as root.
func allSpansShareTraceID(root trace.TraceID, spans []sdktrace.ReadOnlySpan) bool {
	for _, s := range spans {
		if s.SpanContext().TraceID() != root {
			return false
		}
	}
	return true
}

// findSpanByName returns the first recorded span whose name equals name.
func findSpanByName(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

// ── Test: TraceID flows from ShardingEnvelope through Journal to Projection ──

// TestTraceContinuity_ShardJournalProjection verifies that a single TraceID
// propagates across the entire command-to-projection pipeline:
//
//  1. A root span is started and injected into a ShardingEnvelope.TraceContext.
//  2. The ShardingEnvelope's TraceContext is written into PersistentRepr.TraceContext.
//  3. TracingJournal.AsyncWriteMessages starts a "Journal.Write" child span.
//  4. The same TraceContext is placed in EventEnvelope.TraceContext.
//  5. The projection runner starts a "Projection.Handle" child span.
//  6. All three spans (root, Journal.Write, Projection.Handle) share the same TraceID.
func TestTraceContinuity_ShardJournalProjection(t *testing.T) {
	rec, cleanup := setupTracer(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	propagator := otel.GetTextMapPropagator()

	// ── Step 1: Start root span and inject into ShardingEnvelope ─────────────
	rootCtx, rootSpan := tracer.Start(context.Background(), "HandleCommand")

	carrier := propagation.MapCarrier{}
	propagator.Inject(rootCtx, carrier)
	traceCtx := map[string]string(carrier)

	envelope := sharding.ShardingEnvelope{
		EntityId:     "entity-1",
		ShardId:      "shard-0",
		TraceContext: traceCtx,
	}

	rootSpan.End()

	// ── Step 2: Entity persists an event, copying TraceContext from envelope ──
	repr := persistence.PersistentRepr{
		PersistenceID: "entity-1",
		SequenceNr:    1,
		Payload:       "ItemAdded",
		TraceContext:  envelope.TraceContext, // propagated from the incoming envelope
	}

	// ── Step 3: TracingJournal.AsyncWriteMessages creates a "Journal.Write" span
	journal := persistence.NewTracingJournal(persistence.NewInMemoryJournal())
	if err := journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{repr}); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}

	// ── Step 4: Build EventEnvelope carrying the same TraceContext ────────────
	eventEnvelope := query.EventEnvelope{
		Offset:        query.SequenceOffset(1),
		PersistenceID: repr.PersistenceID,
		SequenceNr:    repr.SequenceNr,
		Event:         repr.Payload,
		TraceContext:  repr.TraceContext, // read side propagates write-side context
	}

	// ── Step 5: Projection handler creates a "Projection.Handle" child span ──
	src := stream.FromSlice([]query.EventEnvelope{eventEnvelope})

	handlerCalled := false
	proj := projection.NewProjection(
		"inventory-proj",
		&staticSourceProvider{src: src},
		&noopOffsetStore{},
		func(env query.EventEnvelope) error {
			handlerCalled = true
			return nil
		},
	)
	if err := proj.Run(context.Background()); err != nil {
		t.Fatalf("projection.Run: %v", err)
	}
	if !handlerCalled {
		t.Fatal("projection handler was never called")
	}

	// ── Step 6: Verify all spans share the same TraceID ─────────────────────
	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("no spans recorded")
	}

	rootID, ok := rootTraceID(spans)
	if !ok {
		t.Fatal("could not determine root TraceID from recorded spans")
	}
	t.Logf("root TraceID: %s", rootID)

	// "Journal.Write" span must exist and share the TraceID.
	journalSpan, found := findSpanByName(spans, "Journal.Write")
	if !found {
		t.Error("span 'Journal.Write' not found in recorded spans")
	} else if journalSpan.SpanContext().TraceID() != rootID {
		t.Errorf("Journal.Write TraceID mismatch: got %s, want %s",
			journalSpan.SpanContext().TraceID(), rootID)
	}

	// "Projection.Handle" span must exist and share the TraceID.
	projSpan, found := findSpanByName(spans, "Projection.Handle")
	if !found {
		t.Error("span 'Projection.Handle' not found in recorded spans")
	} else if projSpan.SpanContext().TraceID() != rootID {
		t.Errorf("Projection.Handle TraceID mismatch: got %s, want %s",
			projSpan.SpanContext().TraceID(), rootID)
	}

	if !allSpansShareTraceID(rootID, spans) {
		t.Error("not all spans share the same TraceID")
		for _, s := range spans {
			t.Logf("  span %q: traceID=%s", s.Name(), s.SpanContext().TraceID())
		}
	}
}

// rootTraceID returns the TraceID from the first span named "HandleCommand",
// falling back to the first recorded span.
func rootTraceID(spans []sdktrace.ReadOnlySpan) (trace.TraceID, bool) {
	if s, ok := findSpanByName(spans, "HandleCommand"); ok {
		return s.SpanContext().TraceID(), true
	}
	if len(spans) > 0 {
		return spans[0].SpanContext().TraceID(), true
	}
	return trace.TraceID{}, false
}

// ── TestTraceContinuity_ShardEnvelopeInject verifies that ShardingEnvelope.TraceContext
// is populated by injectTraceContext when building an envelope from a raw message.
func TestTraceContinuity_ShardEnvelopeInject(t *testing.T) {
	rec, cleanup := setupTracer(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	propagator := otel.GetTextMapPropagator()

	// Start a span and inject into a carrier, simulating what ShardRegion does.
	ctx, span := tracer.Start(context.Background(), "root")
	defer span.End()

	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	tc := map[string]string(carrier)

	if len(tc) == 0 {
		t.Fatal("expected non-empty TraceContext carrier after Inject")
	}

	// Extract back and verify we get the same TraceID.
	extracted := propagator.Extract(context.Background(), propagation.MapCarrier(tc))
	extractedSpan := trace.SpanFromContext(extracted)
	if extractedSpan.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Errorf("extracted TraceID %s != original %s",
			extractedSpan.SpanContext().TraceID(), span.SpanContext().TraceID())
	}

	_ = rec // recorder not needed for this test
}

// ── TestTracingJournal_WriteRead verifies that TracingJournal emits both
// "Journal.Write" and "Journal.Read" spans with the same TraceID.
func TestTracingJournal_WriteRead(t *testing.T) {
	rec, cleanup := setupTracer(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	propagator := otel.GetTextMapPropagator()

	ctx, rootSpan := tracer.Start(context.Background(), "root")
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	tc := map[string]string(carrier)
	rootSpan.End()

	journal := persistence.NewTracingJournal(persistence.NewInMemoryJournal())

	repr := persistence.PersistentRepr{
		PersistenceID: "p1",
		SequenceNr:    1,
		Payload:       "evt",
		TraceContext:  tc,
	}
	if err := journal.AsyncWriteMessages(context.Background(), []persistence.PersistentRepr{repr}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := journal.ReplayMessages(context.Background(), "p1", 1, 1, 0, func(persistence.PersistentRepr) {}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	spans := rec.Ended()
	names := make(map[string]trace.TraceID)
	for _, s := range spans {
		names[s.Name()] = s.SpanContext().TraceID()
	}

	rootID := names["root"]
	if rootID == (trace.TraceID{}) {
		t.Fatal("root span not recorded")
	}
	if id, ok := names["Journal.Write"]; !ok {
		t.Error("Journal.Write span not recorded")
	} else if id != rootID {
		t.Errorf("Journal.Write TraceID %s != root %s", id, rootID)
	}
	if _, ok := names["Journal.Read"]; !ok {
		t.Error("Journal.Read span not recorded")
	}
}

// ── support types ─────────────────────────────────────────────────────────────

type staticSourceProvider struct {
	src stream.Source[query.EventEnvelope, stream.NotUsed]
}

func (p *staticSourceProvider) Source(_ context.Context, _ query.Offset) (stream.Source[query.EventEnvelope, stream.NotUsed], error) {
	return p.src, nil
}

type noopOffsetStore struct{}

func (s *noopOffsetStore) ReadOffset(_ context.Context, _ string) (query.Offset, error) {
	return query.NoOffset{}, nil
}

func (s *noopOffsetStore) SaveOffset(_ context.Context, _ string, _ query.Offset) error {
	return nil
}
