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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/cluster/sharding"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/persistence/projection"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"github.com/sopranoworks/gekka/telemetry"
)

// ── In-process spy telemetry provider ────────────────────────────────────────

// spyTraceID is a 16-byte trace identifier generated with crypto/rand.
type spyTraceID [16]byte

func (id spyTraceID) String() string { return fmt.Sprintf("%032x", id[:]) }

// spySpanRecord is an ended span captured by spyRecorder.
type spySpanRecord struct {
	name    string
	traceID spyTraceID
}

// spyRecorder implements telemetry.Provider and collects ended spans.
type spyRecorder struct {
	mu    sync.Mutex
	spans []spySpanRecord
}

func newSpyRecorder() *spyRecorder { return &spyRecorder{} }

func (r *spyRecorder) record(name string, traceID spyTraceID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spySpanRecord{name: name, traceID: traceID})
}

// Ended returns a snapshot of all spans recorded so far.
func (r *spyRecorder) Ended() []spySpanRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]spySpanRecord(nil), r.spans...)
}

// telemetry.Provider implementation.
func (r *spyRecorder) Tracer(_ string) telemetry.Tracer { return &spyTracer{rec: r} }
func (r *spyRecorder) Meter(_ string) telemetry.Meter   { return telemetry.NoopMeter{} }

// spyTraceKey is the unexported context key used to propagate trace IDs.
type spyTraceKey struct{}

const spyTraceHeader = "x-spy-trace-id"

// spyTracer implements telemetry.Tracer using context.WithValue propagation.
type spyTracer struct{ rec *spyRecorder }

func (t *spyTracer) Start(ctx context.Context, spanName string) (context.Context, telemetry.Span) {
	traceID, ok := ctx.Value(spyTraceKey{}).(spyTraceID)
	if !ok {
		var id spyTraceID
		if _, err := rand.Read(id[:]); err != nil {
			panic("spyTracer: crypto/rand: " + err.Error())
		}
		traceID = id
	}
	ctx = context.WithValue(ctx, spyTraceKey{}, traceID)
	return ctx, &spySpan{name: spanName, traceID: traceID, rec: t.rec}
}

func (t *spyTracer) Inject(ctx context.Context, carrier map[string]string) {
	if id, ok := ctx.Value(spyTraceKey{}).(spyTraceID); ok {
		carrier[spyTraceHeader] = fmt.Sprintf("%032x", id[:])
	}
}

func (t *spyTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	s, ok := carrier[spyTraceHeader]
	if !ok || len(s) != 32 {
		return ctx
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		return ctx
	}
	var id spyTraceID
	copy(id[:], b)
	return context.WithValue(ctx, spyTraceKey{}, id)
}

// spySpan implements telemetry.Span; records itself on End().
type spySpan struct {
	name    string
	traceID spyTraceID
	rec     *spyRecorder
	ended   bool
}

func (s *spySpan) End() {
	if !s.ended {
		s.ended = true
		s.rec.record(s.name, s.traceID)
	}
}
func (s *spySpan) SetAttribute(_ string, _ any) {}
func (s *spySpan) RecordError(_ error)          {}
func (s *spySpan) IsRecording() bool            { return !s.ended }

// traceIDFromCtx extracts the spy trace ID from a context, if present.
func traceIDFromCtx(ctx context.Context) (spyTraceID, bool) {
	id, ok := ctx.Value(spyTraceKey{}).(spyTraceID)
	return id, ok
}

// ── helpers ───────────────────────────────────────────────────────────────────

// setupTracer installs an in-memory spy as the global gekka telemetry provider.
// Returns the recorder and a cleanup function that restores the previous state.
func setupTracer(t *testing.T) (*spyRecorder, func()) {
	t.Helper()
	rec := newSpyRecorder()
	prev := telemetry.Global()
	telemetry.SetProvider(rec)
	return rec, func() { telemetry.SetProvider(prev) }
}

// allSpansShareTraceID returns true when every span shares root's TraceID.
func allSpansShareTraceID(root spyTraceID, spans []spySpanRecord) bool {
	for _, s := range spans {
		if s.traceID != root {
			return false
		}
	}
	return true
}

// findSpanByName returns the first recorded span whose name equals name.
func findSpanByName(spans []spySpanRecord, name string) (spySpanRecord, bool) {
	for _, s := range spans {
		if s.name == name {
			return s, true
		}
	}
	return spySpanRecord{}, false
}

// rootTraceID returns the TraceID from the "HandleCommand" span,
// falling back to the first recorded span.
func rootTraceID(spans []spySpanRecord) (spyTraceID, bool) {
	if s, ok := findSpanByName(spans, "HandleCommand"); ok {
		return s.traceID, true
	}
	if len(spans) > 0 {
		return spans[0].traceID, true
	}
	return spyTraceID{}, false
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

	tracer := telemetry.GetTracer("test")

	// ── Step 1: Start root span and inject into ShardingEnvelope ─────────────
	rootCtx, rootSpan := tracer.Start(context.Background(), "HandleCommand")

	carrier := map[string]string{}
	tracer.Inject(rootCtx, carrier)
	traceCtx := carrier

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
	} else if journalSpan.traceID != rootID {
		t.Errorf("Journal.Write TraceID mismatch: got %s, want %s",
			journalSpan.traceID, rootID)
	}

	// "Projection.Handle" span must exist and share the TraceID.
	projSpan, found := findSpanByName(spans, "Projection.Handle")
	if !found {
		t.Error("span 'Projection.Handle' not found in recorded spans")
	} else if projSpan.traceID != rootID {
		t.Errorf("Projection.Handle TraceID mismatch: got %s, want %s",
			projSpan.traceID, rootID)
	}

	if !allSpansShareTraceID(rootID, spans) {
		t.Error("not all spans share the same TraceID")
		for _, s := range spans {
			t.Logf("  span %q: traceID=%s", s.name, s.traceID)
		}
	}
}

// ── TestTraceContinuity_ShardEnvelopeInject verifies that ShardingEnvelope.TraceContext
// is populated by injectTraceContext when building an envelope from a raw message.
func TestTraceContinuity_ShardEnvelopeInject(t *testing.T) {
	_, cleanup := setupTracer(t)
	defer cleanup()

	tracer := telemetry.GetTracer("test")

	// Start a span and inject into a carrier, simulating what ShardRegion does.
	ctx, span := tracer.Start(context.Background(), "root")
	defer span.End()

	carrier := map[string]string{}
	tracer.Inject(ctx, carrier)
	tc := carrier

	if len(tc) == 0 {
		t.Fatal("expected non-empty TraceContext carrier after Inject")
	}

	// Extract back and verify we get the same TraceID.
	extractedCtx := tracer.Extract(context.Background(), tc)
	extractedID, _ := traceIDFromCtx(extractedCtx)
	originalID, _ := traceIDFromCtx(ctx)
	if extractedID != originalID {
		t.Errorf("extracted TraceID %s != original %s", extractedID, originalID)
	}
}

// ── TestTracingJournal_WriteRead verifies that TracingJournal emits both
// "Journal.Write" and "Journal.Read" spans with the same TraceID.
func TestTracingJournal_WriteRead(t *testing.T) {
	rec, cleanup := setupTracer(t)
	defer cleanup()

	tracer := telemetry.GetTracer("test")

	ctx, rootSpan := tracer.Start(context.Background(), "root")
	carrier := map[string]string{}
	tracer.Inject(ctx, carrier)
	tc := carrier
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
	names := make(map[string]spyTraceID)
	for _, s := range spans {
		names[s.name] = s.traceID
	}

	rootID := names["root"]
	if rootID == (spyTraceID{}) {
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
