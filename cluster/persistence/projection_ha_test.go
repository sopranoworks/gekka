/*
 * projection_ha_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"path/filepath"
	"testing"
	"time"
)

// newProjectionActor is a test helper that creates a ProjectionActor with a
// long timer interval (so it never fires during a test) and wires it against
// the shared testActorContext.  The actor is driven manually by delivering
// projectionTickMsg{} or StopProjection{} directly.
func newTestProjectionActor(t *testing.T, proj *Projection, sys *testActorContext, name string) *ProjectionActor {
	t.Helper()
	pa := NewProjectionActor(proj, 24*time.Hour) // timer never fires in tests
	pa.SetSystem(sys)
	pa.SetSelf(&testActorRef{path: "/user/" + name, actor: pa})
	pa.PreStart() // arms the (very far-future) timer, no-op for test purposes
	return pa
}

// ── TestProjectionHA_FailoverResumesFromOffset ────────────────────────────────

// TestProjectionHA_FailoverResumesFromOffset simulates a crash-failover:
//
//  1. Ten tagged events are written to a shared FileJournal.
//  2. Node A's ProjectionActor processes all ten (via a manual tick).
//     The shared InMemoryOffsetStore records offset 10.
//  3. Node A "crashes" — it is abandoned without a StopProjection flush.
//  4. Five additional events are written (Node A never sees them).
//  5. Node B's ProjectionActor starts fresh with the SAME OffsetStore.
//     It resumes from offset 10 and processes exactly five events.
//
// This test verifies that the OffsetStore is the sole source of truth for
// resumption; no state from Node A leaks into Node B.
func TestProjectionHA_FailoverResumesFromOffset(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "ha.jsonl"), counterDecoder)
	sys := &testActorContext{}

	// ── Write 10 events ───────────────────────────────────────────────────
	writer := &CounterActor{}
	w := NewPersistentActorWrapper(writer, journal).(*PersistentActorWrapper)
	w.Tagger = func(event any) []string { return []string{"ha-metric"} }
	w.SetSystem(sys)
	w.SetSelf(&testActorRef{path: "/user/counter-1", actor: w})
	w.PreStart()

	for i := range 10 {
		w.Receive(IncrementCmd{Amount: i + 1})
	}

	// ── Shared OffsetStore (represents a shared DB in a real cluster) ─────
	sharedStore := NewInMemoryOffsetStore()
	rj := NewFileReadJournal(journal)

	// ── Node A: process all 10 events ─────────────────────────────────────
	var nodeAEvents []TaggedEvent
	projA := NewProjection("ha-proj", "ha-metric", rj,
		func(ev TaggedEvent) error {
			nodeAEvents = append(nodeAEvents, ev)
			return nil
		}, sharedStore)

	paA := newTestProjectionActor(t, projA, sys, "projA")
	paA.Receive(projectionTickMsg{}) // manual tick

	if got := len(nodeAEvents); got != 10 {
		t.Fatalf("Node A: expected 10 events processed, got %d", got)
	}
	offsetAfterA, _ := sharedStore.Load("ha-proj")
	if offsetAfterA != 10 {
		t.Fatalf("expected shared offset=10 after Node A, got %d", offsetAfterA)
	}

	// ── Simulate crash: Node A abandoned (no StopProjection flush) ────────
	// 5 new events arrive while Node A is down.
	for i := range 5 {
		w.Receive(IncrementCmd{Amount: 100 + i})
	}

	// ── Node B: start fresh with the SAME shared OffsetStore ──────────────
	var nodeBEvents []TaggedEvent
	projB := NewProjection("ha-proj", "ha-metric", rj,
		func(ev TaggedEvent) error {
			nodeBEvents = append(nodeBEvents, ev)
			return nil
		}, sharedStore)

	paB := newTestProjectionActor(t, projB, sys, "projB")
	paB.Receive(projectionTickMsg{}) // manual tick

	// Node B must process exactly the 5 events Node A never saw.
	if got := len(nodeBEvents); got != 5 {
		t.Fatalf("Node B: expected 5 events (resume), got %d", got)
	}

	// Verify the offsets are the 5 new lines (indices 10–14).
	for i, ev := range nodeBEvents {
		wantOffset := Offset(10 + i)
		if ev.Offset != wantOffset {
			t.Errorf("Node B event[%d]: expected offset %d, got %d", i, wantOffset, ev.Offset)
		}
	}

	// Final shared offset must be 15.
	finalOffset, _ := sharedStore.Load("ha-proj")
	if finalOffset != 15 {
		t.Fatalf("expected final shared offset=15, got %d", finalOffset)
	}
}

// ── TestProjectionHA_GracefulHandoff ─────────────────────────────────────────

// TestProjectionHA_GracefulHandoff simulates a graceful handoff where Node A
// is given a StopProjection message before leadership transfers to Node B.
//
//  1. Node A processes 10 events (tick).
//  2. Five more events arrive — Node A has not ticked again yet.
//  3. StopProjection is sent to Node A.  The final RunOnce flush processes
//     those 5 events and advances the shared offset to 15.
//  4. Node B starts and ticks.  Because the offset is already at 15 there
//     are no new events; Node B processes zero.
//  5. Three additional events arrive and Node B ticks again, processing
//     exactly 3 — demonstrating correct resumption after a clean handoff.
func TestProjectionHA_GracefulHandoff(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "graceful.jsonl"), counterDecoder)
	sys := &testActorContext{}

	writer := &CounterActor{}
	w := NewPersistentActorWrapper(writer, journal).(*PersistentActorWrapper)
	w.Tagger = func(event any) []string { return []string{"graceful"} }
	w.SetSystem(sys)
	w.SetSelf(&testActorRef{path: "/user/counter-1", actor: w})
	w.PreStart()

	// Write 10 events.
	for i := range 10 {
		w.Receive(IncrementCmd{Amount: i + 1})
	}

	sharedStore := NewInMemoryOffsetStore()
	rj := NewFileReadJournal(journal)

	// ── Node A: tick processes the first 10 ───────────────────────────────
	projA := NewProjection("graceful-proj", "graceful", rj,
		func(_ TaggedEvent) error { return nil }, sharedStore)
	paA := newTestProjectionActor(t, projA, sys, "projA-graceful")
	paA.Receive(projectionTickMsg{})

	offsetMidA, _ := sharedStore.Load("graceful-proj")
	if offsetMidA != 10 {
		t.Fatalf("mid-A: expected offset=10, got %d", offsetMidA)
	}

	// 5 more events arrive before Node A's next tick.
	for i := range 5 {
		w.Receive(IncrementCmd{Amount: 100 + i})
	}

	// ── StopProjection: final flush captures the 5 pending events ─────────
	paA.Receive(StopProjection{})

	offsetAfterStop, _ := sharedStore.Load("graceful-proj")
	if offsetAfterStop != 15 {
		t.Fatalf("after StopProjection: expected offset=15, got %d", offsetAfterStop)
	}
	// Confirm sys.Stop was called on the actor.
	if !sys.stopped {
		t.Error("expected ProjectionActor to stop itself after StopProjection")
	}

	// ── Node B: inherits offset=15 — nothing pending ──────────────────────
	var nodeBEvents []TaggedEvent
	projB := NewProjection("graceful-proj", "graceful", rj,
		func(ev TaggedEvent) error {
			nodeBEvents = append(nodeBEvents, ev)
			return nil
		}, sharedStore)
	sysB := &testActorContext{} // fresh context for Node B
	paB := newTestProjectionActor(t, projB, sysB, "projB-graceful")
	paB.Receive(projectionTickMsg{})

	if got := len(nodeBEvents); got != 0 {
		t.Fatalf("Node B first tick: expected 0 events (all consumed), got %d", got)
	}

	// 3 new events arrive on Node B's watch.
	for i := range 3 {
		w.Receive(IncrementCmd{Amount: 200 + i})
	}
	paB.Receive(projectionTickMsg{})

	if got := len(nodeBEvents); got != 3 {
		t.Fatalf("Node B second tick: expected 3 events, got %d", got)
	}
	// Verify offsets are contiguous from 15.
	for i, ev := range nodeBEvents {
		wantOffset := Offset(15 + i)
		if ev.Offset != wantOffset {
			t.Errorf("Node B event[%d]: expected offset %d, got %d", i, wantOffset, ev.Offset)
		}
	}
}

// ── TestProjectionActor_TickLoop ──────────────────────────────────────────────

// TestProjectionActor_TickLoop verifies the basic polling behaviour: each
// projectionTickMsg invokes RunOnce and events written between ticks are
// delivered on the next tick.
func TestProjectionActor_TickLoop(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "loop.jsonl"), counterDecoder)
	sys := &testActorContext{}

	writer := &CounterActor{}
	w := NewPersistentActorWrapper(writer, journal).(*PersistentActorWrapper)
	w.Tagger = func(event any) []string { return []string{"loop"} }
	w.SetSystem(sys)
	w.SetSelf(&testActorRef{path: "/user/counter-1", actor: w})
	w.PreStart()

	store := NewInMemoryOffsetStore()
	rj := NewFileReadJournal(journal)

	var received []TaggedEvent
	proj := NewProjection("loop-proj", "loop", rj,
		func(ev TaggedEvent) error {
			received = append(received, ev)
			return nil
		}, store)
	pa := newTestProjectionActor(t, proj, sys, "loop-proj")

	// Tick 1: no events yet → nothing delivered.
	pa.Receive(projectionTickMsg{})
	if got := len(received); got != 0 {
		t.Fatalf("tick 1: expected 0, got %d", got)
	}

	// Write 3 events between ticks.
	w.Receive(IncrementCmd{Amount: 1})
	w.Receive(IncrementCmd{Amount: 2})
	w.Receive(IncrementCmd{Amount: 3})

	// Tick 2: processes the 3 new events.
	pa.Receive(projectionTickMsg{})
	if got := len(received); got != 3 {
		t.Fatalf("tick 2: expected 3, got %d", got)
	}

	// Tick 3: no new events → idempotent.
	pa.Receive(projectionTickMsg{})
	if got := len(received); got != 3 {
		t.Fatalf("tick 3: expected still 3, got %d", got)
	}

	// Write 2 more events.
	w.Receive(IncrementCmd{Amount: 4})
	w.Receive(IncrementCmd{Amount: 5})

	// Tick 4: only the 2 new events delivered.
	pa.Receive(projectionTickMsg{})
	if got := len(received); got != 5 {
		t.Fatalf("tick 4: expected 5, got %d", got)
	}
}
