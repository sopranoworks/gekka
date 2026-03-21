/*
 * projection_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"path/filepath"
	"slices"
	"testing"
)

// TestProjection_TaggedEventsFromTwoActors is the primary Phase 8 integration
// test.  It exercises the full CQRS write→tag→project pipeline:
//
//  1. Two CounterActors share a single FileJournal, each with a Tagger that
//     marks every IncrementedEvent with the tag "metric".
//  2. Both actors receive IncrementCmd commands; events are persisted with
//     the "metric" tag.
//  3. A Projection backed by FileReadJournal subscribes to the "metric" tag.
//  4. RunOnce processes all events and we assert:
//     a. Both actors' events are received in global (append) order.
//     b. The correct payloads are delivered to the handler.
//     c. The OffsetStore advances after each event (at-least-once guarantee).
func TestProjection_TaggedEventsFromTwoActors(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "metrics.jsonl")
	journal := NewFileJournal(journalPath, counterDecoder)
	sys := &testActorContext{}

	// ── Set up actor A ("counter-A") ──────────────────────────────────────
	actorA := &CounterActor{}
	wA := NewPersistentActorWrapper(actorA, journal).(*PersistentActorWrapper)
	wA.Tagger = func(event any) []string {
		if _, ok := event.(IncrementedEvent); ok {
			return []string{"metric"}
		}
		return nil
	}
	wA.SetSystem(sys)
	wA.SetSelf(&testActorRef{path: "/user/counter-A"})
	wA.PreStart()

	// ── Set up actor B ("counter-B") ──────────────────────────────────────
	// counter-B uses a different PersistenceId by wrapping in an adapter.
	actorB := &namedCounterActor{name: "counter-B"}
	wB := NewPersistentActorWrapper(actorB, journal).(*PersistentActorWrapper)
	wB.Tagger = func(event any) []string {
		if _, ok := event.(IncrementedEvent); ok {
			return []string{"metric"}
		}
		return nil
	}
	wB.SetSystem(sys)
	wB.SetSelf(&testActorRef{path: "/user/counter-B"})
	wB.PreStart()

	// ── Write events: A×2, B×3 ────────────────────────────────────────────
	wA.Receive(IncrementCmd{Amount: 10})
	wA.Receive(IncrementCmd{Amount: 20})
	wB.Receive(IncrementCmd{Amount: 1})
	wB.Receive(IncrementCmd{Amount: 2})
	wB.Receive(IncrementCmd{Amount: 3})

	// Confirm in-memory state.
	if actorA.Value != 30 {
		t.Fatalf("actorA: expected 30, got %d", actorA.Value)
	}
	if actorB.value != 6 {
		t.Fatalf("actorB: expected 6, got %d", actorB.value)
	}

	// ── Run projection ────────────────────────────────────────────────────
	rj := NewFileReadJournal(journal)
	offsetStore := NewInMemoryOffsetStore()

	var received []TaggedEvent
	proj := NewProjection("metrics-proj", "metric", rj, func(ev TaggedEvent) error {
		received = append(received, ev)
		return nil
	}, offsetStore)

	if err := proj.RunOnce(); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// ── Assert event count ────────────────────────────────────────────────
	if got := len(received); got != 5 {
		t.Fatalf("expected 5 tagged events, got %d", got)
	}

	// ── Assert global (append) order ──────────────────────────────────────
	// Events must arrive in write order: A×2 then B×3.
	wantPids := []string{
		"counter-1", "counter-1", // wA uses CounterActor.PersistenceId = "counter-1"
		"counter-B", "counter-B", "counter-B",
	}
	for i, ev := range received {
		if ev.PersistenceId != wantPids[i] {
			t.Errorf("event[%d]: expected pid %q, got %q", i, wantPids[i], ev.PersistenceId)
		}
	}

	// ── Assert payloads ───────────────────────────────────────────────────
	wantAmounts := []int{10, 20, 1, 2, 3}
	for i, ev := range received {
		inc, ok := ev.Payload.(IncrementedEvent)
		if !ok {
			t.Errorf("event[%d]: payload is %T, want IncrementedEvent", i, ev.Payload)
			continue
		}
		if inc.Amount != wantAmounts[i] {
			t.Errorf("event[%d]: expected Amount=%d, got %d", i, wantAmounts[i], inc.Amount)
		}
	}

	// ── Assert tag present ────────────────────────────────────────────────
	for i, ev := range received {
		if !slices.Contains(ev.Tags, "metric") {
			t.Errorf("event[%d]: missing tag %q, tags=%v", i, "metric", ev.Tags)
		}
	}

	// ── Assert offset store advanced after each event ─────────────────────
	// After 5 events the stored offset must equal 5 (next unread line).
	finalOffset, err := offsetStore.Load("metrics-proj")
	if err != nil {
		t.Fatalf("offsetStore.Load: %v", err)
	}
	if finalOffset != 5 {
		t.Errorf("expected finalOffset=5, got %d", finalOffset)
	}
}

// TestProjection_ResumeFromOffset verifies that a Projection resumes exactly
// where it left off: events already processed are not re-delivered.
func TestProjection_ResumeFromOffset(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "resume.jsonl"), counterDecoder)
	sys := &testActorContext{}

	actor1 := &CounterActor{}
	w1 := NewPersistentActorWrapper(actor1, journal).(*PersistentActorWrapper)
	w1.Tagger = func(event any) []string { return []string{"resumable"} }
	w1.SetSystem(sys)
	w1.SetSelf(&testActorRef{path: "/user/counter-1"})
	w1.PreStart()

	// Write 4 events.
	for range 4 {
		w1.Receive(IncrementCmd{Amount: 1})
	}

	rj := NewFileReadJournal(journal)
	offsetStore := NewInMemoryOffsetStore()

	// ── First run: process only first 2 events by stopping early ─────────
	count1 := 0
	proj1 := NewProjection("resume-proj", "resumable", rj, func(ev TaggedEvent) error {
		count1++
		if count1 == 2 {
			// Save offset manually after 2 to simulate a mid-batch checkpoint.
			_ = offsetStore.Save("resume-proj", ev.Offset+1)
		}
		return nil
	}, offsetStore)

	if err := proj1.RunOnce(); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	// RunOnce itself will save the offset for all 4, but the test verifies the
	// resumption path below, so we manually reset to 2.
	_ = offsetStore.Save("resume-proj", 2)

	// ── Second run: should receive exactly the last 2 events ─────────────
	var received2 []TaggedEvent
	proj2 := NewProjection("resume-proj", "resumable", rj, func(ev TaggedEvent) error {
		received2 = append(received2, ev)
		return nil
	}, offsetStore)

	if err := proj2.RunOnce(); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}

	if got := len(received2); got != 2 {
		t.Errorf("expected 2 events on resume, got %d", got)
	}
	// Offsets 2 and 3 (0-based line 2 and 3).
	if len(received2) == 2 {
		if received2[0].Offset != 2 {
			t.Errorf("expected first resumed event at offset 2, got %d", received2[0].Offset)
		}
		if received2[1].Offset != 3 {
			t.Errorf("expected second resumed event at offset 3, got %d", received2[1].Offset)
		}
	}
}

// TestProjection_UntaggedEventsIgnored verifies that events without the target
// tag are not delivered to the handler.
func TestProjection_UntaggedEventsIgnored(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "mixed.jsonl"), counterDecoder)
	sys := &testActorContext{}

	// Actor A: tagged "important".
	actorA := &CounterActor{}
	wA := NewPersistentActorWrapper(actorA, journal).(*PersistentActorWrapper)
	wA.Tagger = func(event any) []string { return []string{"important"} }
	wA.SetSystem(sys)
	wA.SetSelf(&testActorRef{path: "/user/counter-A"})
	wA.PreStart()
	wA.Receive(IncrementCmd{Amount: 1})

	// Actor B: no tagger — events have no tags.
	actorB := &namedCounterActor{name: "counter-B"}
	wB := NewPersistentActorWrapper(actorB, journal).(*PersistentActorWrapper)
	wB.SetSystem(sys)
	wB.SetSelf(&testActorRef{path: "/user/counter-B"})
	wB.PreStart()
	wB.Receive(IncrementCmd{Amount: 99})

	// Actor A again.
	wA.Receive(IncrementCmd{Amount: 2})

	rj := NewFileReadJournal(journal)
	offsetStore := NewInMemoryOffsetStore()

	var received []TaggedEvent
	proj := NewProjection("important-proj", "important", rj, func(ev TaggedEvent) error {
		received = append(received, ev)
		return nil
	}, offsetStore)

	if err := proj.RunOnce(); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Only A's 2 events (tagged "important") must arrive; B's event is skipped.
	if got := len(received); got != 2 {
		t.Errorf("expected 2 events (only tagged ones), got %d", got)
	}
	for _, ev := range received {
		if ev.PersistenceId != "counter-1" {
			t.Errorf("unexpected pid %q (expected counter-1)", ev.PersistenceId)
		}
	}
}

// ── namedCounterActor ─────────────────────────────────────────────────────────

// namedCounterActor wraps CounterActor with a configurable PersistenceId so
// tests can run two distinct actors against the same journal file.
type namedCounterActor struct {
	name  string
	value int
}

func (a *namedCounterActor) PersistenceId() string { return a.name }

func (a *namedCounterActor) OnCommand(ctx PersistContext, cmd any) {
	if c, ok := cmd.(IncrementCmd); ok {
		ctx.Persist(IncrementedEvent{Amount: c.Amount}, nil)
	}
}

func (a *namedCounterActor) OnEvent(event any) {
	if e, ok := event.(IncrementedEvent); ok {
		a.value += e.Amount
	}
}

func (a *namedCounterActor) OnSnapshot(_ any) {}
