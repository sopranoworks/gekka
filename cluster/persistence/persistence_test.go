/*
 * persistence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// ── CounterActor ──────────────────────────────────────────────────────────────

// IncrementCmd is the command sent to CounterActor to increment its value.
type IncrementCmd struct{ Amount int }

// IncrementedEvent is persisted when the counter is incremented.
type IncrementedEvent struct{ Amount int }

// CounterActor persists IncrementedEvent to durably track a running total.
type CounterActor struct {
	Value int
}

func (a *CounterActor) PersistenceId() string { return "counter-1" }

func (a *CounterActor) OnCommand(ctx PersistContext, cmd any) {
	switch c := cmd.(type) {
	case IncrementCmd:
		ctx.Persist(IncrementedEvent{Amount: c.Amount}, nil)
	}
}

func (a *CounterActor) OnEvent(event any) {
	if e, ok := event.(IncrementedEvent); ok {
		a.Value += e.Amount
	}
}

// ── test decoder ─────────────────────────────────────────────────────────────

// counterDecoder maps the bare type name "IncrementedEvent" back to the
// concrete Go struct, enabling round-trip through the FileJournal.
func counterDecoder(typeName string, raw json.RawMessage) (any, error) {
	switch typeName {
	case "IncrementedEvent":
		var e IncrementedEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		return e, nil
	default:
		return nil, nil // unknown events are skipped
	}
}

// ── mock actor context ────────────────────────────────────────────────────────

type testActorContext struct {
	actor.ActorContext
	stopped bool
}

func (m *testActorContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	a := props.New()
	ref := &testActorRef{path: "/user/" + name, actor: a}
	// Wire the actor: inject system and self-ref, then run PreStart.
	if ba, ok := a.(interface {
		SetSystem(actor.ActorContext)
		SetSelf(actor.Ref)
	}); ok {
		ba.SetSystem(m)
		ba.SetSelf(ref)
	}
	if ps, ok := a.(interface{ PreStart() }); ok {
		ps.PreStart()
	}
	return ref, nil
}

func (m *testActorContext) Stop(_ actor.Ref) { m.stopped = true }
func (m *testActorContext) Watch(_, _ actor.Ref) {}

type testActorRef struct {
	path  string
	actor actor.Actor
}

func (r *testActorRef) Path() string { return r.path }
func (r *testActorRef) Tell(msg any, sender ...actor.Ref) {
	var s actor.Ref
	if len(sender) > 0 {
		s = sender[0]
	}
	if ba, ok := r.actor.(interface{ SetSender(actor.Ref) }); ok {
		ba.SetSender(s)
	}
	r.actor.Receive(msg)
}

// ── TestCounterActor_PersistAndRecover ────────────────────────────────────────

// TestCounterActor_PersistAndRecover is the primary Phase 5 integration test:
//
//  1. Create a CounterActor backed by a FileJournal.
//  2. Send 3 IncrementCmd{Amount:1} commands — counter reaches 3.
//  3. "Crash" by discarding the in-memory actor instance.
//  4. Create a brand-new CounterActor with the same PersistenceId and journal.
//  5. After PreStart recovery, verify the counter is exactly 3.
func TestCounterActor_PersistAndRecover(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "counter.jsonl")
	journal := NewFileJournal(journalPath, counterDecoder)

	// ── Phase 1: send 3 increments ────────────────────────────────────────
	counter1 := &CounterActor{}
	w1 := NewPersistentActorWrapper(counter1, journal).(*PersistentActorWrapper)

	sys := &testActorContext{}
	w1.SetSystem(sys)
	w1.SetSelf(&testActorRef{path: "/user/counter-1"})
	w1.PreStart() // journal is empty — recovery is a no-op

	for range 3 {
		w1.Receive(IncrementCmd{Amount: 1})
	}

	if counter1.Value != 3 {
		t.Fatalf("phase 1: expected counter=3 after 3 increments, got %d", counter1.Value)
	}

	// ── Phase 2: new instance — recovery from FileJournal ─────────────────
	counter2 := &CounterActor{} // zero state
	w2 := NewPersistentActorWrapper(counter2, journal).(*PersistentActorWrapper)

	w2.SetSystem(sys)
	w2.SetSelf(&testActorRef{path: "/user/counter-1"})
	w2.PreStart() // replays 3 IncrementedEvents → counter2.Value becomes 3

	if counter2.Value != 3 {
		t.Fatalf("phase 2: expected recovered counter=3, got %d", counter2.Value)
	}
	if w2.seqNr != 3 {
		t.Errorf("phase 2: expected seqNr=3 after recovery, got %d", w2.seqNr)
	}
}

// TestCounterActor_StashDuringRecovery verifies that messages received while
// the actor is in Recovering state are buffered and processed after replay.
func TestCounterActor_StashDuringRecovery(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "counter-stash.jsonl")
	journal := NewFileJournal(journalPath, counterDecoder)

	// Pre-populate the journal with 2 events.
	if err := journal.Write("counter-1", 1, IncrementedEvent{Amount: 10}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Write("counter-1", 2, IncrementedEvent{Amount: 5}); err != nil {
		t.Fatal(err)
	}

	counter := &CounterActor{}
	w := NewPersistentActorWrapper(counter, journal).(*PersistentActorWrapper)

	sys := &testActorContext{}
	w.SetSystem(sys)
	w.SetSelf(&testActorRef{path: "/user/counter-1"})

	// Manually stash a message before recovery finishes (simulates concurrent delivery).
	w.stash = append(w.stash, stashedEnvelope{msg: IncrementCmd{Amount: 1}, sender: nil})

	// PreStart: replays 2 events (counter=15), then drains the stash (counter=16).
	// We call recover() directly and then drain stash manually to test in isolation.
	w.recovering = true
	if err := w.recover(); err != nil {
		t.Fatal(err)
	}
	// After recover(), value is 15 from replayed events.
	if counter.Value != 15 {
		t.Fatalf("expected 15 after replay, got %d", counter.Value)
	}
	// Drain the stash.
	w.recovering = false
	stash := w.stash
	w.stash = nil
	for _, env := range stash {
		w.dispatchCommand(env.msg, env.sender)
	}
	// Stashed IncrementCmd{1} is now processed → counter = 16.
	if counter.Value != 16 {
		t.Fatalf("expected 16 after stash drain, got %d", counter.Value)
	}
}

// TestFileJournal_ReadEmpty verifies that reading a non-existent file yields
// an empty stream rather than an error.
func TestFileJournal_ReadEmpty(t *testing.T) {
	j := NewFileJournal(filepath.Join(t.TempDir(), "missing.jsonl"), nil)
	stream, err := j.Read("any-id", 1)
	if err != nil {
		t.Fatalf("expected no error on missing file, got %v", err)
	}
	_, ok := stream.Next()
	if ok {
		t.Error("expected empty stream from missing file")
	}
}

// TestFileJournal_WriteRead verifies basic write/read round-trip via the JSONL
// file, including filtering by persistenceId and fromSeqNr.
func TestFileJournal_WriteRead(t *testing.T) {
	j := NewFileJournal(filepath.Join(t.TempDir(), "events.jsonl"), counterDecoder)

	// Write events for two different persistence IDs.
	if err := j.Write("a", 1, IncrementedEvent{Amount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.Write("b", 1, IncrementedEvent{Amount: 99}); err != nil {
		t.Fatal(err)
	}
	if err := j.Write("a", 2, IncrementedEvent{Amount: 2}); err != nil {
		t.Fatal(err)
	}

	// Read only persistence ID "a" from seqNr 1.
	stream, err := j.Read("a", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var events []Event
	for {
		ev, ok := stream.Next()
		if !ok {
			break
		}
		events = append(events, ev)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events for 'a', got %d", len(events))
	}
	if events[0].SeqNr != 1 || events[1].SeqNr != 2 {
		t.Errorf("unexpected seqNrs: %v", []int64{events[0].SeqNr, events[1].SeqNr})
	}

	// Confirm fromSeqNr filtering: start at 2.
	stream2, _ := j.Read("a", 2)
	defer stream2.Close()
	ev, ok := stream2.Next()
	if !ok {
		t.Fatal("expected 1 event from seqNr=2")
	}
	if ev.SeqNr != 2 {
		t.Errorf("expected seqNr=2, got %d", ev.SeqNr)
	}
	_, ok = stream2.Next()
	if ok {
		t.Error("expected stream exhausted after seqNr=2")
	}
}
