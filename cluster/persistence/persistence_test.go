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

// CounterState is the snapshot payload for CounterActor.
type CounterState struct{ Value int }

// CounterActor persists IncrementedEvent to durably track a running total.
type CounterActor struct {
	Value int
}

func (a *CounterActor) PersistenceId() string { return "counter-1" }

func (a *CounterActor) OnCommand(ctx PersistContext, cmd any) {
	switch c := cmd.(type) {
	case IncrementCmd:
		ctx.Persist(IncrementedEvent(c), nil)
	}
}

func (a *CounterActor) OnEvent(event any) {
	if e, ok := event.(IncrementedEvent); ok {
		a.Value += e.Amount
	}
}

// OnSnapshot restores state from a CounterState snapshot.
func (a *CounterActor) OnSnapshot(snapshot any) {
	if s, ok := snapshot.(CounterState); ok {
		a.Value = s.Value
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

// counterSnapshotDecoder reconstructs a CounterState snapshot.
func counterSnapshotDecoder(typeName string, raw json.RawMessage) (any, error) {
	switch typeName {
	case "CounterState":
		var s CounterState
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, nil
	}
}

// ── countingJournal ───────────────────────────────────────────────────────────

// countingJournal wraps a Journal and counts events returned by Read (i.e.
// events replayed during recovery).  Used in Phase 6 tests to verify that only
// post-snapshot events are replayed.
type countingJournal struct {
	Journal
	ReplayCount int
}

func (j *countingJournal) Read(pid string, from int64) (EventStream, error) {
	stream, err := j.Journal.Read(pid, from)
	if err != nil {
		return nil, err
	}
	return &countingStream{EventStream: stream, counter: &j.ReplayCount}, nil
}

type countingStream struct {
	EventStream
	counter *int
}

func (s *countingStream) Next() (Event, bool) {
	ev, ok := s.EventStream.Next()
	if ok {
		(*s.counter)++
	}
	return ev, ok
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

func (m *testActorContext) Stop(_ actor.Ref)     { m.stopped = true }
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
	if err := journal.Write("counter-1", 1, IncrementedEvent{Amount: 10}, nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.Write("counter-1", 2, IncrementedEvent{Amount: 5}, nil); err != nil {
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
	if err := j.Write("a", 1, IncrementedEvent{Amount: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := j.Write("b", 1, IncrementedEvent{Amount: 99}, nil); err != nil {
		t.Fatal(err)
	}
	if err := j.Write("a", 2, IncrementedEvent{Amount: 2}, nil); err != nil {
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

// ── Phase 6: snapshot tests ───────────────────────────────────────────────────

// TestSnapshot_RecoveryUsesSnapshot is the primary Phase 6 integration test:
//
//  1. Persist 50 events (counter reaches 50).
//  2. Save a snapshot at SequenceNr 50.
//  3. Persist 5 more events (counter reaches 55, seqNr=55).
//  4. Restart the actor with the same journal + snapshot store.
//  5. Verify:
//     - OnSnapshot is called exactly once (snapshot is loaded).
//     - Only 5 events are replayed from the journal (seqNr 51–55).
//     - Recovered counter value is 55.
func TestSnapshot_RecoveryUsesSnapshot(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "counter.jsonl"), counterDecoder)
	snapStore, err := NewFileSnapshotStore(filepath.Join(dir, "snaps"), counterSnapshotDecoder)
	if err != nil {
		t.Fatalf("NewFileSnapshotStore: %v", err)
	}

	sys := &testActorContext{}

	// ── Phase 1: 50 events + snapshot + 5 more ────────────────────────────
	actor1 := &CounterActor{}
	w1 := NewPersistentActorWrapper(actor1, journal, snapStore).(*PersistentActorWrapper)
	w1.SetSystem(sys)
	w1.SetSelf(&testActorRef{path: "/user/counter-1"})
	w1.PreStart()

	for range 50 {
		w1.Receive(IncrementCmd{Amount: 1})
	}
	if actor1.Value != 50 {
		t.Fatalf("expected counter=50 before snapshot, got %d", actor1.Value)
	}
	if w1.seqNr != 50 {
		t.Fatalf("expected seqNr=50 before snapshot, got %d", w1.seqNr)
	}

	// Save snapshot at seqNr 50 via PersistContext.
	ctx1 := &persistContextImpl{wrapper: w1, sender: nil}
	ctx1.SaveSnapshot(CounterState{Value: actor1.Value})

	// 5 more events after the snapshot.
	for range 5 {
		w1.Receive(IncrementCmd{Amount: 1})
	}
	if actor1.Value != 55 {
		t.Fatalf("expected counter=55 after 5 post-snapshot events, got %d", actor1.Value)
	}

	// ── Phase 2: restart — verify snapshot + 5 event replays ─────────────
	actor2 := &CounterActor{}
	cj := &countingJournal{Journal: journal}
	w2 := NewPersistentActorWrapper(actor2, cj, snapStore).(*PersistentActorWrapper)
	w2.SetSystem(sys)
	w2.SetSelf(&testActorRef{path: "/user/counter-1"})
	w2.PreStart()

	// Counter must be fully restored.
	if actor2.Value != 55 {
		t.Errorf("expected recovered counter=55, got %d", actor2.Value)
	}
	// seqNr must be at the end of the journal.
	if w2.seqNr != 55 {
		t.Errorf("expected seqNr=55 after recovery, got %d", w2.seqNr)
	}
	// Only 5 post-snapshot events should have been replayed (not all 55).
	if cj.ReplayCount != 5 {
		t.Errorf("expected 5 journal events replayed (post-snapshot only), got %d", cj.ReplayCount)
	}
}

// TestSnapshot_NoSnapshotFallsBackToFullReplay verifies that when no snapshot
// exists the actor falls back to replaying the full journal.
func TestSnapshot_NoSnapshotFallsBackToFullReplay(t *testing.T) {
	dir := t.TempDir()
	journal := NewFileJournal(filepath.Join(dir, "counter.jsonl"), counterDecoder)
	snapStore, _ := NewFileSnapshotStore(filepath.Join(dir, "snaps"), counterSnapshotDecoder)

	sys := &testActorContext{}

	// Phase 1: write 10 events, NO snapshot.
	actor1 := &CounterActor{}
	w1 := NewPersistentActorWrapper(actor1, journal, snapStore).(*PersistentActorWrapper)
	w1.SetSystem(sys)
	w1.SetSelf(&testActorRef{path: "/user/counter-1"})
	w1.PreStart()
	for range 10 {
		w1.Receive(IncrementCmd{Amount: 1})
	}

	// Phase 2: restart — all 10 events must be replayed.
	actor2 := &CounterActor{}
	cj := &countingJournal{Journal: journal}
	w2 := NewPersistentActorWrapper(actor2, cj, snapStore).(*PersistentActorWrapper)
	w2.SetSystem(sys)
	w2.SetSelf(&testActorRef{path: "/user/counter-1"})
	w2.PreStart()

	if actor2.Value != 10 {
		t.Errorf("expected counter=10 after full replay, got %d", actor2.Value)
	}
	if cj.ReplayCount != 10 {
		t.Errorf("expected 10 events replayed (full journal), got %d", cj.ReplayCount)
	}
}

// TestFileSnapshotStore_SaveLoad verifies the FileSnapshotStore round-trip.
func TestFileSnapshotStore_SaveLoad(t *testing.T) {
	store, err := NewFileSnapshotStore(t.TempDir(), counterSnapshotDecoder)
	if err != nil {
		t.Fatal(err)
	}

	meta := SnapshotMetadata{PersistenceId: "snap-test", SequenceNr: 42}
	if err := store.Save("snap-test", meta, CounterState{Value: 42}); err != nil {
		t.Fatal(err)
	}

	snap, err := store.Load("snap-test")
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if snap.Metadata.SequenceNr != 42 {
		t.Errorf("expected seqNr=42, got %d", snap.Metadata.SequenceNr)
	}
	state, ok := snap.Snapshot.(CounterState)
	if !ok {
		t.Fatalf("expected CounterState, got %T", snap.Snapshot)
	}
	if state.Value != 42 {
		t.Errorf("expected Value=42, got %d", state.Value)
	}
}

// TestFileSnapshotStore_LoadMissing verifies that loading from an empty store
// returns (nil, nil) rather than an error.
func TestFileSnapshotStore_LoadMissing(t *testing.T) {
	store, _ := NewFileSnapshotStore(t.TempDir(), nil)
	snap, err := store.Load("no-such-id")
	if err != nil {
		t.Fatalf("expected no error for missing snapshot, got %v", err)
	}
	if snap != nil {
		t.Errorf("expected nil snapshot for missing ID")
	}
}

// TestFileSnapshotStore_OverwriteKeepsLatest verifies that saving twice keeps
// only the most recent snapshot.
func TestFileSnapshotStore_OverwriteKeepsLatest(t *testing.T) {
	store, _ := NewFileSnapshotStore(t.TempDir(), counterSnapshotDecoder)

	_ = store.Save("pid", SnapshotMetadata{SequenceNr: 10}, CounterState{Value: 10})
	_ = store.Save("pid", SnapshotMetadata{SequenceNr: 20}, CounterState{Value: 20})

	snap, _ := store.Load("pid")
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if snap.Metadata.SequenceNr != 20 {
		t.Errorf("expected latest seqNr=20, got %d", snap.Metadata.SequenceNr)
	}
	if snap.Snapshot.(CounterState).Value != 20 {
		t.Errorf("expected latest Value=20, got %d", snap.Snapshot.(CounterState).Value)
	}
}
