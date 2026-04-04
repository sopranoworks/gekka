/*
 * snapshot_when_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"math"
	"testing"

	typed_api "github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
)

// TestSnapshotWhen_PredicateTrigger verifies that SnapshotWhen causes a snapshot
// to be saved when the predicate returns true, independent of SnapshotInterval.
func TestSnapshotWhen_PredicateTrigger(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ss := persistence.NewInMemorySnapshotStore()
	ctx := context.Background()
	const pid = "snapwhen-predicate"

	// Snapshot when the event value is a multiple of 3.
	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: pid,
		Journal:       journal,
		SnapshotStore: ss,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(c typed_api.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		SnapshotWhen: func(state counterState, event int, seqNr uint64) bool {
			return event%3 == 0
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/" + pid})
	act.PreStart()

	// Send 6 commands: events 1,2,3,4,5,6
	// Predicate triggers on events 3 and 6 (multiples of 3).
	for i := 1; i <= 6; i++ {
		act.Receive(i)
	}

	if act.seqNr != 6 {
		t.Fatalf("seqNr = %d, want 6", act.seqNr)
	}

	// No snapshot should exist for seqNr 1 or 2 (non-multiples of 3).
	snap, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 2,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(maxSeq=2): %v", err)
	}
	if snap != nil {
		t.Errorf("unexpected snapshot at seqNr <= 2, got seqNr=%d", snap.Metadata.SequenceNr)
	}

	// Snapshot at seqNr 3 must exist (event=3 is multiple of 3).
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 3,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(maxSeq=3): %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot at seqNr=3, got nil")
	}
	if snap.Metadata.SequenceNr != 3 {
		t.Errorf("snapshot seqNr = %d, want 3", snap.Metadata.SequenceNr)
	}
	// State at seqNr 3: 1+2+3 = 6
	if snap.Snapshot.(counterState).Value != 6 {
		t.Errorf("snapshot state.Value = %d, want 6", snap.Snapshot.(counterState).Value)
	}

	// No snapshot should exist between seqNr 4 and 5.
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MinSequenceNr: 4,
		MaxSequenceNr: 5,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(4..5): %v", err)
	}
	if snap != nil {
		t.Errorf("unexpected snapshot between seqNr 4-5, got seqNr=%d", snap.Metadata.SequenceNr)
	}

	// Snapshot at seqNr 6 must exist (event=6 is multiple of 3).
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot(latest): %v", err)
	}
	if snap == nil {
		t.Fatal("expected latest snapshot at seqNr=6, got nil")
	}
	if snap.Metadata.SequenceNr != 6 {
		t.Errorf("latest snapshot seqNr = %d, want 6", snap.Metadata.SequenceNr)
	}
	// State at seqNr 6: 1+2+3+4+5+6 = 21
	if snap.Snapshot.(counterState).Value != 21 {
		t.Errorf("latest snapshot state.Value = %d, want 21", snap.Snapshot.(counterState).Value)
	}

	_ = ctx
}

// TestSnapshotWhen_OrWithInterval verifies that SnapshotWhen and SnapshotInterval
// coexist: a snapshot is saved whenever either condition is true.
func TestSnapshotWhen_OrWithInterval(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ss := persistence.NewInMemorySnapshotStore()
	ctx := context.Background()
	const pid = "snapwhen-or-interval"

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: pid,
		Journal:       journal,
		SnapshotStore: ss,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(c typed_api.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		// Interval every 4 events
		SnapshotInterval: 4,
		// Predicate: snapshot on event value == 2
		SnapshotWhen: func(state counterState, event int, seqNr uint64) bool {
			return event == 2
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/" + pid})
	act.PreStart()

	// Send 5 commands (events 1..5).
	// Predicate fires at seqNr=2 (event=2).
	// Interval fires at seqNr=4 (4%4==0).
	for i := 1; i <= 5; i++ {
		act.Receive(i)
	}

	// Snapshot at seqNr=2 from predicate.
	snap, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MinSequenceNr: 2,
		MaxSequenceNr: 2,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(seqNr=2): %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot at seqNr=2 (predicate trigger), got nil")
	}

	// Snapshot at seqNr=4 from interval.
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MinSequenceNr: 4,
		MaxSequenceNr: 4,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(seqNr=4): %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot at seqNr=4 (interval trigger), got nil")
	}

	// No snapshot at seqNr=1, 3, or 5.
	for _, seqNr := range []uint64{1, 3, 5} {
		snap, err = ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
			MinSequenceNr: seqNr,
			MaxSequenceNr: seqNr,
			MaxTimestamp:  math.MaxInt64,
		})
		if err != nil {
			t.Fatalf("LoadSnapshot(seqNr=%d): %v", seqNr, err)
		}
		if snap != nil {
			t.Errorf("unexpected snapshot at seqNr=%d", seqNr)
		}
	}

	_ = ctx
}

// TestSnapshotWhen_Recovery verifies that an actor recovers from a predicate-triggered
// snapshot plus subsequent journal events.
func TestSnapshotWhen_Recovery(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ss := persistence.NewInMemorySnapshotStore()
	const pid = "snapwhen-recovery"

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: pid,
		Journal:       journal,
		SnapshotStore: ss,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(c typed_api.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		// Snapshot after event==10 (seqNr=2 in this test).
		SnapshotWhen: func(state counterState, event int, seqNr uint64) bool {
			return event == 10
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/" + pid})
	act.PreStart()

	// seqNr=1 (event=5), seqNr=2 (event=10, snapshot), seqNr=3 (event=3)
	act.Receive(5)
	act.Receive(10)
	act.Receive(3)

	if act.seqNr != 3 {
		t.Fatalf("seqNr = %d, want 3", act.seqNr)
	}
	if act.state.Value != 18 {
		t.Fatalf("state.Value = %d, want 18", act.state.Value)
	}

	// Recover into a fresh actor — should load snapshot at seqNr=2 then replay seqNr=3.
	act2 := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act2.SetSystem(&typedMockContext{})
	act2.SetSelf(&typedMockRef{path: "/user/" + pid})
	act2.PreStart()

	if act2.seqNr != 3 {
		t.Errorf("recovered seqNr = %d, want 3", act2.seqNr)
	}
	if act2.state.Value != 18 {
		t.Errorf("recovered state.Value = %d, want 18", act2.state.Value)
	}
}
