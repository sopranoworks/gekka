/*
 * snapshot_cleanup_test.go
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

	"github.com/sopranoworks/gekka/actor"
	typed_api "github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
)

// TestSnapshotCleanup verifies that MaxSnapshots=2 retains only the two most
// recent snapshots after saving 5 (one per event with SnapshotInterval=1).
func TestSnapshotCleanup(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ss := persistence.NewInMemorySnapshotStore()
	ctx := context.Background()

	const pid = "cleanup-actor"

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
		SnapshotInterval: 1,
		MaxSnapshots:     2,
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/" + pid})
	act.PreStart()

	// Send 5 commands — each triggers a snapshot (SnapshotInterval=1).
	for i := 0; i < 5; i++ {
		act.Receive(1)
	}

	if act.seqNr != 5 {
		t.Fatalf("seqNr = %d, want 5", act.seqNr)
	}

	// Snapshots at seqNr 1, 2, 3 must have been deleted (MaxSnapshots=2 keeps 4, 5).
	snap, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 3,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(maxSeq=3): %v", err)
	}
	if snap != nil {
		t.Errorf("expected no snapshot with seqNr <= 3, got seqNr=%d", snap.Metadata.SequenceNr)
	}

	// Snapshot at seqNr 4 must still exist.
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
		MaxSequenceNr: 4,
		MaxTimestamp:  math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("LoadSnapshot(maxSeq=4): %v", err)
	}
	if snap == nil {
		t.Fatal("expected snapshot at seqNr=4, got nil")
	}
	if snap.Metadata.SequenceNr != 4 {
		t.Errorf("snapshot seqNr = %d, want 4", snap.Metadata.SequenceNr)
	}

	// Latest snapshot must be at seqNr 5.
	snap, err = ss.LoadSnapshot(ctx, pid, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot(latest): %v", err)
	}
	if snap == nil {
		t.Fatal("expected latest snapshot at seqNr=5, got nil")
	}
	if snap.Metadata.SequenceNr != 5 {
		t.Errorf("latest snapshot seqNr = %d, want 5", snap.Metadata.SequenceNr)
	}

	// Final state must reflect all 5 increments.
	if act.state.Value != 5 {
		t.Errorf("state.Value = %d, want 5", act.state.Value)
	}

	// Verify recovery uses the latest remaining snapshot (seqNr=4) and replays only seqNr=5.
	act2 := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act2.SetSystem(&typedMockContext{})
	act2.SetSelf(&typedMockRef{path: "/user/" + pid})
	act2.PreStart()

	if act2.state.Value != 5 {
		t.Errorf("recovered state.Value = %d, want 5", act2.state.Value)
	}
	if act2.seqNr != 5 {
		t.Errorf("recovered seqNr = %d, want 5", act2.seqNr)
	}

	_ = ctx
}

// TestSnapshotCleanup_NoCleanupWhenBelowLimit ensures snapshots are not
// deleted before the MaxSnapshots limit is reached.
func TestSnapshotCleanup_NoCleanupWhenBelowLimit(t *testing.T) {
	ss := persistence.NewInMemorySnapshotStore()
	ctx := context.Background()
	const pid = "no-cleanup-actor"

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: pid,
		Journal:       persistence.NewInMemoryJournal(),
		SnapshotStore: ss,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(c typed_api.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		SnapshotInterval: 1,
		MaxSnapshots:     5,
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/" + pid})
	act.PreStart()

	// Send only 3 commands — below the MaxSnapshots=5 limit, nothing should be deleted.
	for i := 0; i < 3; i++ {
		act.Receive(1)
	}

	// All 3 snapshots must be present.
	for seqNr := uint64(1); seqNr <= 3; seqNr++ {
		snap, err := ss.LoadSnapshot(ctx, pid, persistence.SnapshotSelectionCriteria{
			MinSequenceNr: seqNr,
			MaxSequenceNr: seqNr,
			MaxTimestamp:  math.MaxInt64,
		})
		if err != nil {
			t.Fatalf("LoadSnapshot(seqNr=%d): %v", seqNr, err)
		}
		if snap == nil {
			t.Errorf("snapshot at seqNr=%d should not have been deleted", seqNr)
		}
	}

	_ = ctx
}

// actorSystemInterface allows setting up the actor without access to the actor system.
var _ actor.ActorContext = (*typedMockContext)(nil)
