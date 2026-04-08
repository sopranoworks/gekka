/*
 * retention_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
)

func TestRetentionCriteria_SnapshotsAndCleanup(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	snapStore := persistence.NewInMemorySnapshotStore()

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "ret-1",
		Journal:       journal,
		SnapshotStore: snapStore,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		RetentionCriteria: SnapshotEvery(5, 2),
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/ret-1"})
	act.PreStart()

	// Send 20 events
	for i := 1; i <= 20; i++ {
		act.Receive(i)
	}

	// With SnapshotEvery(5, 2), snapshots at 5, 10, 15, 20.
	// Keep 2 → delete up to seqNr 10 after snapshot at 20.
	// Verify latest snapshot exists
	snap, err := snapStore.LoadSnapshot(nil, "ret-1", persistence.LatestSnapshotCriteria())
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Equal(t, uint64(20), snap.Metadata.SequenceNr)
	// State should be sum 1..20 = 210
	assert.Equal(t, 210, snap.Snapshot.(counterState).Value)
}

func TestRetentionCriteria_WithDeleteEventsOnSnapshot(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	snapStore := persistence.NewInMemorySnapshotStore()

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "ret-2",
		Journal:       journal,
		SnapshotStore: snapStore,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		RetentionCriteria: SnapshotEvery(5, 2).WithDeleteEventsOnSnapshot(),
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/ret-2"})
	act.PreStart()

	for i := 1; i <= 20; i++ {
		act.Receive(i)
	}

	// Verify recovery still works with snapshot + remaining events
	act2 := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act2.SetSystem(&typedMockContext{})
	act2.SetSelf(&typedMockRef{path: "/user/ret-2-2"})
	act2.PreStart()

	assert.Equal(t, 210, act2.state.Value)
	assert.Equal(t, uint64(20), act2.seqNr)
}
