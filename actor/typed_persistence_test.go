/*
 * typed_persistence_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/persistence"
)

type CounterCommand interface{}
type Increment struct{}
type GetValue struct {
	ReplyTo TypedActorRef[int]
}

type CounterEvent struct {
	Delta int
}

type CounterState struct {
	Value int
}

func TestPersistentActor(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	snaps := persistence.NewInMemorySnapshotStore()

	behavior := &EventSourcedBehavior[CounterCommand, CounterEvent, CounterState]{
		PersistenceID: "counter-1",
		Journal:       journal,
		SnapshotStore: snaps,
		InitialState:  CounterState{Value: 0},
		CommandHandler: func(ctx TypedContext[CounterCommand], state CounterState, cmd CounterCommand) Effect[CounterEvent, CounterState] {
			switch m := cmd.(type) {
			case Increment:
				return Persist[CounterEvent, CounterState](CounterEvent{Delta: 1})
			case GetValue:
				m.ReplyTo.Tell(state.Value)
				return None[CounterEvent, CounterState]()
			}
			return None[CounterEvent, CounterState]()
		},
		EventHandler: func(state CounterState, event CounterEvent) CounterState {
			state.Value += event.Delta
			return state
		},
		SnapshotInterval: 5,
	}

	sys := &typedMockContext{}
	_, err := SpawnPersistent(sys, behavior, "counter")
	if err != nil {
		t.Fatalf("SpawnPersistent failed: %v", err)
	}

	// 1. Manually trigger recovery and message processing for the test
	p := sys.spawnedProps.New().(*persistentActor[CounterCommand, CounterEvent, CounterState])
	p.setSystem(sys)
	p.PreStart() // recovery

	// 2. Send some commands
	p.Receive(Increment{})
	p.Receive(Increment{})
	p.Receive(Increment{})

	if p.state.Value != 3 {
		t.Errorf("expected value 3, got %d", p.state.Value)
	}

	// 3. Verify journal
	ctx := context.Background()
	count := 0
	if err := journal.ReplayMessages(ctx, "counter-1", 0, 10, 0, func(repr persistence.PersistentRepr) {
		count++
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 events in journal, got %d", count)
	}

	// 4. Test recovery from journal
	p2 := NewPersistentActor(behavior).(*persistentActor[CounterCommand, CounterEvent, CounterState])
	p2.setSystem(sys)
	p2.PreStart()
	if p2.state.Value != 3 {
		t.Errorf("expected recovered value 3, got %d", p2.state.Value)
	}

	// 5. Test snapshotting
	p.Receive(Increment{})
	p.Receive(Increment{}) // Should trigger snapshot (seqNr 5)

	snap, err := snaps.LoadSnapshot(ctx, "counter-1", persistence.LatestSnapshotCriteria())
	if err != nil || snap == nil {
		t.Errorf("expected snapshot to be saved")
	} else if s, ok := snap.Snapshot.(CounterState); !ok {
		t.Errorf("expected snapshot to be CounterState, got %T", snap.Snapshot)
	} else if s.Value != 5 {
		t.Errorf("expected snapshot value 5, got %d", s.Value)
	}

	// 6. Test recovery from snapshot
	p3 := NewPersistentActor(behavior).(*persistentActor[CounterCommand, CounterEvent, CounterState])
	p3.setSystem(sys)
	p3.PreStart()
	if p3.state.Value != 5 {
		t.Errorf("expected recovered value 5 from snapshot, got %d", p3.state.Value)
	}
	if p3.seqNr != 5 {
		t.Errorf("expected seqNr 5, got %d", p3.seqNr)
	}
}
