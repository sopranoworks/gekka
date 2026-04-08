/*
 * recovery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
)

func TestDisabledRecovery_StartsFresh(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	// Pre-populate journal
	_ = journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "dr-1", SequenceNr: 1, Payload: 100},
		{PersistenceID: "dr-1", SequenceNr: 2, Payload: 200},
	})

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "dr-1",
		Journal:       journal,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		RecoveryStrategy: DisabledRecovery(),
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/dr-1"})
	act.PreStart()

	// State should be initial (not recovered)
	assert.Equal(t, 0, act.state.Value)
	// But seqNr should be highest so new events don't conflict
	assert.Equal(t, uint64(2), act.seqNr)
}

func TestRecoveryWithFilter_SkipsEvents(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	// Pre-populate: events 1, 2, 3, 4, 5
	for i := 1; i <= 5; i++ {
		_ = journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
			{PersistenceID: "rf-1", SequenceNr: uint64(i), Payload: i * 10},
		})
	}

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "rf-1",
		Journal:       journal,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		// Skip even-numbered events (20, 40)
		RecoveryStrategy: RecoveryWithFilter(func(repr persistence.PersistentRepr) bool {
			return repr.SequenceNr%2 != 0 // keep odd seqNr only
		}),
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/rf-1"})
	act.PreStart()

	// Applied: 10 + 30 + 50 = 90 (skipped 20, 40)
	assert.Equal(t, 90, act.state.Value)
	// seqNr should still be 5 (advanced past all events)
	assert.Equal(t, uint64(5), act.seqNr)
}

func TestRecoveryCompleted_Signal(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	_ = journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "rc-1", SequenceNr: 1, Payload: 10},
		{PersistenceID: "rc-1", SequenceNr: 2, Payload: 20},
	})

	var signalReceived PersistenceSignal
	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "rc-1",
		Journal:       journal,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		SignalHandler: func(signal PersistenceSignal) {
			signalReceived = signal
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/rc-1"})
	act.PreStart()

	assert.NotNil(t, signalReceived)
	rc, ok := signalReceived.(RecoveryCompleted)
	assert.True(t, ok, "expected RecoveryCompleted signal")
	assert.Equal(t, uint64(2), rc.HighestSequenceNr)
}

func TestRecoveryCompleted_DisabledRecovery(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	_ = journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "rc-2", SequenceNr: 1, Payload: 10},
	})

	var signalReceived PersistenceSignal
	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID:    "rc-2",
		Journal:          journal,
		InitialState:     counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
		RecoveryStrategy: DisabledRecovery(),
		SignalHandler: func(signal PersistenceSignal) {
			signalReceived = signal
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/rc-2"})
	act.PreStart()

	assert.NotNil(t, signalReceived)
	rc, ok := signalReceived.(RecoveryCompleted)
	assert.True(t, ok)
	// With disabled recovery, seqNr is read from journal
	assert.Equal(t, uint64(1), rc.HighestSequenceNr)
}
