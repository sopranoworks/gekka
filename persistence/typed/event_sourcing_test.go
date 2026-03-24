/*
 * event_sourcing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
)

type counterState struct {
	Value int
}

func TestSpawnPersistent(t *testing.T) {
	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "counter-1",
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
	}

	// Mock system and journal
	sys := &typedMockContext{}
	ref, err := SpawnPersistent(sys, behavior, "counter")

	assert.NoError(t, err)
	assert.NotNil(t, ref.Untyped())
}

func TestPersistentActor_Recovery(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	ctx := context.Background()

	// Pre-populate journal
	_ = journal.AsyncWriteMessages(ctx, []persistence.PersistentRepr{
		{PersistenceID: "p1", SequenceNr: 1, Payload: 10},
		{PersistenceID: "p1", SequenceNr: 2, Payload: 20},
	})

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "p1",
		Journal:       journal,
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], state counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(state counterState, event int) counterState {
			return counterState{Value: state.Value + event}
		},
	}

	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/p1"})

	// Trigger recovery
	act.PreStart()

	assert.Equal(t, 30, act.state.Value)
	assert.Equal(t, uint64(2), act.seqNr)
}

type typedMockContext struct {
	actor.ActorContext
	spawnedProps actor.Props
	spawnedName  string
	stoppedRef   actor.Ref
}

func (m *typedMockContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	m.spawnedProps = props
	m.spawnedName = name
	return &typedMockRef{path: "/user/" + name}, nil
}

func (m *typedMockContext) Stop(target actor.Ref) {
	m.stoppedRef = target
}

type typedMockRef struct {
	actor.Ref
	path string
}

func (r *typedMockRef) Path() string                      { return r.path }
func (r *typedMockRef) Tell(msg any, sender ...actor.Ref) {}
