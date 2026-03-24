/*
 * durable_state_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package state

import (
	"context"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/stretchr/testify/assert"
)

type counterState struct {
	Value int
}

type increment struct{}
type getValue struct {
	replyTo typed.TypedActorRef[int]
}

func TestDurableState_Counter(t *testing.T) {
	stateStore := &mockStateStore{
		states:    make(map[string]any),
		revisions: make(map[string]uint64),
	}

	behavior := &DurableStateBehavior[any, counterState]{
		PersistenceID: "counter-1",
		EmptyState:    counterState{Value: 0},
		StateStore:    stateStore,
		OnCommand: func(ctx typed.TypedContext[any], state counterState, cmd any) Effect[counterState] {
			switch m := cmd.(type) {
			case increment:
				return Persist(counterState{Value: state.Value + 1})
			case getValue:
				m.replyTo.Tell(state.Value)
				return None[counterState]()
			}
			return Unhandled[counterState]()
		},
	}

	// 1. Initial run
	act := NewDurableStateActor(behavior).(*durableStateActor[any, counterState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/counter"})
	act.PreStart()

	assert.Equal(t, 0, act.state.Value)

	act.Receive(increment{})
	assert.Equal(t, 1, act.state.Value)
	assert.Equal(t, uint64(1), act.revision)

	act.Receive(increment{})
	assert.Equal(t, 2, act.state.Value)
	assert.Equal(t, uint64(2), act.revision)

	// 2. Restart and recover
	act2 := NewDurableStateActor(behavior).(*durableStateActor[any, counterState])
	act2.SetSystem(&typedMockContext{})
	act2.SetSelf(&typedMockRef{path: "/user/counter"})
	act2.PreStart()

	assert.Equal(t, 2, act2.state.Value)
	assert.Equal(t, uint64(2), act2.revision)

	act2.Receive(increment{})
	assert.Equal(t, 3, act2.state.Value)
	assert.Equal(t, uint64(3), act2.revision)
}

type mockStateStore struct {
	states    map[string]any
	revisions map[string]uint64
}

func (m *mockStateStore) Get(ctx context.Context, persistenceID string) (any, uint64, error) {
	return m.states[persistenceID], m.revisions[persistenceID], nil
}

func (m *mockStateStore) Upsert(ctx context.Context, persistenceID string, revision uint64, state any, tag string) error {
	m.states[persistenceID] = state
	m.revisions[persistenceID] = revision
	return nil
}

func (m *mockStateStore) Delete(ctx context.Context, persistenceID string) error {
	delete(m.states, persistenceID)
	delete(m.revisions, persistenceID)
	return nil
}

type typedMockContext struct {
	actor.ActorContext
}

func (m *typedMockContext) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return &typedMockRef{path: "/user/" + name}, nil
}

func (m *typedMockContext) Stop(target actor.Ref) {}

type typedMockRef struct {
	actor.Ref
	path string
}

func (r *typedMockRef) Path() string                      { return r.path }
func (r *typedMockRef) Tell(msg any, sender ...actor.Ref) {}
