/*
 * stash_overflow_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/persistence"
	"github.com/stretchr/testify/assert"
)

// TestTypedStashOverflow_DropStrategy verifies that the configured "drop"
// stash-overflow-strategy silently drops commands once the recovery stash hits
// its capacity, leaving any earlier commands intact.
//
// HOCON: pekko.persistence.typed.stash-overflow-strategy = drop
func TestTypedStashOverflow_DropStrategy(t *testing.T) {
	prevCap := GetDefaultStashCapacity()
	prevStrat := GetDefaultStashOverflowStrategy()
	defer SetDefaultStashCapacity(prevCap)
	defer SetDefaultStashOverflowStrategy(prevStrat)

	SetDefaultStashCapacity(2)
	SetDefaultStashOverflowStrategy("drop")

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "stash-drop",
		Journal:       persistence.NewInMemoryJournal(),
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], s counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(s counterState, e int) counterState {
			return counterState{Value: s.Value + e}
		},
	}

	sys := &typedMockContext{}
	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(sys)
	act.SetSelf(&typedMockRef{path: "/user/stash-drop"})

	// Do NOT call PreStart — recovering remains true so Receive routes to the
	// stash overflow code path under test.
	act.Receive(1)
	act.Receive(2)
	act.Receive(3) // overflow — must be dropped under "drop" strategy
	act.Receive(4) // overflow — must be dropped under "drop" strategy

	assert.Equal(t, 2, len(act.stash), "stash should be capped at 2 entries with drop strategy")
	assert.Equal(t, []int{1, 2}, act.stash, "earlier commands must survive overflow")
	assert.Nil(t, sys.stoppedRef, "drop strategy must not stop the actor on overflow")
}

// TestTypedStashOverflow_FailStrategy verifies that the configured "fail"
// stash-overflow-strategy stops the actor when the recovery stash overflows.
//
// HOCON: pekko.persistence.typed.stash-overflow-strategy = fail
func TestTypedStashOverflow_FailStrategy(t *testing.T) {
	prevCap := GetDefaultStashCapacity()
	prevStrat := GetDefaultStashOverflowStrategy()
	defer SetDefaultStashCapacity(prevCap)
	defer SetDefaultStashOverflowStrategy(prevStrat)

	SetDefaultStashCapacity(1)
	SetDefaultStashOverflowStrategy("fail")

	behavior := &EventSourcedBehavior[int, int, counterState]{
		PersistenceID: "stash-fail",
		Journal:       persistence.NewInMemoryJournal(),
		InitialState:  counterState{Value: 0},
		CommandHandler: func(ctx typed.TypedContext[int], s counterState, cmd int) Effect[int, counterState] {
			return Persist[int, counterState](cmd)
		},
		EventHandler: func(s counterState, e int) counterState {
			return counterState{Value: s.Value + e}
		},
	}

	sys := &typedMockContext{}
	self := &typedMockRef{path: "/user/stash-fail"}
	act := NewPersistentActor(behavior).(*persistentActor[int, int, counterState])
	act.SetSystem(sys)
	act.SetSelf(self)

	act.Receive(1)
	act.Receive(2) // overflow — must trigger Stop under "fail" strategy

	assert.Equal(t, 1, len(act.stash), "stash should be capped at 1 entry with fail strategy")
	if assert.NotNil(t, sys.stoppedRef, "fail strategy must stop the actor on overflow") {
		assert.Equal(t, actor.Ref(self), sys.stoppedRef, "the stopped actor should be self")
	}
}
