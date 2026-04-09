/*
 * stash_user_test.go
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

// Commands for the lock/unlock FSM.
type lockCmd interface{ isLockCmd() }

type cmdLock struct{}
type cmdUnlock struct{}
type cmdWork struct{ ID int }

func (cmdLock) isLockCmd()   {}
func (cmdUnlock) isLockCmd() {}
func (cmdWork) isLockCmd()   {}

// The single event type persisted when a Work command is processed.
type evtWorked struct{ ID int }

// The actor's persistent state: an ordered log of processed IDs.
type lockState struct {
	Log []int
}

// TestUserStash_StashWhileLockedThenUnstash exercises the full ctx.Stash()
// round trip inside a real persistent actor:
//
//  1. The actor starts "locked" (a test-scoped flag; the state itself only
//     records processed work IDs).
//  2. Three cmdWork commands arrive while locked — each one is routed to
//     ctx.Stash().Stash(cmd) and dropped from the current Receive pass.
//  3. A cmdUnlock command arrives — the handler flips the locked flag to
//     false and calls ctx.Stash().UnstashAll(), which pushes all three
//     stashed commands onto userStashPending via the redeliver closure.
//  4. The unlock handler returns. persistentActor.Receive then invokes
//     drainUserStash(), which pops the three commands in FIFO order and
//     re-enters Receive for each one. Each re-entry handles cmdWork in
//     the unlocked branch, which persists an evtWorked event, which
//     updates state.Log via the EventHandler.
//  5. The final state.Log must equal [1, 2, 3] — proof that (a) stashed
//     commands were replayed, (b) in FIFO order, and (c) the drain loop
//     didn't infinite-loop or drop messages.
//
// Before Task 1's real StashBufferImpl, Stash() was a silent no-op stub
// and this test would produce state.Log == [] (empty).
func TestUserStash_StashWhileLockedThenUnstash(t *testing.T) {
	// Test-scoped lock flag. Flipped by cmdUnlock. We deliberately keep
	// this OUT of the persisted state to avoid coupling the test to the
	// EventHandler ordering guarantees — what's under test here is the
	// stash/drain pipeline, not persistence itself.
	locked := true

	journal := persistence.NewInMemoryJournal()

	behavior := &EventSourcedBehavior[lockCmd, evtWorked, lockState]{
		PersistenceID: "lock-test-1",
		InitialState:  lockState{},
		Journal:       journal,
		CommandHandler: func(ctx typed.TypedContext[lockCmd], state lockState, cmd lockCmd) Effect[evtWorked, lockState] {
			switch c := cmd.(type) {
			case cmdLock:
				locked = true
				return None[evtWorked, lockState]()

			case cmdUnlock:
				locked = false
				if err := ctx.Stash().UnstashAll(); err != nil {
					t.Errorf("UnstashAll returned unexpected error: %v", err)
				}
				return None[evtWorked, lockState]()

			case cmdWork:
				if locked {
					if err := ctx.Stash().Stash(cmd); err != nil {
						t.Errorf("Stash returned unexpected error: %v", err)
					}
					return None[evtWorked, lockState]()
				}
				// cmdWork (command) and evtWorked (event) are semantically distinct
				// types; the gosimple S1016 conversion suggestion would erase that
				// distinction, so we keep the explicit field construction.
				return Persist[evtWorked, lockState](evtWorked{ID: c.ID}) //nolint:gosimple
			}
			return None[evtWorked, lockState]()
		},
		EventHandler: func(state lockState, event evtWorked) lockState {
			state.Log = append(state.Log, event.ID)
			return state
		},
	}

	// Construct and drive the actor directly (same pattern as
	// TestPersistentActor_Recovery in event_sourcing_test.go).
	act := NewPersistentActor(behavior).(*persistentActor[lockCmd, evtWorked, lockState])
	act.SetSystem(&typedMockContext{})
	act.SetSelf(&typedMockRef{path: "/user/lock-test-1"})
	act.PreStart() // runs recovery (no events), sets recovering=false

	// Three Work commands arrive while locked → all stashed.
	act.Receive(cmdWork{ID: 1})
	act.Receive(cmdWork{ID: 2})
	act.Receive(cmdWork{ID: 3})

	// At this point no events have been persisted — the command handler
	// returned None() for each because it routed the commands to the
	// stash buffer instead.
	assert.Empty(t, act.state.Log, "no work should have been processed while locked")

	// Unlock → handler calls UnstashAll → drainUserStash drains three
	// commands through Receive in FIFO order → each one persists an
	// evtWorked event → EventHandler appends ID to Log.
	act.Receive(cmdUnlock{})

	assert.Equal(t, []int{1, 2, 3}, act.state.Log,
		"stashed Work commands must be re-delivered in FIFO order through the real drain loop")
}
