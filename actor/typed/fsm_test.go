/*
 * fsm_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

type lockState int

const (
	Locked lockState = iota
	Unlocked
)

type lockData struct {
	code string
}

type stopMsg struct{}
type unhandledMsg struct{}

func TestFSM_Lifecycle(t *testing.T) {
	fsm := NewFSM[lockState, lockData]()
	fsm.StartWith(Locked, lockData{code: "1234"})

	fsm.When(Locked, func(e actor.Event[lockData]) actor.State[lockState, lockData] {
		switch m := e.Msg.(type) {
		case string:
			if m == e.Data.code {
				t.Log("FSM: [Locked] received correct code")
				return fsm.Goto(Unlocked).Using(lockData{code: ""}).Build()
			}
			t.Log("FSM: [Locked] received wrong code")
			return fsm.Stay().Build()
		case stopMsg:
			return fsm.Stop().Build()
		}
		return fsm.Unhandled().Build()
	})

	fsm.When(Unlocked, func(e actor.Event[lockData]) actor.State[lockState, lockData] {
		switch e.Msg.(type) {
		case string:
			return fsm.Goto(Locked).Using(lockData{code: "1234"}).Build()
		case actor.StateTimeout:
			t.Log("FSM: [Unlocked] timed out, locking")
			return fsm.Goto(Locked).Using(lockData{code: "1234"}).Build()
		}
		return fsm.Unhandled().Build()
	})

	fsm.WhenUnhandled(func(e actor.Event[lockData]) actor.State[lockState, lockData] {
		t.Logf("FSM: unhandled message: %T", e.Msg)
		return fsm.Stay().Build()
	})

	behavior := fsm.Behavior()
	act := newTypedActor(behavior)
	
	// Mock context for testing
	ctx := &typedMockContext{}
	act.SetSystem(ctx)
	act.SetSelf(&typedMockRef{path: "/user/fsm"})
	act.PreStart()

	// 1. Initial state
	if fsm.currentState != Locked {
		t.Errorf("expected state Locked, got %v", fsm.currentState)
	}

	// 2. Send correct code
	t.Log("Sending correct code...")
	act.Receive("1234")
	if fsm.currentState != Unlocked {
		t.Errorf("expected state Unlocked, got %v", fsm.currentState)
	}

	// 3. Send unhandled message
	t.Log("Sending unhandled message...")
	act.Receive(unhandledMsg{})
	if fsm.currentState != Unlocked {
		t.Errorf("expected state Unlocked, got %v", fsm.currentState)
	}

	// 4. Test timeout
	t.Log("Waiting for FSM timeout...")
	act.Receive(actor.StateTimeout{})
	if fsm.currentState != Locked {
		t.Errorf("expected state Locked after timeout, got %v", fsm.currentState)
	}

	// 5. Test stop
	t.Log("Sending stop message...")
	act.Receive(stopMsg{})
	if !act.stopped {
		t.Error("expected actor to be stopped")
	}
}
