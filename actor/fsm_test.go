/*
 * fsm_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type LockState string

const (
	Locked   LockState = "Locked"
	Unlocked LockState = "Unlocked"
)

type LockData struct {
	Code string
}

type unhandledMsg struct{}
type stopMsg struct{}

func TestFSM_Lifecycle(t *testing.T) {
	received := make(chan string, 20)

	behavior := Setup(func(ctx TypedContext[any]) Behavior[any] {
		fsm := NewFSM[LockState, LockData](ctx)

		fsm.StartWith(Locked, LockData{Code: "1234"})

		fsm.When(Locked, func(e Event[LockData]) State[LockState, LockData] {
			t.Logf("FSM: [Locked] received %T", e.Msg)
			if code, ok := e.Msg.(string); ok && code == e.Data.Code {
				received <- "unlocking"
				return fsm.Goto(Unlocked).ForMax(200 * time.Millisecond).Build()
			}
			return fsm.Unhandled().Build()
		})

		fsm.When(Unlocked, func(e Event[LockData]) State[LockState, LockData] {
			t.Logf("FSM: [Unlocked] received %T", e.Msg)
			switch e.Msg.(type) {
			case StateTimeout:
				received <- "timeout-locking"
				return fsm.Goto(Locked).Build()
			}
			return fsm.Unhandled().Build()
		})

		fsm.WhenUnhandled(func(e Event[LockData]) State[LockState, LockData] {
			switch e.Msg.(type) {
			case unhandledMsg:
				received <- "caught-unhandled"
			case stopMsg:
				return fsm.Stop().Build()
			}
			return fsm.Stay().Build()
		})

		fsm.OnTransition(func(from, to LockState) {
			received <- fmt.Sprintf("%s->%s", from, to)
		})

		fsm.OnTermination(func(e StopEvent[LockState, LockData]) {
			received <- fmt.Sprintf("terminated-%s", e.FinalState)
		})

		return fsm.Initialize()
	})

	// Use manual setup
	actor := newTypedActor(behavior)
	actor.SetSelf(&fsmLoopbackRef{a: actor})
	actor.PreStart()
	defer actor.PostStop()

	// Helper to wait for messages with timeout
	wait := func(expected string, timeout time.Duration) {
		select {
		case msg := <-received:
			assert.Equal(t, expected, msg)
		case <-time.After(timeout):
			t.Fatalf("Timed out waiting for %q", expected)
		}
	}

	// 1. Correct code -> Unlock
	t.Log("Sending correct code...")
	actor.Receive("1234")
	wait("unlocking", 100*time.Millisecond)
	wait("Locked->Unlocked", 100*time.Millisecond)

	// 2. Test Unhandled
	t.Log("Sending unhandled message...")
	actor.Receive(unhandledMsg{})
	wait("caught-unhandled", 100*time.Millisecond)

	// 3. Test Timeout
	t.Log("Waiting for FSM timeout...")
	wait("timeout-locking", 500*time.Millisecond)
	wait("Unlocked->Locked", 100*time.Millisecond)

	// 4. Test Termination
	t.Log("Sending stop message...")
	actor.Receive(stopMsg{})
	wait("terminated-Locked", 100*time.Millisecond)
}

type fsmLoopbackRef struct {
	Ref
	a *typedActor[any]
}

func (r *fsmLoopbackRef) Tell(msg any, _ ...Ref) {
	// For direct unit tests, we call Receive on the actor instance.
	// In a real system, the dispatcher would call Receive.
	r.a.Receive(msg)
}
func (r *fsmLoopbackRef) Path() string { return "/test/fsm" }
