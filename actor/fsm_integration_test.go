//go:build integration

/*
 * fsm_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
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

func TestFSM_Integration(t *testing.T) {
	received := make(chan string, 20)
	system, _ := gekka.NewActorSystem("FSMIntegration")

	behavior := actor.Setup(func(ctx actor.TypedContext[any]) actor.Behavior[any] {
		fsm := actor.NewFSM[LockState, LockData](ctx)

		fsm.StartWith(Locked, LockData{Code: "1234"})

		fsm.When(Locked, func(e actor.Event[LockData]) actor.State[LockState, LockData] {
			if code, ok := e.Msg.(string); ok && code == e.Data.Code {
				received <- "unlocking"
				return fsm.Goto(Unlocked).ForMax(500 * time.Millisecond).Build()
			}
			return fsm.Unhandled().Build()
		})

		fsm.When(Unlocked, func(e actor.Event[LockData]) actor.State[LockState, LockData] {
			switch e.Msg.(type) {
			case actor.StateTimeout:
				received <- "timeout-locking"
				return fsm.Goto(Locked).Build()
			}
			return fsm.Unhandled().Build()
		})

		fsm.WhenUnhandled(func(e actor.Event[LockData]) actor.State[LockState, LockData] {
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

		fsm.OnTermination(func(e actor.StopEvent[LockState, LockData]) {
			t.Logf("FSM: OnTermination called with state %v", e.FinalState)
			received <- fmt.Sprintf("terminated-%s", e.FinalState)
		})

		return fsm.Initialize()
	})

	ref, _ := gekka.Spawn(system, behavior, "lock")

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
	ref.Tell("1234")
	wait("unlocking", 500*time.Millisecond)
	wait("Locked->Unlocked", 500*time.Millisecond)

	// 2. Test Unhandled
	ref.Tell(unhandledMsg{})
	wait("caught-unhandled", 500*time.Millisecond)

	// 3. Test Timeout
	wait("timeout-locking", 1*time.Second)
	wait("Unlocked->Locked", 500*time.Millisecond) // Corrected expectation

	// 4. Test Termination
	ref.Tell(stopMsg{})
	wait("terminated-Locked", 500*time.Millisecond)
}
