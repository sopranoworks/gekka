/*
 * fsm_classic_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type lockFSM struct {
	BaseFSM[string, int]
}

func newLockFSM() *lockFSM {
	f := &lockFSM{
		BaseFSM: *NewBaseFSM[string, int](),
	}
	
	f.StartWith("Locked", 0)

	f.When("Locked", func(e Event[int]) State[string, int] {
		if code, ok := e.Msg.(string); ok && code == "correct-code" {
			return f.Goto("Unlocked").Using(0).Build()
		}
		return f.Stay().Using(e.Data + 1).Build()
	})

	f.When("Unlocked", func(e Event[int]) State[string, int] {
		return f.Goto("Locked").Build()
	})

	return f
}

func TestClassicFSM_Lifecycle(t *testing.T) {
	f := newLockFSM()
	
	// Mock environment
	f.SetSelf(&FunctionalMockRef{PathURI: "/user/fsm", Handler: func(m any) { f.Receive(m) }})
	InjectSystem(f, &ScatterGatherTestSystem{t: t})
	f.PreStart() // Manual call for mock test
	Start(f)

	assert.Equal(t, "Locked", f.currentState)
	assert.Equal(t, 0, f.stateData)

	// Send wrong code
	f.Receive("wrong-code")
	assert.Equal(t, "Locked", f.currentState)
	assert.Equal(t, 1, f.stateData)

	// Send correct code
	f.Receive("correct-code")
	assert.Equal(t, "Unlocked", f.currentState)
	assert.Equal(t, 0, f.stateData)

	// Send any message to lock again
	f.Receive("lock-me")
	assert.Equal(t, "Locked", f.currentState)
}

func TestClassicFSM_Timeout(t *testing.T) {
	received := make(chan any, 10)
	f := NewBaseFSM[string, int]()
	f.StartWith("StateA", 0)
	f.When("StateA", func(e Event[int]) State[string, int] {
		switch e.Msg.(type) {
		case string:
			return f.Goto("StateB").ForMax(100 * time.Millisecond).Build()
		case StateTimeout:
			received <- "timeout"
			return f.Stay().Build()
		}
		return f.Stay().Build()
	})
	f.When("StateB", func(e Event[int]) State[string, int] {
		if _, ok := e.Msg.(StateTimeout); ok {
			received <- "timeout"
			return f.Goto("StateA").Build()
		}
		return f.Stay().Build()
	})

	f.SetSelf(&FunctionalMockRef{PathURI: "/user/fsm", Handler: func(m any) { 
		f.Receive(m) 
	}})
	InjectSystem(f, &ScatterGatherTestSystem{t: t})
	f.PreStart() // Manual call for mock test
	Start(f)

	f.Receive("go-to-b")
	assert.Equal(t, "StateB", f.currentState)

	select {
	case msg := <-received:
		assert.Equal(t, "timeout", msg)
		assert.Equal(t, "StateA", f.currentState)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout message not received")
	}
}
