/*
 * fsm_classic.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"reflect"
)

// BaseFSM is a finite state machine implementation for classic actors.
// S is the state type (must be comparable), D is the state data type.
type BaseFSM[S any, D any] struct {
	BaseActor
	currentState S
	stateData    D
	timers       *timerScheduler[any]

	handlers           map[any]func(Event[D]) State[S, D]
	unhandledHandler   func(Event[D]) State[S, D]
	transitions        []func(S, S)
	terminationHandler func(StopEvent[S, D])

	initialized bool
	startCalled bool
}

// NewBaseFSM creates a new BaseFSM instance.
func NewBaseFSM[S any, D any]() *BaseFSM[S, D] {
	return &BaseFSM[S, D]{
		BaseActor: NewBaseActor(),
		handlers:  make(map[any]func(Event[D]) State[S, D]),
	}
}

// StartWith sets the initial state and data for the FSM.
func (f *BaseFSM[S, D]) StartWith(state S, data D) {
	f.currentState = state
	f.stateData = data
	f.startCalled = true
}

// When defines the handler for a specific state.
func (f *BaseFSM[S, D]) When(state S, handler func(Event[D]) State[S, D]) {
	f.handlers[state] = handler
}

// WhenUnhandled defines a fallback handler for messages not matched by any state handler.
func (f *BaseFSM[S, D]) WhenUnhandled(handler func(Event[D]) State[S, D]) {
	f.unhandledHandler = handler
}

// OnTransition registers a callback for state transitions.
func (f *BaseFSM[S, D]) OnTransition(handler func(from S, to S)) {
	f.transitions = append(f.transitions, handler)
}

// OnTermination registers a callback for FSM termination.
func (f *BaseFSM[S, D]) OnTermination(handler func(StopEvent[S, D])) {
	f.terminationHandler = handler
}

// Goto transitions the FSM to a new state.
func (f *BaseFSM[S, D]) Goto(nextState S) *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: nextState, data: f.stateData}
}

// Stay remains in the current state.
func (f *BaseFSM[S, D]) Stay() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData}
}

// Unhandled indicates that the current event was not handled.
func (f *BaseFSM[S, D]) Unhandled() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData, unhandled: true}
}

// Stop terminates the FSM.
func (f *BaseFSM[S, D]) Stop() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData, stop: true, reason: Normal}
}

// PreStart initializes the FSM.
func (f *BaseFSM[S, D]) PreStart() {
	if !f.startCalled {
		panic("FSM: StartWith must be called before actor starts")
	}
	f.timers = newTimerScheduler[any](f.Self())
	f.initialized = true
	f.Log().Info("FSM: started", "state", f.currentState)
}

// PostStop cleans up FSM resources.
func (f *BaseFSM[S, D]) PostStop() {
	if f.timers != nil {
		f.timers.cancelAll()
	}
}

// Receive handles incoming messages by delegating to the current state's handler.
func (f *BaseFSM[S, D]) Receive(msg any) {
	f.Log().Debug("FSM: processing message", "state", f.currentState, "msgType", fmt.Sprintf("%T", msg))

	handler, ok := f.handlers[f.currentState]
	event := Event[D]{Msg: msg, Data: f.stateData}
	var next State[S, D]

	if ok {
		next = handler(event)
	}

	if !ok || next.Unhandled {
		if f.unhandledHandler != nil {
			next = f.unhandledHandler(event)
		} else if next.Unhandled {
			return
		} else {
			f.Log().Warn("FSM: unhandled message", "state", f.currentState, "msgType", fmt.Sprintf("%T", msg))
			return
		}
	}

	// Handle Stop
	if next.Stop {
		f.currentState = next.NextState
		f.stateData = next.NextData
		if f.terminationHandler != nil {
			f.terminationHandler(StopEvent[S, D]{
				Reason:     next.Reason,
				FinalState: f.currentState,
				FinalData:  f.stateData,
			})
		}
		f.Log().Info("FSM: terminating", "reason", next.Reason, "state", f.currentState)
		f.System().Stop(f.Self())
		return
	}

	// Check for transition
	from := f.currentState
	to := next.NextState

	f.currentState = to
	f.stateData = next.NextData

	if !reflect.DeepEqual(from, to) {
		f.Log().Info("FSM: state transition", "from", from, "to", to)
		for _, t := range f.transitions {
			t(from, to)
		}
	}

	// Handle timeouts
	if next.Timeout > 0 {
		f.timers.StartSingleTimer("fsm-timeout", StateTimeout{}, next.Timeout)
	} else if !reflect.DeepEqual(from, to) {
		f.timers.Cancel("fsm-timeout")
	}
}
