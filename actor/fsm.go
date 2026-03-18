/*
 * fsm.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"reflect"
	"time"
)

// FSM is a finite state machine builder/manager for typed actors.
// S is the state type (typically an enum or string), D is the state data type.
type FSM[S any, D any] struct {
	ctx          TypedContext[any]
	currentState S
	stateData    D

	// Handlers
	handlers           map[any]func(Event[D]) State[S, D]
	unhandledHandler   func(Event[D]) State[S, D]
	transitions        []func(S, S)
	terminationHandler func(StopEvent[S, D])

	initialized bool
	startCalled bool
}

// Event represents a message received in a particular FSM state.
type Event[D any] struct {
	Msg  any
	Data D
}

// State represents the result of a state transition.
type State[S any, D any] struct {
	NextState S
	NextData  D
	Timeout   time.Duration
	Stop      bool
	Reason    Reason
	Unhandled bool
}

// Reason indicates why the FSM is stopping.
type Reason int

const (
	Normal Reason = iota
	Shutdown
	FailureReason
)

// StopEvent contains information about the FSM termination.
type StopEvent[S any, D any] struct {
	Reason     Reason
	FinalState S
	FinalData  D
}

// NewFSM creates a new FSM instance.
func NewFSM[S any, D any](ctx TypedContext[any]) *FSM[S, D] {
	return &FSM[S, D]{
		ctx:      ctx,
		handlers: make(map[any]func(Event[D]) State[S, D]),
	}
}

// StartWith sets the initial state and data for the FSM.
func (f *FSM[S, D]) StartWith(state S, data D) {
	f.currentState = state
	f.stateData = data
	f.startCalled = true
}

// When defines the handler for a specific state.
func (f *FSM[S, D]) When(state S, handler func(Event[D]) State[S, D]) {
	f.handlers[state] = handler
}

// WhenUnhandled defines a fallback handler for messages not matched by any state handler.
func (f *FSM[S, D]) WhenUnhandled(handler func(Event[D]) State[S, D]) {
	f.unhandledHandler = handler
}

// OnTransition registers a callback for state transitions.
func (f *FSM[S, D]) OnTransition(handler func(from S, to S)) {
	f.transitions = append(f.transitions, handler)
}

// OnTermination registers a callback for FSM termination.
func (f *FSM[S, D]) OnTermination(handler func(StopEvent[S, D])) {
	f.terminationHandler = handler
}

// Goto transitions the FSM to a new state.
func (f *FSM[S, D]) Goto(nextState S) *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: nextState, data: f.stateData}
}

// Stay remains in the current state.
func (f *FSM[S, D]) Stay() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData}
}

// Unhandled indicates that the current event was not handled.
func (f *FSM[S, D]) Unhandled() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData, unhandled: true}
}

// Stop terminates the FSM.
func (f *FSM[S, D]) Stop() *StateBuilder[S, D] {
	return &StateBuilder[S, D]{nextState: f.currentState, data: f.stateData, stop: true, reason: Normal}
}

// StateBuilder facilitates building a State result with optional data and timeout.
type StateBuilder[S any, D any] struct {
	nextState S
	data      D
	timeout   time.Duration
	stop      bool
	reason    Reason
	unhandled bool
}

// Using updates the state data for the transition.
func (b *StateBuilder[S, D]) Using(nextData D) *StateBuilder[S, D] {
	b.data = nextData
	return b
}

// ForMax sets a timeout for the state.
func (b *StateBuilder[S, D]) ForMax(timeout time.Duration) *StateBuilder[S, D] {
	b.timeout = timeout
	return b
}

// WithReason sets the stop reason if stopping.
func (b *StateBuilder[S, D]) WithReason(reason Reason) *StateBuilder[S, D] {
	b.reason = reason
	return b
}

// Build returns the final State object.
func (b *StateBuilder[S, D]) Build() State[S, D] {
	return State[S, D]{
		NextState: b.nextState,
		NextData:  b.data,
		Timeout:   b.timeout,
		Stop:      b.stop,
		Reason:    b.reason,
		Unhandled: b.unhandled,
	}
}

// Initialize validates the FSM setup and returns the starting Behavior.
func (f *FSM[S, D]) Initialize() Behavior[any] {
	if !f.startCalled {
		panic("FSM: StartWith must be called before Initialize")
	}
	f.ctx.Log().Info("FSM: initialized", "address", fmt.Sprintf("%p", f))
	return f.Behavior()
}

// ─── Implementation of Behavior ──────────────────────────────────────────

// Behavior returns a typed actor Behavior that drives the FSM.
func (f *FSM[S, D]) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		// Update context reference to ensure we have the latest injected system/timers
		f.ctx = ctx

		if !f.initialized {
			ctx.Log().Info("FSM: started", "state", f.currentState, "address", fmt.Sprintf("%p", f))
			f.initialized = true
		}

		ctx.Log().Info("FSM: processing message", "state", f.currentState, "msgType", fmt.Sprintf("%T", msg), "msg", msg, "address", fmt.Sprintf("%p", f))

		handler, ok := f.handlers[f.currentState]
		event := Event[D]{Msg: msg, Data: f.stateData}
		var next State[S, D]

		if ok {
			ctx.Log().Debug("FSM: found handler for state", "state", f.currentState)
			next = handler(event)
		}

		if !ok || next.Unhandled {
			if f.unhandledHandler != nil {
				ctx.Log().Debug("FSM: using unhandled handler", "state", f.currentState)
				next = f.unhandledHandler(event)
			} else if next.Unhandled {
				// We stay if it's explicitly unhandled but no global handler
				return Same[any]()
			} else {
				ctx.Log().Warn("FSM: unhandled message", "state", f.currentState, "msgType", fmt.Sprintf("%T", msg))
				return Same[any]()
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
			ctx.Log().Info("FSM: terminating", "reason", next.Reason, "state", f.currentState)
			return Stopped[any]()
		}

		// Check for transition
		from := f.currentState
		to := next.NextState

		f.currentState = to
		f.stateData = next.NextData

		if !reflect.DeepEqual(from, to) {
			ctx.Log().Info("FSM: state transition", "from", from, "to", to)
			for _, t := range f.transitions {
				t(from, to)
			}
		}

		// Handle timeouts via existing TimerScheduler
		if next.Timeout > 0 {
			ctx.Log().Debug("FSM: starting state timer", "duration", next.Timeout)
			ctx.Timers().StartSingleTimer("fsm-timeout", StateTimeout{}, next.Timeout)
		} else if !reflect.DeepEqual(from, to) {
			// Cancel timer only on transition to a state without timeout
			ctx.Log().Debug("FSM: cancelling state timer due to transition")
			ctx.Timers().Cancel("fsm-timeout")
		} else {
			// Same state, no new timeout: keep existing timer running (Akka FSM behavior)
			ctx.Log().Debug("FSM: keeping existing state timer")
		}

		return Same[any]()

	}
}

// StateTimeout is a sentinel message sent when an FSM state times out.
type StateTimeout struct{}

func (s StateTimeout) String() string {
	return "FSM.StateTimeout"
}

// Error formats an FSM related error.
func (f *FSM[S, D]) Error(msg string, args ...any) error {
	return fmt.Errorf("FSM[%T, %T]: %s", f.currentState, f.stateData, fmt.Sprintf(msg, args...))
}
