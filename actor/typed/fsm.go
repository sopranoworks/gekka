/*
 * fsm.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"fmt"
	"reflect"

	"github.com/sopranoworks/gekka/actor"
)

// FSM is a finite state machine implementation for typed actors.
// S is the state type (must be comparable), D is the state data type.
type FSM[S comparable, D any] struct {
	currentState S
	stateData    D
	timers       TimerScheduler[any]

	handlers           map[S]func(actor.Event[D]) actor.State[S, D]
	unhandledHandler   func(actor.Event[D]) actor.State[S, D]
	transitions        []func(S, S)
	terminationHandler func(actor.StopEvent[S, D])

	initialized bool
}

// NewFSM creates a new FSM instance.
func NewFSM[S comparable, D any]() *FSM[S, D] {
	return &FSM[S, D]{
		handlers: make(map[S]func(actor.Event[D]) actor.State[S, D]),
	}
}

// Behavior returns the actor behavior that drives this FSM.
func (f *FSM[S, D]) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		if !f.initialized {
			f.timers = ctx.Timers()
			f.initialized = true
			ctx.Log().Info("FSM: initialized", "state", f.currentState)
		}

		handler, ok := f.handlers[f.currentState]
		event := actor.Event[D]{Msg: msg, Data: f.stateData}
		var next actor.State[S, D]

		if ok {
			next = handler(event)
		}

		if !ok || next.Unhandled {
			if f.unhandledHandler != nil {
				next = f.unhandledHandler(event)
			} else if next.Unhandled {
				// Truly unhandled — keep current state
				return Same[any]()
			} else {
				ctx.Log().Warn("FSM: unhandled message", "state", f.currentState, "msgType", reflect.TypeOf(msg))
				return Same[any]()
			}
		}

		// Handle Stop
		if next.Stop {
			f.currentState = next.NextState
			f.stateData = next.NextData
			if f.terminationHandler != nil {
				f.terminationHandler(actor.StopEvent[S, D]{
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

		if from != to {
			ctx.Log().Info("FSM: state transition", "from", from, "to", to)
			for _, t := range f.transitions {
				t(from, to)
			}
		}

		// Handle timeouts
		if next.Timeout > 0 {
			f.timers.StartSingleTimer("fsm-timeout", actor.StateTimeout{}, next.Timeout)
		} else if from != to {
			f.timers.Cancel("fsm-timeout")
		}

		return Same[any]()
	}
}

// DSL Methods using actor.StateBuilder

func (f *FSM[S, D]) StartWith(state S, data D) {
	f.currentState = state
	f.stateData = data
}

func (f *FSM[S, D]) When(state S, handler func(actor.Event[D]) actor.State[S, D]) {
	f.handlers[state] = handler
}

func (f *FSM[S, D]) WhenUnhandled(handler func(actor.Event[D]) actor.State[S, D]) {
	f.unhandledHandler = handler
}

func (f *FSM[S, D]) OnTransition(handler func(from S, to S)) {
	f.transitions = append(f.transitions, handler)
}

func (f *FSM[S, D]) OnTermination(handler func(actor.StopEvent[S, D])) {
	f.terminationHandler = handler
}

func (f *FSM[S, D]) Goto(nextState S) *actor.StateBuilder[S, D] {
	return &actor.StateBuilder[S, D]{NextState: nextState, Data: f.stateData}
}

func (f *FSM[S, D]) Stay() *actor.StateBuilder[S, D] {
	return &actor.StateBuilder[S, D]{NextState: f.currentState, Data: f.stateData}
}

func (f *FSM[S, D]) Unhandled() *actor.StateBuilder[S, D] {
	return &actor.StateBuilder[S, D]{NextState: f.currentState, Data: f.stateData, Unhandled: true}
}

func (f *FSM[S, D]) Stop() *actor.StateBuilder[S, D] {
	return &actor.StateBuilder[S, D]{NextState: f.currentState, Data: f.stateData, Stop: true, Reason: actor.Normal}
}

// Error formats an FSM related error.
func (f *FSM[S, D]) Error(msg string, args ...any) error {
	return fmt.Errorf("FSM[%T, %T]: %s", f.currentState, f.stateData, fmt.Sprintf(msg, args...))
}
