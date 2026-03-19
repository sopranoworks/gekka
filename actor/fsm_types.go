/*
 * fsm_types.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"time"
)

// Event carries the message and current state data.
type Event[D any] struct {
	Msg  any
	Data D
}

// State represents the target state after a transition.
type State[S any, D any] struct {
	NextState S
	NextData  D
	Timeout   time.Duration
	Stop      bool
	Reason    Reason
	Unhandled bool
}

// Reason explains why the FSM stopped.
type Reason int

const (
	Normal Reason = iota
	Shutdown
	FSMFailure
)

// StopEvent is provided to the termination handler.
type StopEvent[S any, D any] struct {
	Reason     Reason
	FinalState S
	FinalData  D
}

// StateBuilder helps construct the next State using a fluent API.
type StateBuilder[S any, D any] struct {
	NextState S
	Data      D
	Timeout   time.Duration
	Stop      bool
	Reason    Reason
	Unhandled bool
}

func (b *StateBuilder[S, D]) Using(data D) *StateBuilder[S, D] {
	b.Data = data
	return b
}

func (b *StateBuilder[S, D]) ForMax(timeout time.Duration) *StateBuilder[S, D] {
	b.Timeout = timeout
	return b
}

func (b *StateBuilder[S, D]) Build() State[S, D] {
	return State[S, D]{
		NextState: b.NextState,
		NextData:  b.Data,
		Timeout:   b.Timeout,
		Stop:      b.Stop,
		Reason:    b.Reason,
		Unhandled: b.Unhandled,
	}
}
