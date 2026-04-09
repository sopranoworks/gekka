/*
 * stash.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
)

// DefaultStashCapacity is the default maximum number of messages that can be stashed.
const DefaultStashCapacity = 1024

// StashBufferImpl is a bounded FIFO buffer of typed messages with a redeliver
// hook. It matches Pekko typed's StashBuffer semantics:
//
//   - Stash(msg) appends the message to the buffer.
//   - UnstashAll() delivers every buffered message, in FIFO order, by invoking
//     the redeliver callback supplied at construction. The buffer is emptied
//     as a side effect.
//   - The redeliver callback is expected to route the message back through
//     the owning actor's dispatch path (typically by pushing onto an internal
//     pending slice that the actor's Receive loop drains).
//
// StashBufferImpl is not safe for concurrent access. It is intended to be
// owned by a single actor and called only from within that actor's Receive
// goroutine.
type StashBufferImpl[T any] struct {
	messages  []T
	capacity  int
	redeliver func(T)
}

// NewStashBuffer constructs a StashBufferImpl. The redeliver callback must not
// be nil. It is invoked synchronously, once per message, during UnstashAll in
// FIFO order.
func NewStashBuffer[T any](capacity int, redeliver func(T)) *StashBufferImpl[T] {
	if redeliver == nil {
		panic("actor.NewStashBuffer: redeliver callback must not be nil")
	}
	return &StashBufferImpl[T]{
		capacity:  capacity,
		redeliver: redeliver,
	}
}

// Stash appends msg to the buffer. It returns an error if the buffer is full;
// in that case the buffer is unmodified.
func (s *StashBufferImpl[T]) Stash(msg T) error {
	if len(s.messages) >= s.capacity {
		return fmt.Errorf("stash capacity exceeded (%d)", s.capacity)
	}
	s.messages = append(s.messages, msg)
	return nil
}

// UnstashAll delivers every buffered message via the redeliver callback in
// FIFO order, then clears the buffer. Messages stashed *during* a redeliver
// call land in the buffer and remain for the next UnstashAll.
func (s *StashBufferImpl[T]) UnstashAll() error {
	if len(s.messages) == 0 {
		return nil
	}
	// Snapshot and clear first, so that messages re-stashed by redeliver
	// callbacks do not get delivered in this pass.
	pending := s.messages
	s.messages = nil
	for _, msg := range pending {
		s.redeliver(msg)
	}
	return nil
}

// Size returns the number of messages currently in the buffer.
func (s *StashBufferImpl[T]) Size() int {
	return len(s.messages)
}

// IsFull reports whether the buffer has reached its capacity.
func (s *StashBufferImpl[T]) IsFull() bool {
	return len(s.messages) >= s.capacity
}

// Clear drops all buffered messages without delivering them.
func (s *StashBufferImpl[T]) Clear() {
	s.messages = nil
}
