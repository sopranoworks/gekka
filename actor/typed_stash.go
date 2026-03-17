/*
 * typed_stash.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

// DefaultStashCapacity is the default maximum number of messages a StashBuffer
// will hold. Override this value during system initialization to configure the
// default for all newly created actors.
//
// For HOCON-based configuration, read "gekka.actor.typed.stash-capacity" and
// call SetDefaultStashCapacity before any actors that use Stash() are started.
var DefaultStashCapacity = 100

// SetDefaultStashCapacity updates DefaultStashCapacity. Must be called before
// actors that use Stash() are created; has no effect on already-running actors.
func SetDefaultStashCapacity(n int) {
	if n > 0 {
		DefaultStashCapacity = n
	}
}

// StashBuffer provides a bounded FIFO buffer for temporarily holding messages
// that should not be processed in the actor's current behavior.
// Buffered messages are delivered to the actor's mailbox via UnstashAll,
// typically during a behavior transition.
type StashBuffer[T any] interface {
	// Stash appends msg to the buffer. Returns true if successful, false if
	// the buffer has reached its capacity.
	Stash(msg T) bool

	// UnstashAll sends all buffered messages to the actor's mailbox in FIFO
	// order and clears the buffer. Messages are processed after the current
	// Receive call returns.
	UnstashAll()

	// Clear discards all buffered messages without delivering them.
	Clear()

	// Size returns the number of messages currently buffered.
	Size() int
}

type stashBuffer[T any] struct {
	buf      []T
	capacity int
	self     Ref
}

func newStashBuffer[T any](self Ref, capacity int) *stashBuffer[T] {
	if capacity <= 0 {
		capacity = DefaultStashCapacity
	}
	return &stashBuffer[T]{
		buf:      make([]T, 0, min(capacity, 64)),
		capacity: capacity,
		self:     self,
	}
}

func (s *stashBuffer[T]) Stash(msg T) bool {
	if len(s.buf) >= s.capacity {
		return false
	}
	s.buf = append(s.buf, msg)
	return true
}

func (s *stashBuffer[T]) UnstashAll() {
	msgs := s.buf
	s.buf = s.buf[:0]
	for _, msg := range msgs {
		s.self.Tell(msg)
	}
}

func (s *stashBuffer[T]) Clear() {
	s.buf = s.buf[:0]
}

func (s *stashBuffer[T]) Size() int {
	return len(s.buf)
}
