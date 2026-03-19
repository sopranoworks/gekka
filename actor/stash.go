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

type StashBufferImpl[T any] struct {
	self     Ref
	Messages []Envelope
	capacity int
}

func NewStashBuffer[T any](self Ref, capacity int) *StashBufferImpl[T] {
	return &StashBufferImpl[T]{
		self:     self,
		capacity: capacity,
	}
}

func (s *StashBufferImpl[T]) Stash() error {
	if len(s.Messages) >= s.capacity {
		return fmt.Errorf("stash capacity exceeded (%d)", s.capacity)
	}
	// Placeholder: In a real system, we'd need access to the current envelope.
	return nil
}

func (s *StashBufferImpl[T]) UnstashAll() error {
	// Placeholder: Prepend messages to mailbox.
	return nil
}

func (s *StashBufferImpl[T]) Clear() {
	s.Messages = nil
}
