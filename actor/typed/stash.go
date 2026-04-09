/*
 * typed_stash.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

// StashBuffer provides temporary storage for messages that should not be
// processed by an actor's current behavior. It is created by the owning
// typed actor (TypedActor, persistent, or durable-state) and exposed via
// the typed context.
//
// Stash appends a message for later redelivery.
// UnstashAll redelivers every buffered message through the actor's normal
// dispatch path, in FIFO order, and empties the buffer.
type StashBuffer[T any] interface {
	// Stash adds msg to the buffer. Returns an error if the buffer is full.
	Stash(msg T) error

	// UnstashAll redelivers every buffered message in FIFO order.
	UnstashAll() error

	// Size returns the current number of buffered messages.
	Size() int

	// IsFull reports whether the buffer has reached its capacity.
	IsFull() bool

	// Clear drops all buffered messages without delivering them.
	Clear()
}
