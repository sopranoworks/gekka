/*
 * typed_stash.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"github.com/sopranoworks/gekka/actor"
)

// StashBuffer provides a temporary storage for messages that should not be
// processed by an actor's current behavior.
type StashBuffer[T any] interface {
	// Stash adds the current message to the stash.
	Stash() error

	// UnstashAll prepends all stashed messages to the actor's mailbox.
	UnstashAll() error

	// Clear removes all messages from the stash.
	Clear()
}

type stashBuffer[T any] struct {
	*actor.StashBufferImpl[T]
}

func newStashBuffer[T any](self actor.Ref, capacity int) *stashBuffer[T] {
	return &stashBuffer[T]{
		StashBufferImpl: actor.NewStashBuffer[T](self, capacity),
	}
}
