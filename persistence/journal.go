/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
)

// PersistentRepr is an envelope for a persistent event.
type PersistentRepr struct {
	PersistenceID string
	SequenceNr    uint64
	Payload       any
	Deleted       bool
	SenderPath    string
}

// Journal is the interface for storing and replaying events.
type Journal interface {
	// ReplayMessages replays messages from fromSequenceNr to toSequenceNr (inclusive)
	// for the given persistenceId. The callback is invoked for each message.
	ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error

	// ReadHighestSequenceNr returns the highest sequence number stored for the given persistenceId.
	ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error)

	// AsyncWriteMessages asynchronously stores a batch of messages.
	AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error

	// AsyncDeleteMessagesTo deletes all messages up to (and including) the given sequenceNr.
	AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error
}
