/*
 * persistence_signal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

// PersistenceSignal is the base interface for signals delivered to the
// [EventSourcedBehavior.SignalHandler].
type PersistenceSignal interface {
	isPersistenceSignal()
}

// RecoveryCompleted is delivered after recovery has finished, before the first
// command is processed.  HighestSequenceNr is the sequence number of the last
// recovered event (or 0 if no events were replayed).
type RecoveryCompleted struct {
	HighestSequenceNr uint64
}

func (RecoveryCompleted) isPersistenceSignal() {}

// SnapshotCompleted is delivered after a snapshot has been successfully saved.
type SnapshotCompleted struct {
	SequenceNr uint64
}

func (SnapshotCompleted) isPersistenceSignal() {}
