/*
 * journal.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package query

import (
	"github.com/sopranoworks/gekka/stream"
)

// Offset represents a position in the event stream.
type Offset interface {
	// IsAfter returns true if this offset is strictly after the other offset.
	IsAfter(other Offset) bool
}

// SequenceOffset is an offset based on a monotonically increasing sequence number.
type SequenceOffset int64

func (o SequenceOffset) IsAfter(other Offset) bool {
	if s, ok := other.(SequenceOffset); ok {
		return o > s
	}
	return true // SequenceOffset is after NoOffset
}

// TimeOffset is an offset based on a timestamp.
type TimeOffset int64 // unix nano

func (o TimeOffset) IsAfter(other Offset) bool {
	if t, ok := other.(TimeOffset); ok {
		return o > t
	}
	return true // TimeOffset is after NoOffset
}

// NoOffset represents the beginning of the stream.
type NoOffset struct{}

func (o NoOffset) IsAfter(other Offset) bool { return false }

// EventEnvelope wraps a persistent event with metadata.
//
// TraceContext carries the W3C TraceContext headers that were stored with the
// original PersistentRepr when the event was written.  Projections should
// extract this context to create child spans linked to the write-side trace,
// enabling a single TraceID to span the entire command-to-projection pipeline.
type EventEnvelope struct {
	Offset        Offset
	PersistenceID string
	SequenceNr    uint64
	Event         any
	Timestamp     int64
	TraceContext  map[string]string // W3C TraceContext headers propagated from the write side
}

// ReadJournal is the interface for querying events from a persistence journal.
type ReadJournal interface {
	// EventsByPersistenceId provides a stream of events for a specific persistent actor.
	// fromSequenceNr and toSequenceNr are inclusive.
	EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[EventEnvelope, stream.NotUsed]

	// EventsByTag provides a stream of events across different actors based on tags.
	EventsByTag(tag string, offset Offset) stream.Source[EventEnvelope, stream.NotUsed]
}
