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
	// SequenceOffset is after NoOffset
	if _, ok := other.(NoOffset); ok {
		return true
	}
	return false
}

// TimeOffset is an offset based on a timestamp (unix nano).
type TimeOffset int64

func (o TimeOffset) IsAfter(other Offset) bool {
	if t, ok := other.(TimeOffset); ok {
		return o > t
	}
	// TimeOffset is after NoOffset
	if _, ok := other.(NoOffset); ok {
		return true
	}
	return false
}

// NoOffset represents the beginning of the stream.
type NoOffset struct{}

func (o NoOffset) IsAfter(other Offset) bool { return false }

// EventEnvelope wraps a persistent event with metadata.
type EventEnvelope struct {
	Offset        Offset
	PersistenceID string
	SequenceNr    uint64
	Event         any
	Timestamp     int64
	TraceContext  map[string]string // W3C TraceContext headers propagated from the write side
}

// ReadJournal is the base interface for querying events from a persistence journal.
type ReadJournal interface {
	IsReadJournal()
}

// EventsByPersistenceIdQuery provides a stream of events for a specific persistent actor.
type EventsByPersistenceIdQuery interface {
	ReadJournal
	// EventsByPersistenceId provides a stream of events for a specific persistent actor.
	// fromSequenceNr and toSequenceNr are inclusive.
	// This query lives forever: as new events are appended, they are emitted to the source.
	EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[EventEnvelope, stream.NotUsed]
}

// EventsByTagQuery provides a stream of events across different actors based on tags.
type EventsByTagQuery interface {
	ReadJournal
	// EventsByTag provides a stream of events across different actors based on tags.
	// This query lives forever: as new events with the matching tag are committed,
	// they are emitted to the source.
	EventsByTag(tag string, offset Offset) stream.Source[EventEnvelope, stream.NotUsed]
}

// CurrentEventsByPersistenceIdQuery provides a non-streaming query for current events.
type CurrentEventsByPersistenceIdQuery interface {
	ReadJournal
	// CurrentEventsByPersistenceId provides a stream of events for a specific persistent actor
	// up to the highest sequence number currently stored. The source completes once
	// the current state is exhausted.
	CurrentEventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[EventEnvelope, stream.NotUsed]
}
