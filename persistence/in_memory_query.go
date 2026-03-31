/*
 * in_memory_query.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
)

// InMemoryReadJournal wraps an InMemoryJournal and provides EventsByTag query
// support for testing.  It is a finite (non-live) read journal: EventsByTag
// completes once all currently stored events with the given tag have been
// yielded.
type InMemoryReadJournal struct {
	journal *InMemoryJournal
}

// NewInMemoryReadJournal creates an InMemoryReadJournal backed by j.
// The caller must use the same *InMemoryJournal instance for both writes
// (via Journal.AsyncWriteMessages) and reads.
func NewInMemoryReadJournal(j *InMemoryJournal) *InMemoryReadJournal {
	return &InMemoryReadJournal{journal: j}
}

func (r *InMemoryReadJournal) IsReadJournal() {}

// EventsByTag returns a finite stream of all events currently in the journal
// whose Tags slice contains tag, in insertion order, starting after offset.
// For SequenceOffset(n), events at global positions > n are included.
// For NoOffset or any other Offset type, all matching events are returned.
func (r *InMemoryReadJournal) EventsByTag(tag string, offset query.Offset) stream.Source[query.EventEnvelope, stream.NotUsed] {
	startPos := 0
	if so, ok := offset.(query.SequenceOffset); ok {
		startPos = int(so)
	}

	// Capture a snapshot of the current global log to avoid holding a lock
	// across the entire stream lifetime.
	r.journal.mu.RLock()
	snapshot := make([]PersistentRepr, len(r.journal.globalLog))
	copy(snapshot, r.journal.globalLog)
	r.journal.mu.RUnlock()

	pos := startPos
	return stream.FromIteratorFunc(func() (query.EventEnvelope, bool, error) {
		for pos < len(snapshot) {
			msg := snapshot[pos]
			globalPos := pos + 1 // 1-based ordering for SequenceOffset
			pos++
			for _, t := range msg.Tags {
				if t == tag {
					return query.EventEnvelope{
						Offset:        query.SequenceOffset(globalPos),
						PersistenceID: msg.PersistenceID,
						SequenceNr:    msg.SequenceNr,
						Event:         msg.Payload,
					}, true, nil
				}
			}
		}
		return query.EventEnvelope{}, false, nil
	})
}

var (
	_ query.ReadJournal    = (*InMemoryReadJournal)(nil)
	_ query.EventsByTagQuery = (*InMemoryReadJournal)(nil)
)
