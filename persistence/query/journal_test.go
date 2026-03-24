/*
 * journal_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package query

import (
	"testing"

	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
)

type mockReadJournal struct{}

func (m *mockReadJournal) EventsByPersistenceId(persistenceId string, fromSequenceNr, toSequenceNr int64) stream.Source[EventEnvelope, stream.NotUsed] {
	events := []EventEnvelope{
		{Offset: SequenceOffset(1), PersistenceID: persistenceId, SequenceNr: 1, Event: "event-1"},
		{Offset: SequenceOffset(2), PersistenceID: persistenceId, SequenceNr: 2, Event: "event-2"},
	}
	return stream.FromSlice(events)
}

func (m *mockReadJournal) EventsByTag(tag string, offset Offset) stream.Source[EventEnvelope, stream.NotUsed] {
	events := []EventEnvelope{
		{Offset: SequenceOffset(1), PersistenceID: "p1", SequenceNr: 1, Event: "event-1"},
		{Offset: SequenceOffset(2), PersistenceID: "p2", SequenceNr: 1, Event: "event-2"},
	}
	return stream.FromSlice(events)
}

func TestReadJournalInterface(t *testing.T) {
	var rj ReadJournal = &mockReadJournal{}

	src := rj.EventsByPersistenceId("p1", 1, 2)
	assert.NotNil(t, src)

	srcTag := rj.EventsByTag("tag1", NoOffset{})
	assert.NotNil(t, srcTag)
}
