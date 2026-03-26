/*
 * query_journal_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

//go:build spanner

package spannerstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/persistence"
	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	spannerstore "github.com/sopranoworks/gekka-extensions-persistence-spanner"
	"github.com/stretchr/testify/assert"
)

func TestSpanner_ReadJournal_EventsByTag(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	journal := spannerstore.NewSpannerJournal(client, codec)
	readJournal := spannerstore.NewSpannerReadJournal(client, codec)
	ctx := context.Background()

	// Write events from multiple actors with the same tag
	const tag = "color-green"
	pids := []string{"actor-A", "actor-B", "actor-C"}

	for _, pid := range pids {
		events := []persistence.PersistentRepr{
			{PersistenceID: pid, SequenceNr: 1, Payload: Evt{V: 100}, Tags: []string{tag}},
		}
		if err := journal.AsyncWriteMessages(ctx, events); err != nil {
			t.Fatalf("AsyncWriteMessages for %s: %v", pid, err)
		}
		// Small sleep to ensure distinct commit timestamps if necessary
		time.Sleep(100 * time.Millisecond)
	}

	// Query events by tag
	src := readJournal.EventsByTag(tag, query.NoOffset{})
	
	// Collect events from the source.
	// Since it's a live stream, we'll take exactly 3 and then stop.
	m := stream.SyncMaterializer{}
	collected, err := stream.RunWith(src.Take(3), stream.Collect[query.EventEnvelope](), m)
	if err != nil {
		t.Fatalf("RunWith EventsByTag: %v", err)
	}

	assert.Equal(t, 3, len(collected))
	
	// Verify global ordering by checking commit timestamps (Offsets)
	for i := 0; i < len(collected)-1; i++ {
		assert.True(t, collected[i+1].Offset.IsAfter(collected[i].Offset), 
			"Event %d should be after event %d", i+1, i)
	}

	// Verify the actors and values
	for i, pid := range pids {
		assert.Equal(t, pid, collected[i].PersistenceID)
		assert.Equal(t, Evt{V: 100}, collected[i].Event)
	}
}

func TestSpanner_ReadJournal_EventsByPersistenceId(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	journal := spannerstore.NewSpannerJournal(client, codec)
	readJournal := spannerstore.NewSpannerReadJournal(client, codec)
	ctx := context.Background()

	const pid = "actor-streaming-1"
	for i := 1; i <= 5; i++ {
		events := []persistence.PersistentRepr{
			{PersistenceID: pid, SequenceNr: uint64(i), Payload: Evt{V: i}},
		}
		if err := journal.AsyncWriteMessages(ctx, events); err != nil {
			t.Fatalf("AsyncWriteMessages: %v", err)
		}
	}

	src := readJournal.EventsByPersistenceId(pid, 1, 5)
	m := stream.SyncMaterializer{}
	collected, err := stream.RunWith(src, stream.Collect[query.EventEnvelope](), m)
	if err != nil {
		t.Fatalf("RunWith EventsByPersistenceId: %v", err)
	}

	assert.Equal(t, 5, len(collected))
	for i := 0; i < 5; i++ {
		assert.Equal(t, uint64(i+1), collected[i].SequenceNr)
		assert.Equal(t, pid, collected[i].PersistenceID)
		assert.Equal(t, Evt{V: i + 1}, collected[i].Event)
	}
}

func TestSpanner_ReadJournal_CurrentEventsByPersistenceId(t *testing.T) {
	f := getFixture(t)
	client := newClient(t, f)

	codec := spannerstore.NewJSONCodec()
	codec.Register(Evt{})

	journal := spannerstore.NewSpannerJournal(client, codec)
	readJournal := spannerstore.NewSpannerReadJournal(client, codec)
	ctx := context.Background()

	const pid = "actor-current-1"
	for i := 1; i <= 3; i++ {
		events := []persistence.PersistentRepr{
			{PersistenceID: pid, SequenceNr: uint64(i), Payload: Evt{V: i}},
		}
		if err := journal.AsyncWriteMessages(ctx, events); err != nil {
			t.Fatalf("AsyncWriteMessages: %v", err)
		}
	}

	src := readJournal.CurrentEventsByPersistenceId(pid, 1, 3)
	m := stream.SyncMaterializer{}
	collected, err := stream.RunWith(src, stream.Collect[query.EventEnvelope](), m)
	if err != nil {
		t.Fatalf("RunWith CurrentEventsByPersistenceId: %v", err)
	}

	assert.Equal(t, 3, len(collected))
	assert.Equal(t, uint64(1), collected[0].SequenceNr)
	assert.Equal(t, uint64(2), collected[1].SequenceNr)
	assert.Equal(t, uint64(3), collected[2].SequenceNr)
}
