/*
 * eventsourced_batching_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// countingJournal wraps InMemoryJournal and records the size of each batch
// passed to AsyncWriteMessages so a test can assert how the Shard coalesces
// remember-entities events under
// pekko.cluster.sharding.event-sourced-remember-entities-store.max-updates-per-write.
type countingJournal struct {
	inner *persistence.InMemoryJournal
	mu    sync.Mutex
	calls [][]persistence.PersistentRepr
}

func newCountingJournal() *countingJournal {
	return &countingJournal{inner: persistence.NewInMemoryJournal()}
}

func (j *countingJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(persistence.PersistentRepr)) error {
	return j.inner.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
}

func (j *countingJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return j.inner.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

func (j *countingJournal) AsyncWriteMessages(ctx context.Context, messages []persistence.PersistentRepr) error {
	j.mu.Lock()
	cp := make([]persistence.PersistentRepr, len(messages))
	copy(cp, messages)
	j.calls = append(j.calls, cp)
	j.mu.Unlock()
	return j.inner.AsyncWriteMessages(ctx, messages)
}

func (j *countingJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return j.inner.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

func (j *countingJournal) writeCallCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.calls)
}

func (j *countingJournal) batchSizes() []int {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]int, len(j.calls))
	for i, b := range j.calls {
		out[i] = len(b)
	}
	return out
}

// Legacy path: max-updates-per-write = 0 → one journal write per event.
func TestEventSourcedBatching_LegacyOneWritePerEvent(t *testing.T) {
	j := newCountingJournal()
	settings := ShardSettings{
		RememberEntities:               true,
		Journal:                        j,
		EventSourcedMaxUpdatesPerWrite: 0,
	}
	shard, _ := newTestShard(t, "Order", "shard-0", settings)
	shard.PreStart()

	sendEnvelope(shard, "o-1", "create")
	sendEnvelope(shard, "o-2", "create")
	sendEnvelope(shard, "o-3", "create")

	if got := j.writeCallCount(); got != 3 {
		t.Fatalf("legacy: want 3 individual writes, got %d (sizes=%v)", got, j.batchSizes())
	}
	for i, sz := range j.batchSizes() {
		if sz != 1 {
			t.Errorf("legacy: batch %d had %d events, want 1", i, sz)
		}
	}
}

// Cap = 3 with 5 spawn events: a single full-batch write fires when the buffer
// hits the cap; the trailing 2 events stay buffered until PostStop.
func TestEventSourcedBatching_FlushesAtCap(t *testing.T) {
	j := newCountingJournal()
	settings := ShardSettings{
		RememberEntities:               true,
		Journal:                        j,
		EventSourcedMaxUpdatesPerWrite: 3,
	}
	shard, _ := newTestShard(t, "Order", "shard-0", settings)
	shard.PreStart()

	for _, id := range []EntityId{"o-1", "o-2", "o-3", "o-4", "o-5"} {
		sendEnvelope(shard, id, "create")
	}

	if got := j.writeCallCount(); got != 1 {
		t.Fatalf("after 5 events with cap=3: want 1 batch write, got %d (sizes=%v)", got, j.batchSizes())
	}
	if sz := j.batchSizes()[0]; sz != 3 {
		t.Fatalf("first batch: want 3 events, got %d", sz)
	}

	// Trailing 2 events still buffered — flushed on PostStop.
	shard.PostStop()
	if got := j.writeCallCount(); got != 2 {
		t.Fatalf("after PostStop: want 2 batch writes total, got %d (sizes=%v)", got, j.batchSizes())
	}
	if sz := j.batchSizes()[1]; sz != 2 {
		t.Fatalf("trailing batch: want 2 events, got %d", sz)
	}
}

// Cap = 10 with 4 events, all of which sit in the buffer until PostStop, which
// flushes them in a single write.
func TestEventSourcedBatching_FlushOnPostStop(t *testing.T) {
	j := newCountingJournal()
	settings := ShardSettings{
		RememberEntities:               true,
		Journal:                        j,
		EventSourcedMaxUpdatesPerWrite: 10,
	}
	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	for _, id := range []EntityId{"c-1", "c-2", "c-3", "c-4"} {
		sendEnvelope(shard, id, "create")
	}
	if got := j.writeCallCount(); got != 0 {
		t.Fatalf("before PostStop: want 0 writes, got %d", got)
	}

	shard.PostStop()
	if got := j.writeCallCount(); got != 1 {
		t.Fatalf("after PostStop: want 1 batch write, got %d", got)
	}
	if sz := j.batchSizes()[0]; sz != 4 {
		t.Fatalf("batch: want 4 events, got %d", sz)
	}
}

// Compile-time guard: countingJournal satisfies persistence.Journal.
var _ persistence.Journal = (*countingJournal)(nil)

// Quiet unused-import lint when actor isn't directly referenced — the helpers
// in sharding_advanced_test.go already pull it in.
var _ = actor.Ref(nil)
