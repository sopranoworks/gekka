/*
 * eventsourced_store_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"sort"
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/persistence"
)

// TestEventSourcedEntityStore_RoundTrip exercises the ShardStore contract
// directly: AddEntity persists EntityStarted, RemoveEntity persists
// EntityStopped, and a brand-new store instance built on the same journal
// recovers the active set via replay.
func TestEventSourcedEntityStore_RoundTrip(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	store := NewEventSourcedEntityStore(journal, "Order", 0)

	if err := store.AddEntity("shard-0", "order-1"); err != nil {
		t.Fatalf("AddEntity order-1: %v", err)
	}
	if err := store.AddEntity("shard-0", "order-2"); err != nil {
		t.Fatalf("AddEntity order-2: %v", err)
	}
	if err := store.AddEntity("shard-0", "order-3"); err != nil {
		t.Fatalf("AddEntity order-3: %v", err)
	}
	if err := store.RemoveEntity("shard-0", "order-2"); err != nil {
		t.Fatalf("RemoveEntity order-2: %v", err)
	}

	got, err := store.GetEntities("shard-0")
	if err != nil {
		t.Fatalf("GetEntities: %v", err)
	}
	sort.Strings(got)
	want := []EntityId{"order-1", "order-3"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("active set: got %v, want %v", got, want)
	}

	// Simulate a process restart by building a fresh store on the same
	// journal — replay must reproduce the active set.
	revived := NewEventSourcedEntityStore(journal, "Order", 0)
	revived2, err := revived.GetEntities("shard-0")
	if err != nil {
		t.Fatalf("revived GetEntities: %v", err)
	}
	sort.Strings(revived2)
	if len(revived2) != len(want) || revived2[0] != want[0] || revived2[1] != want[1] {
		t.Fatalf("post-restart set: got %v, want %v", revived2, want)
	}
}

// TestEventSourcedEntityStore_BatchesAtCap verifies max-updates-per-write
// coalesces consecutive writes and that Flush drains the trailing buffer.
func TestEventSourcedEntityStore_BatchesAtCap(t *testing.T) {
	j := newCountingJournal()
	store := NewEventSourcedEntityStore(j, "Cart", 3)

	for _, id := range []EntityId{"c-1", "c-2", "c-3", "c-4", "c-5"} {
		if err := store.AddEntity("shard-0", id); err != nil {
			t.Fatalf("AddEntity %s: %v", id, err)
		}
	}

	if got := j.writeCallCount(); got != 1 {
		t.Fatalf("after 5 adds with cap=3: want 1 batch write, got %d (sizes=%v)", got, j.batchSizes())
	}
	if sz := j.batchSizes()[0]; sz != 3 {
		t.Fatalf("first batch: want 3 events, got %d", sz)
	}

	if err := store.Flush("shard-0"); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := j.writeCallCount(); got != 2 {
		t.Fatalf("after Flush: want 2 batch writes total, got %d (sizes=%v)", got, j.batchSizes())
	}
	if sz := j.batchSizes()[1]; sz != 2 {
		t.Fatalf("trailing batch: want 2 events, got %d", sz)
	}
}

// TestEventSourcedEntityStore_PassivationKeepsEntity drives a Shard with the
// EventSourcedEntityStore wired in and verifies passivation does NOT remove
// the entity from the journal — a brand-new Shard on the same store re-spawns
// the passivated entity, matching Pekko's eventsourced backend semantics.
func TestEventSourcedEntityStore_PassivationKeepsEntity(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	store := NewEventSourcedEntityStore(journal, "Cart", 0)

	settings := ShardSettings{
		RememberEntities: true,
		Store:            store,
	}

	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	sendEnvelope(shard, "cart-1", "addItem")
	entityRef := shard.entities["cart-1"]
	shard.Receive(actor.Passivate{Entity: entityRef})
	shard.PostStop()

	if _, ok := shard.entities["cart-1"]; ok {
		t.Fatal("entity should be removed from shard after passivation")
	}
	stored, err := store.GetEntities("shard-0")
	if err != nil {
		t.Fatalf("GetEntities: %v", err)
	}
	if len(stored) != 1 || stored[0] != "cart-1" {
		t.Fatalf("passivated entity must remain in store, got %v", stored)
	}

	// New Shard sharing the same store — passivated entity is recovered.
	shard2, _ := newTestShard(t, "Cart", "shard-0", ShardSettings{
		RememberEntities: true,
		Store:            store,
	})
	shard2.PreStart()
	if _, ok := shard2.entities["cart-1"]; !ok {
		t.Error("passivated entity must be recovered on shard restart")
	}
}

// TestEventSourcedEntityStore_TerminatedRemovesEntity verifies an explicit
// TerminatedMessage drops the entity from the journal so it is NOT recovered
// on restart.
func TestEventSourcedEntityStore_TerminatedRemovesEntity(t *testing.T) {
	journal := persistence.NewInMemoryJournal()
	store := NewEventSourcedEntityStore(journal, "Cart", 0)

	settings := ShardSettings{
		RememberEntities: true,
		Store:            store,
	}

	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	sendEnvelope(shard, "cart-A", "create")
	sendEnvelope(shard, "cart-B", "create")

	entityRef := shard.entities["cart-A"]
	shard.Receive(terminatedMsg{ref: entityRef})
	shard.PostStop()

	stored, _ := store.GetEntities("shard-0")
	for _, id := range stored {
		if id == "cart-A" {
			t.Error("explicitly terminated entity must be removed from store")
		}
	}

	shard2, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard2.PreStart()
	if _, ok := shard2.entities["cart-A"]; ok {
		t.Error("explicitly terminated cart-A must NOT be recovered")
	}
	if _, ok := shard2.entities["cart-B"]; !ok {
		t.Error("cart-B must be recovered after restart")
	}
}

// TestEventSourcedEntityStore_FlushOnPostStop verifies Shard.PostStop drains
// the store's pending buffer when EventSourcedMaxUpdatesPerWrite > 0.
func TestEventSourcedEntityStore_FlushOnPostStop(t *testing.T) {
	j := newCountingJournal()
	store := NewEventSourcedEntityStore(j, "Order", 10)

	settings := ShardSettings{
		RememberEntities: true,
		Store:            store,
	}
	shard, _ := newTestShard(t, "Order", "shard-0", settings)
	shard.PreStart()

	for _, id := range []EntityId{"o-1", "o-2", "o-3", "o-4"} {
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
