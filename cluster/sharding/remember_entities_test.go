/*
 * remember_entities_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"path/filepath"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// TestFileStore_Recovery is the primary Phase 4 integration test.
//
// Scenario:
//  1. Start a Shard backed by a FileStore.
//  2. Send messages to 3 different entity IDs → all 3 are spawned and recorded.
//  3. "Stop" the shard (simulate crash — no PreStop bookkeeping).
//  4. Create a NEW Shard pointing at the same FileStore path.
//  5. Call PreStart on the new shard.
//  6. Assert all 3 entities are automatically re-spawned without any external
//     messages, verifying persistence and recovery.
func TestFileStore_Recovery(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "entities.json")
	fs, err := NewFileStore(storePath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	settings := ShardSettings{
		RememberEntities: true,
		Store:            fs,
	}

	// ── Phase 1: spawn 3 entities ─────────────────────────────────────────
	shard1, _ := newTestShard(t, "Order", "shard-0", settings)
	shard1.PreStart()

	sendEnvelope(shard1, "order-1", "create")
	sendEnvelope(shard1, "order-2", "create")
	sendEnvelope(shard1, "order-3", "create")

	if got := len(shard1.entities); got != 3 {
		t.Fatalf("phase 1: expected 3 entities spawned, got %d", got)
	}

	// Verify the store captured all 3 entity IDs.
	stored, err := fs.GetEntities("shard-0")
	if err != nil {
		t.Fatalf("GetEntities: %v", err)
	}
	if got := len(stored); got != 3 {
		t.Fatalf("phase 1: expected 3 entities in FileStore, got %d", got)
	}

	// ── Phase 2: new shard — recovery from FileStore ──────────────────────
	shard2, mctx2 := newTestShard(t, "Order", "shard-0", settings)
	shard2.PreStart()

	if got := len(shard2.entities); got != 3 {
		t.Fatalf("phase 2: expected 3 entities recovered, got %d", got)
	}
	for _, id := range []EntityId{"order-1", "order-2", "order-3"} {
		if _, ok := shard2.entities[id]; !ok {
			t.Errorf("phase 2: entity %q not recovered", id)
		}
	}

	// Confirm entityCreator was called for each recovered entity.
	if got := len(mctx2.created); got != 3 {
		t.Errorf("phase 2: expected 3 actor creations during recovery, got %d", got)
	}
}

// TestFileStore_PassivationKeepsEntity verifies that passivating an entity does
// NOT remove it from the FileStore, so the entity is recovered after a restart.
func TestFileStore_PassivationKeepsEntity(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "entities.json")
	fs, _ := NewFileStore(storePath)

	settings := ShardSettings{
		RememberEntities: true,
		Store:            fs,
	}

	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	sendEnvelope(shard, "cart-1", "addItem")
	entityRef := shard.entities["cart-1"]

	// Passivate — entity removed from in-memory map but MUST stay in store.
	shard.Receive(actor.Passivate{Entity: entityRef})

	if _, ok := shard.entities["cart-1"]; ok {
		t.Fatal("entity should be removed from shard after passivation")
	}

	// Store still contains the entity.
	stored, _ := fs.GetEntities("shard-0")
	found := false
	for _, id := range stored {
		if id == "cart-1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("passivated entity must remain in FileStore")
	}

	// Recovery re-spawns the passivated entity.
	shard2, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard2.PreStart()

	if _, ok := shard2.entities["cart-1"]; !ok {
		t.Error("passivated entity must be recovered on shard restart")
	}
}

// TestFileStore_TerminatedRemovesEntity verifies that an actor.TerminatedMessage
// removes the entity from the FileStore, preventing re-spawn after restart.
func TestFileStore_TerminatedRemovesEntity(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "entities.json")
	fs, _ := NewFileStore(storePath)

	settings := ShardSettings{
		RememberEntities: true,
		Store:            fs,
	}

	shard, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard.PreStart()

	sendEnvelope(shard, "cart-A", "create")
	sendEnvelope(shard, "cart-B", "create")

	// Explicitly terminate cart-A via a TerminatedMessage.
	entityRef := shard.entities["cart-A"]
	shard.Receive(terminatedMsg{ref: entityRef})

	// cart-A should be gone from both shard and store.
	if _, ok := shard.entities["cart-A"]; ok {
		t.Error("explicitly terminated entity must be removed from shard")
	}
	stored, _ := fs.GetEntities("shard-0")
	for _, id := range stored {
		if id == "cart-A" {
			t.Error("explicitly terminated entity must be removed from FileStore")
		}
	}

	// cart-B is unaffected.
	if _, ok := shard.entities["cart-B"]; !ok {
		t.Error("cart-B must still be alive")
	}

	// Recovery: only cart-B is re-spawned.
	shard2, _ := newTestShard(t, "Cart", "shard-0", settings)
	shard2.PreStart()

	if _, ok := shard2.entities["cart-B"]; !ok {
		t.Error("cart-B must be recovered after restart")
	}
	if _, ok := shard2.entities["cart-A"]; ok {
		t.Error("explicitly terminated cart-A must NOT be recovered")
	}
}

// TestFileStore_AutoCreateFromJournalStorePath verifies that setting
// JournalStorePath (without an explicit Store) auto-creates a FileStore and
// performs recovery on a second shard.
func TestFileStore_AutoCreateFromJournalStorePath(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "auto.json")

	settings := ShardSettings{
		RememberEntities: true,
		JournalStorePath: storePath,
	}

	shard1, _ := newTestShard(t, "Product", "shard-0", settings)
	shard1.PreStart()

	sendEnvelope(shard1, "prod-X", "create")
	sendEnvelope(shard1, "prod-Y", "create")

	if got := len(shard1.entities); got != 2 {
		t.Fatalf("expected 2 entities, got %d", got)
	}

	// New shard with same JournalStorePath should recover both.
	shard2, _ := newTestShard(t, "Product", "shard-0", settings)
	shard2.PreStart()

	if got := len(shard2.entities); got != 2 {
		t.Fatalf("expected 2 recovered entities, got %d", got)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// terminatedMsg is a minimal actor.TerminatedMessage implementation for tests.
type terminatedMsg struct {
	ref actor.Ref
}

func (t terminatedMsg) TerminatedActor() actor.Ref { return t.ref }
