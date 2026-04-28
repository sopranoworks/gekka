/*
 * durable_replicator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"testing"
)

// TestReplicator_PersistsOnMutate proves the durable hook fires from
// the local mutate paths whenever the key matches DurableKeys.  This
// is the contract the cluster.go wiring depends on for S22's BoltDB
// backend to pick up writes without further plumbing.
func TestReplicator_PersistsOnMutate(t *testing.T) {
	store := NewMemoryDurableStore()
	r := NewReplicator("nodeA", nil)
	r.DurableEnabled = true
	r.DurableStore = store
	r.DurableKeys = []string{"shard-*"}

	r.IncrementCounter("shard-Counter-1", 5, WriteLocal)
	r.AddToSet("shard-Set-1", "x", WriteLocal)
	r.PutInMap("shard-Map-1", "k", "v", WriteLocal)

	r.IncrementCounter("hits", 1, WriteLocal)

	if got, want := store.Len(), 3; got != want {
		t.Fatalf("Len = %d, want %d (durable keys only)", got, want)
	}
	for _, k := range []string{"shard-Counter-1", "shard-Set-1", "shard-Map-1"} {
		if _, err := store.Load(context.Background(), k); err != nil {
			t.Errorf("Load(%s): %v", k, err)
		}
	}
}

// TestReplicator_NoStoreWhenDisabled covers the negative path: even with
// a DurableStore plugged in, DurableEnabled=false short-circuits writes.
// The host wiring relies on this so a config that omits durable.enabled
// behaves identically to the legacy in-memory replicator.
func TestReplicator_NoStoreWhenDisabled(t *testing.T) {
	store := NewMemoryDurableStore()
	r := NewReplicator("nodeA", nil)
	r.DurableEnabled = false
	r.DurableStore = store
	r.DurableKeys = []string{"shard-*"}

	r.IncrementCounter("shard-Counter-1", 1, WriteLocal)
	if got := store.Len(); got != 0 {
		t.Errorf("Len with DurableEnabled=false = %d, want 0", got)
	}
}

// TestReplicator_RecoverRehydrates verifies a node restart cycle: write
// state, simulate process death by creating a fresh Replicator pointing
// at the same store, and prove Recover repopulates the in-memory CRDTs.
func TestReplicator_RecoverRehydrates(t *testing.T) {
	store := NewMemoryDurableStore()

	r1 := NewReplicator("nodeA", nil)
	r1.DurableEnabled = true
	r1.DurableStore = store
	r1.DurableKeys = []string{"shard-*"}
	r1.IncrementCounter("shard-Counter-1", 7, WriteLocal)
	r1.AddToSet("shard-Set-1", "x", WriteLocal)
	r1.AddToSet("shard-Set-1", "y", WriteLocal)

	r2 := NewReplicator("nodeA", nil)
	r2.DurableEnabled = true
	r2.DurableStore = store
	r2.DurableKeys = []string{"shard-*"}
	if err := r2.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if v := r2.GCounter("shard-Counter-1").Value(); v != 7 {
		t.Errorf("recovered counter = %d, want 7", v)
	}
	if got := r2.ORSet("shard-Set-1").Elements(); !containsAll(got, []string{"x", "y"}) {
		t.Errorf("recovered set = %v, want [x y]", got)
	}
}

// TestReplicator_RecoverNoOpWhenDisabled keeps Recover silent when the
// caller has not opted in to durability.  Equivalent to a fresh start.
func TestReplicator_RecoverNoOpWhenDisabled(t *testing.T) {
	store := NewMemoryDurableStore()
	_ = store.Store(context.Background(), DurableEntry{
		Key: "shard-Counter-1", Type: CRDTTypeGCounter,
		Payload: []byte(`{"state":{"nodeA":99}}`),
	})

	r := NewReplicator("nodeA", nil)
	r.DurableEnabled = false
	r.DurableStore = store
	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := r.LookupGCounter("shard-Counter-1"); ok {
		t.Errorf("Recover with DurableEnabled=false rehydrated counter — should be silent")
	}
}

// TestReplicator_RecoverFromBolt is the end-to-end S22 acceptance test:
// a Replicator wired to BoltDurableStore writes CRDT state, the store is
// closed (simulating process death), and a fresh Replicator opens the
// same on-disk file and recovers the state.  This exercises the full
// disk-round-trip path the in-memory store cannot.
func TestReplicator_RecoverFromBolt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store1, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBoltDurableStore#1: %v", err)
	}
	r1 := NewReplicator("nodeA", nil)
	r1.DurableEnabled = true
	r1.DurableStore = store1
	r1.DurableKeys = []string{"shard-*"}
	r1.IncrementCounter("shard-Counter-1", 7, WriteLocal)
	r1.AddToSet("shard-Set-1", "x", WriteLocal)
	r1.AddToSet("shard-Set-1", "y", WriteLocal)
	r1.PutInMap("shard-Map-1", "k", "v", WriteLocal)
	if err := store1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	store2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBoltDurableStore#2: %v", err)
	}
	defer store2.Close()
	r2 := NewReplicator("nodeA", nil)
	r2.DurableEnabled = true
	r2.DurableStore = store2
	r2.DurableKeys = []string{"shard-*"}
	if err := r2.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if v := r2.GCounter("shard-Counter-1").Value(); v != 7 {
		t.Errorf("recovered counter = %d, want 7", v)
	}
	if got := r2.ORSet("shard-Set-1").Elements(); !containsAll(got, []string{"x", "y"}) {
		t.Errorf("recovered set = %v, want [x y]", got)
	}
	if got, _ := r2.LWWMap("shard-Map-1").Get("k"); got != "v" {
		t.Errorf("recovered map[k] = %q, want v", got)
	}
}

// TestReplicator_PrunedStateSurvivesBoltRestart covers the S22 acceptance
// "pruning markers survive restart": when a node is removed and the oldest
// node prunes prunable CRDTs, the rewrite must be persisted so a restart
// does not resurrect the removed node's contribution.
func TestReplicator_PrunedStateSurvivesBoltRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store1, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBoltDurableStore#1: %v", err)
	}
	r1 := NewReplicator("nodeA", nil)
	r1.DurableEnabled = true
	r1.DurableStore = store1
	r1.DurableKeys = []string{"shard-*"}

	// Build a counter with contributions from two nodes, then prune nodeB.
	c := r1.GCounter("shard-Counter-1")
	c.Increment("nodeA", 3)
	c.Increment("nodeB", 4)
	r1.IncrementCounter("shard-Counter-1", 0, WriteLocal) // trigger persist
	if v := c.Value(); v != 7 {
		t.Fatalf("pre-prune counter = %d, want 7", v)
	}
	c.Prune("nodeB", "nodeA")
	r1.IncrementCounter("shard-Counter-1", 0, WriteLocal) // trigger persist
	if v := c.Value(); v != 7 {
		t.Fatalf("post-prune counter total = %d, want 7", v)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	store2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBoltDurableStore#2: %v", err)
	}
	defer store2.Close()
	r2 := NewReplicator("nodeA", nil)
	r2.DurableEnabled = true
	r2.DurableStore = store2
	r2.DurableKeys = []string{"shard-*"}
	if err := r2.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	c2 := r2.GCounter("shard-Counter-1")
	if v := c2.Value(); v != 7 {
		t.Errorf("post-restart counter total = %d, want 7", v)
	}
	if c2.NeedsPruning("nodeB") {
		t.Errorf("post-restart counter still needs pruning of nodeB — prune was not persisted")
	}
}

func containsAll(haystack, needles []string) bool {
	have := make(map[string]struct{}, len(haystack))
	for _, h := range haystack {
		have[h] = struct{}{}
	}
	for _, n := range needles {
		if _, ok := have[n]; !ok {
			return false
		}
	}
	return true
}
