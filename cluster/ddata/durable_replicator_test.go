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
