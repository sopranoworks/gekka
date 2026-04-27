/*
 * durable_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestIsDurableKey covers the prefix-glob matcher that maps Pekko's
// "shard-*" patterns onto Go strings.  Exact match, prefix match, empty
// pattern handling, and miss cases all need explicit coverage.
func TestIsDurableKey(t *testing.T) {
	cases := []struct {
		patterns []string
		key      string
		want     bool
	}{
		{[]string{"shard-*"}, "shard-Counter-1", true},
		{[]string{"shard-*"}, "ShardCounter-1", false},
		{[]string{"hits"}, "hits", true},
		{[]string{"hits"}, "misses", false},
		{[]string{""}, "anything", false},
		{[]string{}, "anything", false},
		{[]string{"a-*", "b"}, "b", true},
		{[]string{"a-*", "b"}, "a-1", true},
		{[]string{"a-*", "b"}, "c", false},
	}
	for _, tc := range cases {
		got := IsDurableKey(tc.patterns, tc.key)
		if got != tc.want {
			t.Errorf("IsDurableKey(%v, %q) = %v, want %v", tc.patterns, tc.key, got, tc.want)
		}
	}
}

// TestMemoryDurableStore_RoundTrip covers the basic Store/Load contract
// every DurableStore implementation must honor.  The round-trip is the
// hot path used by S22's BoltDB backend so locking it down here gives us
// a contract test the disk backend can be checked against.
func TestMemoryDurableStore_RoundTrip(t *testing.T) {
	store := NewMemoryDurableStore()
	ctx := context.Background()

	in := DurableEntry{
		Key:     "shard-Counter-1",
		Type:    CRDTTypeGCounter,
		Payload: []byte(`{"state":{"node1":7}}`),
	}
	if err := store.Store(ctx, in); err != nil {
		t.Fatalf("Store: %v", err)
	}
	out, err := store.Load(ctx, "shard-Counter-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if out.Key != in.Key || out.Type != in.Type || string(out.Payload) != string(in.Payload) {
		t.Errorf("Load returned %+v, want %+v", out, in)
	}

	out.Payload[0] = 'Z'
	again, _ := store.Load(ctx, "shard-Counter-1")
	if string(again.Payload) != string(in.Payload) {
		t.Errorf("Load returns aliased payload: got %q after mutation", again.Payload)
	}
}

// TestMemoryDurableStore_MissReturnsSentinel proves that a fresh store
// reports "key absent" via ErrDurableKeyNotFound — recovery must rely
// on this signal to distinguish empty-store from backend-failure.
func TestMemoryDurableStore_MissReturnsSentinel(t *testing.T) {
	store := NewMemoryDurableStore()
	_, err := store.Load(context.Background(), "missing")
	if !errors.Is(err, ErrDurableKeyNotFound) {
		t.Errorf("Load on miss returned %v, want ErrDurableKeyNotFound", err)
	}
}

// TestMemoryDurableStore_LoadAll exercises the bulk-recovery path used
// by Replicator.Recover.  Order is undefined per contract, but the set
// of returned entries must equal the set of stored entries.
func TestMemoryDurableStore_LoadAll(t *testing.T) {
	store := NewMemoryDurableStore()
	ctx := context.Background()

	entries := []DurableEntry{
		{Key: "a", Type: CRDTTypeGCounter, Payload: []byte("1")},
		{Key: "b", Type: CRDTTypeORSet, Payload: []byte("2")},
		{Key: "c", Type: CRDTTypeLWWMap, Payload: []byte("3")},
	}
	for _, e := range entries {
		if err := store.Store(ctx, e); err != nil {
			t.Fatalf("Store(%s): %v", e.Key, err)
		}
	}
	out, err := store.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(out) != len(entries) {
		t.Fatalf("LoadAll returned %d entries, want %d", len(out), len(entries))
	}
	got := make(map[string]DurableEntry, len(out))
	for _, e := range out {
		got[e.Key] = e
	}
	for _, want := range entries {
		have, ok := got[want.Key]
		if !ok {
			t.Errorf("LoadAll missing %q", want.Key)
			continue
		}
		if have.Type != want.Type || string(have.Payload) != string(want.Payload) {
			t.Errorf("LoadAll[%s] = %+v, want %+v", want.Key, have, want)
		}
	}
}

// TestMemoryDurableStore_Delete checks idempotent delete: a second
// delete on an absent key must succeed.  Replicator's pruning path
// relies on this contract so a slow gossip can't crash the store.
func TestMemoryDurableStore_Delete(t *testing.T) {
	store := NewMemoryDurableStore()
	ctx := context.Background()

	_ = store.Store(ctx, DurableEntry{Key: "k", Type: CRDTTypeGCounter, Payload: []byte("1")})
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Load(ctx, "k"); !errors.Is(err, ErrDurableKeyNotFound) {
		t.Errorf("Load after Delete: got %v, want ErrDurableKeyNotFound", err)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Errorf("Delete on absent key returned %v, want nil", err)
	}
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete on never-stored key returned %v, want nil", err)
	}
}

// TestMemoryDurableStore_ConcurrentWrites stress-tests the mutex
// discipline.  We don't expect any backend bugs for the in-mem store
// but the test pins the contract for the BoltDB backend in S22.
func TestMemoryDurableStore_ConcurrentWrites(t *testing.T) {
	store := NewMemoryDurableStore()
	ctx := context.Background()

	const writers = 16
	const writes = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				key := keyN(w, i)
				_ = store.Store(ctx, DurableEntry{
					Key:     key,
					Type:    CRDTTypeGCounter,
					Payload: []byte("1"),
				})
				_, _ = store.Load(ctx, key)
			}
		}(w)
	}
	wg.Wait()

	if got := store.Len(); got != writers*writes {
		t.Errorf("Len after concurrent writes = %d, want %d", got, writers*writes)
	}
}

func keyN(w, i int) string {
	const hex = "0123456789abcdef"
	buf := []byte("k----")
	buf[1] = hex[w&0xf]
	buf[2] = hex[(i>>8)&0xf]
	buf[3] = hex[(i>>4)&0xf]
	buf[4] = hex[i&0xf]
	return string(buf)
}
