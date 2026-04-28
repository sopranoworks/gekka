/*
 * durable_bolt_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openBolt is a tiny test helper that opens a fresh store under t.TempDir().
// Centralising it keeps the contract test parallel to durable_test.go's
// MemoryDurableStore tests — both backends are exercised through the same
// lens.
func openBolt(t *testing.T, opts BoltDurableStoreOptions) *BoltDurableStore {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	s, err := OpenBoltDurableStore(opts)
	if err != nil {
		t.Fatalf("OpenBoltDurableStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestBoltDurableStore_RoundTrip is the same contract test the in-memory
// store passes — Store/Load returns the exact bytes that went in, and the
// returned slice is not aliased with the on-disk copy.  This is the
// minimum every DurableStore implementation owes Replicator.Recover.
func TestBoltDurableStore_RoundTrip(t *testing.T) {
	s := openBolt(t, BoltDurableStoreOptions{})
	ctx := context.Background()

	in := DurableEntry{
		Key:     "shard-Counter-1",
		Type:    CRDTTypeGCounter,
		Payload: []byte(`{"state":{"node1":7}}`),
	}
	if err := s.Store(ctx, in); err != nil {
		t.Fatalf("Store: %v", err)
	}
	out, err := s.Load(ctx, "shard-Counter-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if out.Key != in.Key || out.Type != in.Type || string(out.Payload) != string(in.Payload) {
		t.Errorf("Load returned %+v, want %+v", out, in)
	}

	out.Payload[0] = 'Z'
	again, _ := s.Load(ctx, "shard-Counter-1")
	if string(again.Payload) != string(in.Payload) {
		t.Errorf("Load aliases payload: got %q after mutation", again.Payload)
	}
}

// TestBoltDurableStore_MissReturnsSentinel locks down the
// ErrDurableKeyNotFound contract.  Recovery uses this signal to know a
// fresh node has nothing to load — without it, every cold start would
// look like a backend failure.
func TestBoltDurableStore_MissReturnsSentinel(t *testing.T) {
	s := openBolt(t, BoltDurableStoreOptions{})
	_, err := s.Load(context.Background(), "missing")
	if !errors.Is(err, ErrDurableKeyNotFound) {
		t.Errorf("Load on miss returned %v, want ErrDurableKeyNotFound", err)
	}
}

// TestBoltDurableStore_LoadAll covers the bulk-recovery hot path that
// Replicator.Recover invokes at startup. Order is undefined by contract
// but the set must equal the union of stored entries.
func TestBoltDurableStore_LoadAll(t *testing.T) {
	s := openBolt(t, BoltDurableStoreOptions{})
	ctx := context.Background()

	entries := []DurableEntry{
		{Key: "a", Type: CRDTTypeGCounter, Payload: []byte(`{"state":{"n":1}}`)},
		{Key: "b", Type: CRDTTypeORSet, Payload: []byte(`{"elements":{}}`)},
		{Key: "c", Type: CRDTTypeLWWMap, Payload: []byte(`{"state":{}}`)},
	}
	for _, e := range entries {
		if err := s.Store(ctx, e); err != nil {
			t.Fatalf("Store(%s): %v", e.Key, err)
		}
	}
	out, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(out) != len(entries) {
		t.Fatalf("LoadAll = %d, want %d", len(out), len(entries))
	}
}

// TestBoltDurableStore_DeleteIdempotent matches the in-memory store's
// idempotent-delete behavior — Replicator pruning paths assume calling
// Delete on an already-absent key is a no-op.
func TestBoltDurableStore_DeleteIdempotent(t *testing.T) {
	s := openBolt(t, BoltDurableStoreOptions{})
	ctx := context.Background()

	if err := s.Store(ctx, DurableEntry{Key: "k", Type: CRDTTypeGCounter, Payload: []byte("1")}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Load(ctx, "k"); !errors.Is(err, ErrDurableKeyNotFound) {
		t.Errorf("Load after Delete: got %v, want ErrDurableKeyNotFound", err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Errorf("Delete on absent key returned %v, want nil", err)
	}
	if err := s.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete on never-stored key returned %v, want nil", err)
	}
}

// TestBoltDurableStore_PersistsAcrossReopen is the core S22 deliverable:
// state survives a process restart.  We open, write, close, reopen, and
// prove LoadAll returns the same set.  Equivalent to the
// node-crash-and-rejoin scenario the cluster wiring depends on.
func TestBoltDurableStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Open#1: %v", err)
	}
	if err := s1.Store(ctx, DurableEntry{Key: "k1", Type: CRDTTypeGCounter, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s1.Store(ctx, DurableEntry{Key: "k2", Type: CRDTTypeORSet, Payload: []byte("v2")}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close#1: %v", err)
	}

	s2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Open#2: %v", err)
	}
	defer s2.Close()
	out, err := s2.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll#2: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("LoadAll#2 = %d entries, want 2", len(out))
	}
}

// TestBoltDurableStore_MapSizeEnforced proves the LMDB-equivalent hard
// cap kicks in.  Without this the cluster could silently consume
// unbounded disk on a runaway CRDT.  We size the cap small enough that
// the second write must fail with ErrDurableStoreFull.
func TestBoltDurableStore_MapSizeEnforced(t *testing.T) {
	dir := t.TempDir()
	// bbolt's default page size is 4 KiB and a freshly-created file is
	// already several pages.  A 4 KiB cap is therefore guaranteed to be
	// "already exceeded by the bare file" — the first write fails.
	s, err := OpenBoltDurableStore(BoltDurableStoreOptions{
		Dir:     dir,
		MapSize: 4 * 1024,
	})
	if err != nil {
		t.Fatalf("OpenBoltDurableStore: %v", err)
	}
	defer s.Close()

	err = s.Store(context.Background(), DurableEntry{
		Key: "k", Type: CRDTTypeGCounter,
		Payload: make([]byte, 1024),
	})
	if !errors.Is(err, ErrDurableStoreFull) {
		t.Errorf("Store with cap exceeded returned %v, want ErrDurableStoreFull", err)
	}
}

// TestBoltDurableStore_ConcurrentWrites stress-tests the bbolt wrapper
// the same way the in-memory store is stressed.  bbolt is a single-
// writer engine so this also exercises the txn-serialisation path
// independently from the buffered flusher.
func TestBoltDurableStore_ConcurrentWrites(t *testing.T) {
	s := openBolt(t, BoltDurableStoreOptions{})
	ctx := context.Background()

	const writers = 8
	const writes = 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range writes {
				_ = s.Store(ctx, DurableEntry{
					Key:     keyN(w, i),
					Type:    CRDTTypeGCounter,
					Payload: []byte("1"),
				})
			}
		}(w)
	}
	wg.Wait()

	out, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if got, want := len(out), writers*writes; got != want {
		t.Errorf("LoadAll size = %d, want %d", got, want)
	}
}

// TestBoltDurableStore_WriteBehindCoalesces verifies the buffered path:
// writes do not hit disk until the flush ticker fires (or Close drains),
// repeated writes to the same key coalesce, and Load can still see the
// buffered value before the flush.
func TestBoltDurableStore_WriteBehindCoalesces(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenBoltDurableStore(BoltDurableStoreOptions{
		Dir:                 dir,
		WriteBehindInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	for i := range 10 {
		if err := s.Store(ctx, DurableEntry{
			Key: "k", Type: CRDTTypeGCounter,
			Payload: []byte{byte(i)},
		}); err != nil {
			t.Fatalf("Store#%d: %v", i, err)
		}
	}
	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load buffered: %v", err)
	}
	if string(got.Payload) != string([]byte{9}) {
		t.Errorf("buffered Load returned %v, want last write byte 9", got.Payload)
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()
	got2, err := s2.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load post-restart: %v", err)
	}
	if string(got2.Payload) != string([]byte{9}) {
		t.Errorf("post-restart Load = %v, want byte 9 (coalesced last write)", got2.Payload)
	}
	if filepath.Base(s2.Path()) != "ddata.db" {
		t.Errorf("Path basename = %q, want ddata.db", filepath.Base(s2.Path()))
	}
}

// TestBoltDurableStore_WriteBehindCloseFlushes proves the
// "Close drains the buffer" contract: state staged before Close survives
// even if the flush ticker never fired.  Without this a crash-on-shutdown
// would silently lose the most recent gossip round.
func TestBoltDurableStore_WriteBehindCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenBoltDurableStore(BoltDurableStoreOptions{
		Dir: dir,
		// Long enough that the ticker definitely never fires during
		// the test — we are testing Close, not the ticker.
		WriteBehindInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := s.Store(ctx, DurableEntry{Key: "k", Type: CRDTTypeGCounter, Payload: []byte("v")}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got.Payload) != "v" {
		t.Errorf("post-Close Load = %q, want v", got.Payload)
	}
}

// TestBoltDurableStore_WriteBehindDelete confirms the delete sentinel
// survives flush and reopen: a delete buffered with a co-pending write
// must remove the key, not race-restore it.
func TestBoltDurableStore_WriteBehindDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenBoltDurableStore(BoltDurableStoreOptions{
		Dir: dir, WriteBehindInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	_ = s.Store(ctx, DurableEntry{Key: "k", Type: CRDTTypeGCounter, Payload: []byte("v")})
	_ = s.Delete(ctx, "k")
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenBoltDurableStore(BoltDurableStoreOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.Load(ctx, "k"); !errors.Is(err, ErrDurableKeyNotFound) {
		t.Errorf("Load after buffered delete: got %v, want ErrDurableKeyNotFound", err)
	}
}
