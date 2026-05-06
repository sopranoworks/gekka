/*
 * serializer_cache_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func bgCtx() context.Context { return context.Background() }

// TestSerializerCache_HitMissAndExpiry covers the cache primitive directly
// using an injected clock so eviction is deterministic.
func TestSerializerCache_HitMissAndExpiry(t *testing.T) {
	cache := newSerializerCache()
	var fakeNow atomic.Int64
	fakeNow.Store(time.Unix(0, 0).UnixNano())
	cache.now = func() time.Time { return time.Unix(0, fakeNow.Load()) }

	ttl := 100 * time.Millisecond

	// First Store + matching Lookup.
	cache.Store("k1", "fp-A", []byte("payload-A"), ttl)
	got, ok := cache.Lookup("k1", "fp-A")
	if !ok || string(got) != "payload-A" {
		t.Fatalf("hit: want (payload-A, true); got (%q, %v)", got, ok)
	}

	// Fingerprint mismatch is a miss.
	if _, ok := cache.Lookup("k1", "fp-DIFFERENT"); ok {
		t.Fatalf("fingerprint mismatch must miss")
	}

	// Unknown key is a miss.
	if _, ok := cache.Lookup("missing", "fp-A"); ok {
		t.Fatalf("unknown key must miss")
	}

	// Step time past expiry → miss + entry purged.
	fakeNow.Add(int64(ttl + time.Millisecond))
	if _, ok := cache.Lookup("k1", "fp-A"); ok {
		t.Fatalf("expired entry must miss")
	}
	cache.mu.Lock()
	if _, exists := cache.entries["k1"]; exists {
		cache.mu.Unlock()
		t.Fatalf("expired Lookup must purge entry")
	}
	cache.mu.Unlock()

	// Store with TTL <= 0 is a no-op.
	cache.Store("k2", "fp-B", []byte("payload-B"), 0)
	if _, ok := cache.Lookup("k2", "fp-B"); ok {
		t.Fatalf("ttl<=0 Store must be a no-op")
	}
}

// TestSerializerCache_FingerprintHelpers locks down the per-CRDT fingerprint
// helpers so a refactor of the underlying snapshot shapes doesn't silently
// change cache identity.
func TestSerializerCache_FingerprintHelpers(t *testing.T) {
	a := map[string]uint64{"x": 1, "y": 2}
	b := map[string]uint64{"y": 2, "x": 1} // same content, different insertion order
	c := map[string]uint64{"x": 1, "y": 3} // different value
	if fingerprintGCounterState(a) != fingerprintGCounterState(b) {
		t.Errorf("gcounter fingerprint must be order-independent")
	}
	if fingerprintGCounterState(a) == fingerprintGCounterState(c) {
		t.Errorf("gcounter fingerprint must change with content")
	}

	snapA := ORSetSnapshot{
		VV:   map[string]uint64{"n1": 1, "n2": 1},
		Dots: map[string][]Dot{"e1": {{NodeID: "n1", Counter: 1}}},
	}
	snapB := ORSetSnapshot{
		VV:   map[string]uint64{"n2": 1, "n1": 1},
		Dots: map[string][]Dot{"e1": {{NodeID: "n1", Counter: 1}}},
	}
	snapC := ORSetSnapshot{
		VV:   map[string]uint64{"n1": 2, "n2": 1},
		Dots: map[string][]Dot{"e1": {{NodeID: "n1", Counter: 2}}},
	}
	if fingerprintORSetSnapshot(snapA) != fingerprintORSetSnapshot(snapB) {
		t.Errorf("orset fingerprint must be VV-order-independent")
	}
	if fingerprintORSetSnapshot(snapA) == fingerprintORSetSnapshot(snapC) {
		t.Errorf("orset fingerprint must change when VV/dots change")
	}

	mapA := map[string]LWWEntry{"k": {Value: "v", Timestamp: 1}}
	mapB := map[string]LWWEntry{"k": {Value: "v", Timestamp: 1}}
	mapC := map[string]LWWEntry{"k": {Value: "v", Timestamp: 2}}
	if fingerprintLWWMapState(mapA) != fingerprintLWWMapState(mapB) {
		t.Errorf("lwwmap fingerprint must be stable for equal state")
	}
	if fingerprintLWWMapState(mapA) == fingerprintLWWMapState(mapC) {
		t.Errorf("lwwmap fingerprint must change when timestamp changes")
	}
}

// TestReplicator_GossipCacheHitsRepeatedSerialization exercises the full
// gossip path: IncrementCounter with WriteAll triggers gossipCounter, which
// must consult the serializer cache. Two calls that leave state unchanged
// (delta=0) hit the cache; a real mutation misses.
func TestReplicator_GossipCacheHitsRepeatedSerialization(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.SerializerCacheTimeToLive = 5 * time.Second
	// No peers configured → sendToPeers fans out to nobody, but gossipCounter
	// still runs through cachedMarshal with our fingerprint.

	r.IncrementCounter("hits", 1, WriteAll) // miss #1: state {node-1:1}
	r.IncrementCounter("hits", 0, WriteAll) // hit:   state unchanged → fp match
	r.IncrementCounter("hits", 0, WriteAll) // hit:   still unchanged
	r.IncrementCounter("hits", 1, WriteAll) // miss #2: state {node-1:2}, new fp

	hits, misses := r.SerializerCacheStats()
	if hits != 2 {
		t.Errorf("hits = %d, want 2", hits)
	}
	if misses != 2 {
		t.Errorf("misses = %d, want 2", misses)
	}
}

// TestReplicator_GossipCacheTTLEviction verifies an entry inserted with a
// short TTL is treated as a miss after the deadline passes, even if the
// fingerprint would otherwise match.
func TestReplicator_GossipCacheTTLEviction(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.SerializerCacheTimeToLive = 20 * time.Millisecond

	r.IncrementCounter("hits", 1, WriteAll) // miss → store
	r.IncrementCounter("hits", 0, WriteAll) // hit  → cached

	hits, misses := r.SerializerCacheStats()
	if hits != 1 || misses != 1 {
		t.Fatalf("pre-expiry: hits=%d misses=%d, want 1/1", hits, misses)
	}

	// Wait past TTL.
	time.Sleep(40 * time.Millisecond)

	// State is unchanged — fingerprint still matches — but the entry has
	// expired, so this MUST count as a miss.
	r.IncrementCounter("hits", 0, WriteAll)

	hits, misses = r.SerializerCacheStats()
	if hits != 1 {
		t.Errorf("post-expiry hits = %d, want 1 (no new hit)", hits)
	}
	if misses != 2 {
		t.Errorf("post-expiry misses = %d, want 2 (expired = miss)", misses)
	}
}

// TestReplicator_GossipCacheVersionMiss verifies that a mutated CRDT
// produces a different fingerprint and therefore a cache miss, even within
// the TTL window.
func TestReplicator_GossipCacheVersionMiss(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.SerializerCacheTimeToLive = 1 * time.Hour

	r.IncrementCounter("k", 1, WriteAll) // miss (new key, fp1)
	r.IncrementCounter("k", 1, WriteAll) // miss (mutated → fp2)
	r.IncrementCounter("k", 1, WriteAll) // miss (mutated → fp3)

	hits, misses := r.SerializerCacheStats()
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (every call mutates)", hits)
	}
	if misses != 3 {
		t.Errorf("misses = %d, want 3", misses)
	}
}

// TestReplicator_GossipCacheDisabledByZeroTTL confirms that
// SerializerCacheTimeToLive=0 disables the cache: every call serializes
// fresh and Store is a no-op, so the entries map stays empty.
func TestReplicator_GossipCacheDisabledByZeroTTL(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.SerializerCacheTimeToLive = 0

	r.IncrementCounter("k", 1, WriteAll)
	r.IncrementCounter("k", 0, WriteAll) // would normally hit
	r.IncrementCounter("k", 0, WriteAll)

	hits, _ := r.SerializerCacheStats()
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (cache disabled)", hits)
	}
	r.serializerCache.mu.Lock()
	defer r.serializerCache.mu.Unlock()
	if got := len(r.serializerCache.entries); got != 0 {
		t.Errorf("entries = %d, want 0 (Store must be a no-op when ttl<=0)", got)
	}
}

// TestReplicator_GossipCacheCoversAllCRDTs sweeps the full set of gossip
// entry points to make sure cachedMarshal is wired into every flavour.
func TestReplicator_GossipCacheCoversAllCRDTs(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.SerializerCacheTimeToLive = 1 * time.Hour

	// Each of these triggers gossipXxx exactly once via WriteAll.
	r.IncrementCounter("c", 1, WriteAll)
	r.AddToSet("s", "x", WriteAll)
	r.PutInMap("m", "k", "v", WriteAll)
	r.PNCounter("p").Increment(r.NodeID(), 1)
	r.gossipPNCounter(bgCtx(), "p", r.PNCounter("p"), WriteAll)
	r.ORFlag("f").SwitchOn(r.NodeID())
	r.gossipORFlag(bgCtx(), "f", r.ORFlag("f"), WriteAll)
	r.LWWRegister("reg").Set(r.NodeID(), "value")
	r.gossipLWWRegister(bgCtx(), "reg", r.LWWRegister("reg"), WriteAll)

	hits, misses := r.SerializerCacheStats()
	if misses < 6 {
		t.Errorf("misses = %d, want at least 6 (one per CRDT type)", misses)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (each call has unique fp)", hits)
	}

	// Repeating each call without mutation must hit the cache for every type.
	r.gossipCounter(bgCtx(), "c", r.GCounter("c"), WriteAll)
	r.gossipSet(bgCtx(), "s", r.ORSet("s"), WriteAll)
	r.gossipMap(bgCtx(), "m", r.LWWMap("m"), WriteAll)
	r.gossipPNCounter(bgCtx(), "p", r.PNCounter("p"), WriteAll)
	r.gossipORFlag(bgCtx(), "f", r.ORFlag("f"), WriteAll)
	r.gossipLWWRegister(bgCtx(), "reg", r.LWWRegister("reg"), WriteAll)

	hits, _ = r.SerializerCacheStats()
	if hits < 6 {
		t.Errorf("repeat hits = %d, want at least 6 (one per CRDT type)", hits)
	}
}
