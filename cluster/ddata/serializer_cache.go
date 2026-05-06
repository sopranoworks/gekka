/*
 * serializer_cache.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"time"
)

// serializerCacheEntry is one cached serialization for a single CRDT key.
// `fingerprint` identifies the version of the snapshot — when the underlying
// CRDT mutates, a freshly computed fingerprint will differ and the cache hit
// is rejected.
type serializerCacheEntry struct {
	fingerprint string
	bytes       []byte
	expiresAt   time.Time
}

// serializerCache is a per-Replicator TTL cache of serialized CRDT gossip
// payloads, keyed by `(crdt-key, fingerprint)`. It implements
// `pekko.cluster.distributed-data.serializer-cache-time-to-live`: across
// gossip rounds within the TTL window, repeated identical snapshots are
// returned from the cache instead of re-serializing.
//
// The lookup contract is "miss on either fingerprint mismatch or expiry";
// stale entries are removed on the lookup that observes them so the cache
// does not grow without bound when CRDTs mutate frequently.
//
// Thread-safe.
type serializerCache struct {
	mu      sync.Mutex
	entries map[string]serializerCacheEntry
	now     func() time.Time

	hits   uint64
	misses uint64
}

func newSerializerCache() *serializerCache {
	return &serializerCache{
		entries: make(map[string]serializerCacheEntry),
		now:     time.Now,
	}
}

// Lookup returns cached bytes when an entry exists for `key`, its fingerprint
// matches, and the entry has not expired. Returns (nil, false) otherwise.
// Callers MUST treat a miss by serializing fresh and calling Store.
func (c *serializerCache) Lookup(key, fingerprint string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		c.misses++
		return nil, false
	}
	if e.fingerprint != fingerprint {
		c.misses++
		return nil, false
	}
	if c.now().After(e.expiresAt) {
		delete(c.entries, key)
		c.misses++
		return nil, false
	}
	c.hits++
	return e.bytes, true
}

// Store records `bytes` as the cached serialization for `key`+`fingerprint`.
// The entry expires at `now + ttl`. A `ttl <= 0` makes Store a no-op so
// callers can disable caching by setting SerializerCacheTimeToLive=0 without
// branching at every call site.
func (c *serializerCache) Store(key, fingerprint string, bytes []byte, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = serializerCacheEntry{
		fingerprint: fingerprint,
		bytes:       bytes,
		expiresAt:   c.now().Add(ttl),
	}
}

// Stats returns (hits, misses) since the cache was created.
func (c *serializerCache) Stats() (uint64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}

// Clear drops every cached entry and zeroes the stat counters. Test-only;
// production code should rely on TTL expiry.
func (c *serializerCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]serializerCacheEntry)
	c.hits = 0
	c.misses = 0
}

// ── Fingerprint helpers ──────────────────────────────────────────────────
//
// Each helper hashes the snapshot's logical content into a 16-char hex
// string. Stable across Go runs because we always sort map keys before
// hashing, and FNV-64a is deterministic. Cheaper than re-marshaling: for
// each (key, value) pair we write the raw bytes once instead of producing
// JSON-escaped output.

func fingerprintGCounterState(state map[string]uint64) string {
	h := fnv.New64a()
	writeNodeMap(h, state)
	return formatHash(h)
}

func fingerprintORSetSnapshot(snap ORSetSnapshot) string {
	h := fnv.New64a()
	writeNodeMap(h, snap.VV)
	h.Write([]byte{0xff})
	elems := make([]string, 0, len(snap.Dots))
	for e := range snap.Dots {
		elems = append(elems, e)
	}
	sort.Strings(elems)
	var buf [8]byte
	for _, e := range elems {
		h.Write([]byte(e))
		h.Write([]byte{0})
		ds := snap.Dots[e]
		binary.LittleEndian.PutUint64(buf[:], uint64(len(ds)))
		h.Write(buf[:])
		sortedDots := make([]Dot, len(ds))
		copy(sortedDots, ds)
		sort.Slice(sortedDots, func(i, j int) bool {
			if sortedDots[i].NodeID != sortedDots[j].NodeID {
				return sortedDots[i].NodeID < sortedDots[j].NodeID
			}
			return sortedDots[i].Counter < sortedDots[j].Counter
		})
		for _, d := range sortedDots {
			h.Write([]byte(d.NodeID))
			h.Write([]byte{0})
			binary.LittleEndian.PutUint64(buf[:], d.Counter)
			h.Write(buf[:])
		}
	}
	return formatHash(h)
}

func fingerprintLWWMapState(state map[string]LWWEntry) string {
	h := fnv.New64a()
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf [8]byte
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		entry := state[k]
		binary.LittleEndian.PutUint64(buf[:], uint64(entry.Timestamp))
		h.Write(buf[:])
		h.Write([]byte(fmt.Sprintf("%v", entry.Value)))
		h.Write([]byte{0})
	}
	return formatHash(h)
}

func fingerprintPNCounterSnapshot(snap PNCounterSnapshot) string {
	h := fnv.New64a()
	h.Write([]byte("p:"))
	writeNodeMap(h, snap.Pos)
	h.Write([]byte("n:"))
	writeNodeMap(h, snap.Neg)
	return formatHash(h)
}

func fingerprintLWWRegisterSnapshot(snap LWWRegisterSnapshot) string {
	h := fnv.New64a()
	h.Write([]byte(snap.NodeID))
	h.Write([]byte{0})
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(snap.Timestamp))
	h.Write(buf[:])
	h.Write([]byte(fmt.Sprintf("%v", snap.Value)))
	return formatHash(h)
}

func writeNodeMap(h hash.Hash64, m map[string]uint64) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf [8]byte
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint64(buf[:], m[k])
		h.Write(buf[:])
	}
}

func formatHash(h hash.Hash64) string {
	return strconv.FormatUint(h.Sum64(), 16)
}
