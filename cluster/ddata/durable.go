/*
 * durable.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"errors"
	"strings"
)

// CRDTType identifies a CRDT family in a stored DurableEntry. Mirrors the
// strings used in the gossip envelope (see Replicator.HandleIncoming).
type CRDTType string

const (
	CRDTTypeGCounter    CRDTType = "gcounter"
	CRDTTypeORSet       CRDTType = "orset"
	CRDTTypeLWWMap      CRDTType = "lwwmap"
	CRDTTypePNCounter   CRDTType = "pncounter"
	CRDTTypeORFlag      CRDTType = "orflag"
	CRDTTypeLWWRegister CRDTType = "lwwregister"
)

// DurableEntry is the unit a DurableStore persists. It is keyed by the CRDT's
// user-visible key (e.g. "shard-Counter-1") and tagged with the CRDT family
// so that recovery can reconstitute the right type without sniffing the
// payload.  The Payload itself is the JSON snapshot the Replicator already
// uses for gossip — keeping the same wire format avoids a second codec.
type DurableEntry struct {
	Key     string
	Type    CRDTType
	Payload []byte
}

// ErrDurableKeyNotFound is returned by DurableStore.Load when the key is not
// persisted.  Callers must distinguish "key absent" from "store failure" so
// that recovery can succeed silently on a fresh node.
var ErrDurableKeyNotFound = errors.New("ddata: durable key not found")

// DurableStore persists CRDT snapshots so that a replicator can recover
// state across restarts.  Implementations must be safe for concurrent use.
//
// The contract follows Pekko's LmdbDurableStore semantics translated into
// Go: Load is point-lookup by key, LoadAll is the bulk-recovery hot path
// invoked from Replicator.Start, Store overwrites, Delete is idempotent,
// Close releases backend resources.
//
// Backends do not interpret Payload bytes — they are opaque.  CRDT-family
// tagging is carried in DurableEntry.Type; the Replicator uses that tag to
// route the payload to the correct merge handler.
type DurableStore interface {
	// Load returns the DurableEntry for key, or ErrDurableKeyNotFound when
	// the key has never been stored.  A non-nil non-ErrDurableKeyNotFound
	// error indicates a backend failure.
	Load(ctx context.Context, key string) (DurableEntry, error)

	// LoadAll returns every persisted entry.  Used at startup to repopulate
	// the Replicator before joining the cluster.  Iteration order is not
	// guaranteed.
	LoadAll(ctx context.Context) ([]DurableEntry, error)

	// Store overwrites the persisted snapshot for entry.Key.  Implementations
	// may batch writes; flushing semantics are backend-specific (see
	// pekko.cluster.distributed-data.durable.lmdb.write-behind-interval in
	// session 23 wiring for the on-disk backend).
	Store(ctx context.Context, entry DurableEntry) error

	// Delete removes the persisted entry for key.  Idempotent: deleting an
	// absent key is a no-op and must not error.
	Delete(ctx context.Context, key string) error

	// Close releases any backend resources (file handles, goroutines).
	// Subsequent operations on a closed store may return errors — callers
	// should treat Close as terminal.
	Close() error
}

// IsDurableKey returns true when key matches one of the configured glob
// patterns.  Pekko supports a single wildcard form: a trailing "*" expands
// to a prefix match; everything else is an exact match.  Round 2 mirrors
// that exactly so that durable.keys configured for Pekko works unchanged.
func IsDurableKey(patterns []string, key string) bool {
	for _, pat := range patterns {
		pat = strings.TrimSpace(pat)
		if pat == "" {
			continue
		}
		if strings.HasSuffix(pat, "*") {
			if strings.HasPrefix(key, strings.TrimSuffix(pat, "*")) {
				return true
			}
			continue
		}
		if pat == key {
			return true
		}
	}
	return false
}
