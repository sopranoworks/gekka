/*
 * register.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package spannerstore

import (
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/sopranoworks/gekka/persistence"
)

var (
	configMu      sync.RWMutex
	configuredDB  *spanner.Client
	configuredCodec PayloadCodec
	configured    bool
)

// Configure sets the Spanner client and codec that the "spanner" provider will
// use. Call this once during application initialization, before any actors that
// use persistent state are started.
//
// Importing this package alone registers the "spanner" name in the persistence
// registry; calling Configure wires up the actual backend. Omitting Configure
// causes the provider factory to panic with a descriptive error.
func Configure(client *spanner.Client, codec PayloadCodec) {
	configMu.Lock()
	configuredDB = client
	configuredCodec = codec
	configured = true
	configMu.Unlock()
}

func init() {
	persistence.RegisterJournalProvider("spanner", func() persistence.Journal {
		configMu.RLock()
		ok := configured
		client := configuredDB
		codec := configuredCodec
		configMu.RUnlock()
		if !ok {
			panic("spannerstore: persistence backend not configured; call spannerstore.Configure(client, codec) before spawning persistent actors")
		}
		return NewSpannerJournal(client, codec)
	})
	persistence.RegisterSnapshotStoreProvider("spanner", func() persistence.SnapshotStore {
		configMu.RLock()
		ok := configured
		client := configuredDB
		codec := configuredCodec
		configMu.RUnlock()
		if !ok {
			panic("spannerstore: persistence backend not configured; call spannerstore.Configure(client, codec) before spawning persistent actors")
		}
		return NewSpannerSnapshotStore(client, codec)
	})
}
