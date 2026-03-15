/*
 * cluster_persistence.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"database/sql"
	"sync"

	"github.com/sopranoworks/gekka/persistence"
)

// persistenceState is embedded in Cluster to hold the optionally-provisioned
// Journal and SnapshotStore backends.
type persistenceState struct {
	mu            sync.RWMutex
	journal       persistence.Journal
	snapshotStore persistence.SnapshotStore
}

// ── Cluster methods ───────────────────────────────────────────────────────────

// ProvideJournalDB creates a Journal using the factory registered under
// plugin (via persistence.RegisterJournal) and stores it on the Cluster.
// Retrieve it later with node.Journal().
//
// This method is the bridge between the driver-agnostic SQL backend and
// your application-specific *sql.DB:
//
//	// 1. Register the backend once (e.g. in init or main):
//	sqlstore.RegisterPostgresBackend("postgres", codec, sqlstore.DefaultConfig())
//
//	// 2. Open a DB connection (driver is in YOUR code, not in gekka):
//	db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
//
//	// 3. Wire up the DB to the cluster:
//	if err := node.ProvideJournalDB("postgres", db); err != nil {
//	    log.Fatal(err)
//	}
//
//	// 4. Use the journal in persistent actors:
//	behavior.Journal = node.Journal()
func (c *Cluster) ProvideJournalDB(plugin string, db *sql.DB) error {
	j, err := persistence.NewJournalFromDB(plugin, db)
	if err != nil {
		return err
	}
	c.ps.mu.Lock()
	c.ps.journal = j
	c.ps.mu.Unlock()
	return nil
}

// ProvideSnapshotStoreDB creates a SnapshotStore using the factory registered
// under plugin (via persistence.RegisterSnapshotStore) and stores it on the
// Cluster.  Retrieve it later with node.SnapshotStore().
func (c *Cluster) ProvideSnapshotStoreDB(plugin string, db *sql.DB) error {
	ss, err := persistence.NewSnapshotStoreFromDB(plugin, db)
	if err != nil {
		return err
	}
	c.ps.mu.Lock()
	c.ps.snapshotStore = ss
	c.ps.mu.Unlock()
	return nil
}

// Journal returns the Journal provisioned by ProvideJournalDB, or nil if none
// has been provisioned.  A nil return signals that no SQL-backed journal has
// been wired up yet; fall back to persistence.NewInMemoryJournal() in tests.
func (c *Cluster) Journal() persistence.Journal {
	c.ps.mu.RLock()
	j := c.ps.journal
	c.ps.mu.RUnlock()
	return j
}

// SnapshotStore returns the SnapshotStore provisioned by
// ProvideSnapshotStoreDB, or nil if none has been provisioned.
func (c *Cluster) SnapshotStore() persistence.SnapshotStore {
	c.ps.mu.RLock()
	ss := c.ps.snapshotStore
	c.ps.mu.RUnlock()
	return ss
}
