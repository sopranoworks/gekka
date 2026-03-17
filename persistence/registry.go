/*
 * registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"database/sql"
	"fmt"
	"sync"
)

// JournalFactory creates a Journal backed by a pre-opened *sql.DB.
// Registered implementations are looked up by name via NewJournalFromDB.
//
// Example registration (typically in an init function):
//
//	persistence.RegisterJournal("postgres", func(db *sql.DB) persistence.Journal {
//	    return sqlstore.NewSQLJournal(db, sqlstore.PostgresDialect{}, sqlstore.NewJSONCodec(), sqlstore.DefaultConfig())
//	})
type JournalFactory func(db *sql.DB) Journal

// SnapshotStoreFactory creates a SnapshotStore backed by a pre-opened *sql.DB.
// Registered implementations are looked up by name via NewSnapshotStoreFromDB.
type SnapshotStoreFactory func(db *sql.DB) SnapshotStore

var (
	registryMu  sync.RWMutex
	journalReg  = make(map[string]JournalFactory)
	snapshotReg = make(map[string]SnapshotStoreFactory)
)

// RegisterJournal registers a JournalFactory under name.
// Subsequent calls with the same name overwrite the previous registration.
//
// This function is safe for concurrent use and is typically called from an
// init() function in the backend package:
//
//	func init() {
//	    persistence.RegisterJournal("sql", func(db *sql.DB) persistence.Journal { ... })
//	}
func RegisterJournal(name string, factory JournalFactory) {
	registryMu.Lock()
	journalReg[name] = factory
	registryMu.Unlock()
}

// RegisterSnapshotStore registers a SnapshotStoreFactory under name.
// Subsequent calls with the same name overwrite the previous registration.
func RegisterSnapshotStore(name string, factory SnapshotStoreFactory) {
	registryMu.Lock()
	snapshotReg[name] = factory
	registryMu.Unlock()
}

// NewJournalFromDB looks up the JournalFactory registered under name and
// calls it with db to produce a Journal.
//
// Returns an error if no factory is registered under name.  The caller is
// responsible for opening (and closing) db.
func NewJournalFromDB(name string, db *sql.DB) (Journal, error) {
	registryMu.RLock()
	f, ok := journalReg[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: no journal registered under %q (did you import the backend package?)", name)
	}
	return f(db), nil
}

// NewSnapshotStoreFromDB looks up the SnapshotStoreFactory registered under
// name and calls it with db to produce a SnapshotStore.
//
// Returns an error if no factory is registered under name.
func NewSnapshotStoreFromDB(name string, db *sql.DB) (SnapshotStore, error) {
	registryMu.RLock()
	f, ok := snapshotReg[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: no snapshot store registered under %q (did you import the backend package?)", name)
	}
	return f(db), nil
}

// JournalNames returns the names of all registered Journal factories.
func JournalNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(journalReg))
	for n := range journalReg {
		names = append(names, n)
	}
	return names
}

// SnapshotStoreNames returns the names of all registered SnapshotStore factories.
func SnapshotStoreNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(snapshotReg))
	for n := range snapshotReg {
		names = append(names, n)
	}
	return names
}
