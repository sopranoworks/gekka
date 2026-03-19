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
type JournalFactory func(db *sql.DB) Journal

// SnapshotStoreFactory creates a SnapshotStore backed by a pre-opened *sql.DB.
type SnapshotStoreFactory func(db *sql.DB) SnapshotStore

// DurableStateStoreFactory creates a DurableStateStore backed by a pre-opened *sql.DB.
type DurableStateStoreFactory func(db *sql.DB) DurableStateStore

var (
	registryMu      sync.RWMutex
	journalReg      = make(map[string]JournalFactory)
	snapshotReg     = make(map[string]SnapshotStoreFactory)
	durableStateReg = make(map[string]DurableStateStoreFactory)
)

// RegisterJournal registers a JournalFactory under name.
func RegisterJournal(name string, factory JournalFactory) {
	registryMu.Lock()
	journalReg[name] = factory
	registryMu.Unlock()
}

// RegisterSnapshotStore registers a SnapshotStoreFactory under name.
func RegisterSnapshotStore(name string, factory SnapshotStoreFactory) {
	registryMu.Lock()
	snapshotReg[name] = factory
	registryMu.Unlock()
}

// RegisterDurableStateStore registers a DurableStateStoreFactory under name.
func RegisterDurableStateStore(name string, factory DurableStateStoreFactory) {
	registryMu.Lock()
	durableStateReg[name] = factory
	registryMu.Unlock()
}

// NewJournalFromDB looks up the JournalFactory registered under name and
// calls it with db to produce a Journal.
func NewJournalFromDB(name string, db *sql.DB) (Journal, error) {
	registryMu.RLock()
	f, ok := journalReg[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: no journal registered under %q", name)
	}
	return f(db), nil
}

// NewSnapshotStoreFromDB looks up the SnapshotStoreFactory registered under
// name and calls it with db to produce a SnapshotStore.
func NewSnapshotStoreFromDB(name string, db *sql.DB) (SnapshotStore, error) {
	registryMu.RLock()
	f, ok := snapshotReg[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: no snapshot store registered under %q", name)
	}
	return f(db), nil
}

// NewDurableStateStoreFromDB looks up the DurableStateStoreFactory registered
// under name and calls it with db to produce a DurableStateStore.
func NewDurableStateStoreFromDB(name string, db *sql.DB) (DurableStateStore, error) {
	registryMu.RLock()
	f, ok := durableStateReg[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: no durable state store registered under %q", name)
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

// DurableStateStoreNames returns the names of all registered DurableStateStore factories.
func DurableStateStoreNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(durableStateReg))
	for n := range durableStateReg {
		names = append(names, n)
	}
	return names
}
