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

// ── SQL-backed factories ──────────────────────────────────────────────────────

// JournalFactory creates a Journal backed by a pre-opened *sql.DB.
// Used by SQL backends (postgres, mysql). Register with RegisterJournal.
type JournalFactory func(db *sql.DB) Journal

// SnapshotStoreFactory creates a SnapshotStore backed by a pre-opened *sql.DB.
type SnapshotStoreFactory func(db *sql.DB) SnapshotStore

// DurableStateStoreFactory creates a DurableStateStore backed by a pre-opened *sql.DB.
type DurableStateStoreFactory func(db *sql.DB) DurableStateStore

// ── Zero-config providers ─────────────────────────────────────────────────────

// JournalProvider creates a Journal with no external dependencies.
// Used by self-contained backends such as "in-memory". Register with
// RegisterJournalProvider; retrieve with NewJournal.
type JournalProvider func() Journal

// SnapshotStoreProvider creates a SnapshotStore with no external dependencies.
type SnapshotStoreProvider func() SnapshotStore

var (
	registryMu      sync.RWMutex
	journalReg      = make(map[string]JournalFactory)
	snapshotReg     = make(map[string]SnapshotStoreFactory)
	durableStateReg = make(map[string]DurableStateStoreFactory)

	journalProviders      = make(map[string]JournalProvider)
	snapshotProviders     = make(map[string]SnapshotStoreProvider)
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

// JournalProviderNames returns the names of all registered zero-config Journal providers.
func JournalProviderNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(journalProviders))
	for n := range journalProviders {
		names = append(names, n)
	}
	return names
}

// SnapshotStoreProviderNames returns the names of all registered zero-config SnapshotStore providers.
func SnapshotStoreProviderNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(snapshotProviders))
	for n := range snapshotProviders {
		names = append(names, n)
	}
	return names
}

// ── Zero-config provider registration ────────────────────────────────────────

// RegisterJournalProvider registers a zero-config JournalProvider under name.
// The provider is a closure that captures all configuration at registration
// time and needs no external resources when called.
//
// The built-in "in-memory" provider is pre-registered automatically.
func RegisterJournalProvider(name string, provider JournalProvider) {
	registryMu.Lock()
	journalProviders[name] = provider
	registryMu.Unlock()
}

// RegisterSnapshotStoreProvider registers a zero-config SnapshotStoreProvider under name.
func RegisterSnapshotStoreProvider(name string, provider SnapshotStoreProvider) {
	registryMu.Lock()
	snapshotProviders[name] = provider
	registryMu.Unlock()
}

// NewJournal looks up the JournalProvider registered under name and returns a
// new Journal instance.  The provider registry is checked first; if not found
// there the SQL-backed JournalFactory registry is consulted but returns an
// error because a *sql.DB is required for SQL factories.
//
// Use the name "in-memory" for the built-in in-process journal.
func NewJournal(name string) (Journal, error) {
	registryMu.RLock()
	p, ok := journalProviders[name]
	registryMu.RUnlock()
	if ok {
		return p(), nil
	}
	registryMu.RLock()
	_, sqlOK := journalReg[name]
	registryMu.RUnlock()
	if sqlOK {
		return nil, fmt.Errorf("persistence: journal %q requires a *sql.DB; use NewJournalFromDB instead", name)
	}
	return nil, fmt.Errorf("persistence: no journal registered under %q", name)
}

// NewSnapshotStore looks up the SnapshotStoreProvider registered under name
// and returns a new SnapshotStore instance.
func NewSnapshotStore(name string) (SnapshotStore, error) {
	registryMu.RLock()
	p, ok := snapshotProviders[name]
	registryMu.RUnlock()
	if ok {
		return p(), nil
	}
	registryMu.RLock()
	_, sqlOK := snapshotReg[name]
	registryMu.RUnlock()
	if sqlOK {
		return nil, fmt.Errorf("persistence: snapshot store %q requires a *sql.DB; use NewSnapshotStoreFromDB instead", name)
	}
	return nil, fmt.Errorf("persistence: no snapshot store registered under %q", name)
}
