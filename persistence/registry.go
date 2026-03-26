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
	"sort"
	"strings"
	"sync"

	hocon "github.com/sopranoworks/gekka-config"
)

// ── Config validation ─────────────────────────────────────────────────────────

// ValidateProviderConfig checks that every key in requiredKeys is present and
// non-empty in cfg.  Returns a descriptive error listing every missing key and
// the HOCON path at which each one should be set.
//
// Use this at the top of a JournalProvider or SnapshotStoreProvider factory to
// fail fast before any I/O is attempted:
//
//	persistence.RegisterJournalProvider("mybackend", func(cfg hocon.Config) (persistence.Journal, error) {
//	    if err := persistence.ValidateProviderConfig(cfg, "host", "port", "database"); err != nil {
//	        return nil, err
//	    }
//	    // ... create the client
//	})
func ValidateProviderConfig(cfg hocon.Config, requiredKeys ...string) error {
	var missing []string
	for _, key := range requiredKeys {
		v, err := cfg.GetString(key)
		if err != nil || strings.TrimSpace(v) == "" {
			missing = append(missing, "'"+key+"'")
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("persistence: missing required configuration keys: [%s]. Set them under persistence.journal.settings (or snapshot-store.settings) in your HOCON file",
			strings.Join(missing, ", "))
	}
	return nil
}

// ── SQL-backed factories ──────────────────────────────────────────────────────

// JournalFactory creates a Journal backed by a pre-opened *sql.DB.
// Used by SQL backends (postgres, mysql). Register with RegisterJournal.
type JournalFactory func(db *sql.DB) Journal

// SnapshotStoreFactory creates a SnapshotStore backed by a pre-opened *sql.DB.
type SnapshotStoreFactory func(db *sql.DB) SnapshotStore

// DurableStateStoreFactory creates a DurableStateStore backed by a pre-opened *sql.DB.
type DurableStateStoreFactory func(db *sql.DB) DurableStateStore

// ── Config-driven providers ───────────────────────────────────────────────────

// JournalProvider creates a Journal from a HOCON sub-config.
// The config is the value at persistence.journal.settings in the actor system
// configuration.  Self-contained backends (e.g. "in-memory") may ignore it.
type JournalProvider func(cfg hocon.Config) (Journal, error)

// SnapshotStoreProvider creates a SnapshotStore from a HOCON sub-config.
type SnapshotStoreProvider func(cfg hocon.Config) (SnapshotStore, error)

// DurableStateStoreProvider creates a DurableStateStore from a HOCON sub-config.
// The config is the value at persistence.state-store.settings in the actor system
// configuration.  Self-contained backends may ignore it.
type DurableStateStoreProvider func(cfg hocon.Config) (DurableStateStore, error)

// ReadJournalProvider creates a ReadJournal from a HOCON sub-config.
type ReadJournalProvider func(cfg hocon.Config) (any, error)

var (
	registryMu      sync.RWMutex
	journalReg      = make(map[string]JournalFactory)
	snapshotReg     = make(map[string]SnapshotStoreFactory)
	durableStateReg = make(map[string]DurableStateStoreFactory)

	journalProviders      = make(map[string]JournalProvider)
	snapshotProviders     = make(map[string]SnapshotStoreProvider)
	durableStateProviders = make(map[string]DurableStateStoreProvider)
	readJournalProviders  = make(map[string]ReadJournalProvider)
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

// JournalNames returns the names of all registered SQL-backed Journal factories.
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

// JournalProviderNames returns the names of all registered config-driven Journal providers.
func JournalProviderNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(journalProviders))
	for n := range journalProviders {
		names = append(names, n)
	}
	return names
}

// SnapshotStoreProviderNames returns the names of all registered config-driven SnapshotStore providers.
func SnapshotStoreProviderNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(snapshotProviders))
	for n := range snapshotProviders {
		names = append(names, n)
	}
	return names
}

// ── Provider registration ─────────────────────────────────────────────────────

// RegisterJournalProvider registers a config-driven JournalProvider under name.
// The factory receives the HOCON sub-config at persistence.journal.settings.
//
// The built-in "in-memory" provider is pre-registered automatically.
func RegisterJournalProvider(name string, provider JournalProvider) {
	registryMu.Lock()
	journalProviders[name] = provider
	registryMu.Unlock()
}

// RegisterSnapshotStoreProvider registers a config-driven SnapshotStoreProvider under name.
func RegisterSnapshotStoreProvider(name string, provider SnapshotStoreProvider) {
	registryMu.Lock()
	snapshotProviders[name] = provider
	registryMu.Unlock()
}

// RegisterDurableStateStoreProvider registers a config-driven DurableStateStoreProvider under name.
// The factory receives the HOCON sub-config at persistence.state-store.settings.
func RegisterDurableStateStoreProvider(name string, provider DurableStateStoreProvider) {
	registryMu.Lock()
	durableStateProviders[name] = provider
	registryMu.Unlock()
}

// RegisterReadJournalProvider registers a config-driven ReadJournalProvider under name.
func RegisterReadJournalProvider(name string, provider ReadJournalProvider) {
	registryMu.Lock()
	readJournalProviders[name] = provider
	registryMu.Unlock()
}

// NewJournal looks up the JournalProvider registered under name, passes cfg to
// it, and returns the resulting Journal.  cfg is the HOCON sub-config at
// persistence.journal.settings; backends that need no configuration may ignore it.
//
// Use the name "in-memory" for the built-in in-process journal.
func NewJournal(name string, cfg hocon.Config) (Journal, error) {
	registryMu.RLock()
	p, ok := journalProviders[name]
	registryMu.RUnlock()
	if ok {
		return p(cfg)
	}
	registryMu.RLock()
	_, sqlOK := journalReg[name]
	available := journalProviderNames()
	registryMu.RUnlock()
	if sqlOK {
		return nil, fmt.Errorf("persistence: journal %q requires a *sql.DB; use NewJournalFromDB instead", name)
	}
	return nil, fmt.Errorf("persistence: journal provider %q not found — available providers: [%s]. Did you forget a blank import?",
		name, strings.Join(available, ", "))
}

// NewSnapshotStore looks up the SnapshotStoreProvider registered under name,
// passes cfg to it, and returns the resulting SnapshotStore.
func NewSnapshotStore(name string, cfg hocon.Config) (SnapshotStore, error) {
	registryMu.RLock()
	p, ok := snapshotProviders[name]
	registryMu.RUnlock()
	if ok {
		return p(cfg)
	}
	registryMu.RLock()
	_, sqlOK := snapshotReg[name]
	available := snapshotProviderNames()
	registryMu.RUnlock()
	if sqlOK {
		return nil, fmt.Errorf("persistence: snapshot store %q requires a *sql.DB; use NewSnapshotStoreFromDB instead", name)
	}
	return nil, fmt.Errorf("persistence: snapshot store provider %q not found — available providers: [%s]. Did you forget a blank import?",
		name, strings.Join(available, ", "))
}

// journalProviderNames returns sorted provider names; must be called with registryMu held.
func journalProviderNames() []string {
	names := make([]string, 0, len(journalProviders))
	for n := range journalProviders {
		names = append(names, "'"+n+"'")
	}
	sort.Strings(names)
	return names
}

// snapshotProviderNames returns sorted provider names; must be called with registryMu held.
func snapshotProviderNames() []string {
	names := make([]string, 0, len(snapshotProviders))
	for n := range snapshotProviders {
		names = append(names, "'"+n+"'")
	}
	sort.Strings(names)
	return names
}

// NewDurableStateStore looks up the DurableStateStoreProvider registered under name,
// passes cfg to it, and returns the resulting DurableStateStore.
func NewDurableStateStore(name string, cfg hocon.Config) (DurableStateStore, error) {
	registryMu.RLock()
	p, ok := durableStateProviders[name]
	registryMu.RUnlock()
	if ok {
		return p(cfg)
	}
	registryMu.RLock()
	_, sqlOK := durableStateReg[name]
	available := durableStateProviderNames()
	registryMu.RUnlock()
	if sqlOK {
		return nil, fmt.Errorf("persistence: durable state store %q requires a *sql.DB; use NewDurableStateStoreFromDB instead", name)
	}
	return nil, fmt.Errorf("persistence: durable state store provider %q not found — available providers: [%s]. Did you forget a blank import?",
		name, strings.Join(available, ", "))
}

// NewReadJournal looks up the ReadJournalProvider registered under name,
// passes cfg to it, and returns the resulting ReadJournal instance.
// The return type is any because concrete journals might implement different
// subsets of query interfaces.
func NewReadJournal(name string, cfg hocon.Config) (any, error) {
	registryMu.RLock()
	p, ok := readJournalProviders[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence: read journal provider %q not found", name)
	}
	return p(cfg)
}

// DurableStateStoreProviderNames returns the names of all registered config-driven DurableStateStore providers.
func DurableStateStoreProviderNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return durableStateProviderNames()
}

// durableStateProviderNames returns sorted provider names; must be called with registryMu held.
func durableStateProviderNames() []string {
	names := make([]string, 0, len(durableStateProviders))
	for n := range durableStateProviders {
		names = append(names, "'"+n+"'")
	}
	sort.Strings(names)
	return names
}
