/*
 * auto_start.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
)

// ── Round-2 session 17: persistence small features ──────────────────────────
//
// The package-level vars below mirror the corresponding HOCON paths and are
// populated by gekka.LoadConfig at cluster bring-up.  They surface tunables
// that historically lived only in Pekko reference.conf so that existing
// consumers (typed event-sourced behaviors, persistent FSMs, durable-state
// fallback paths) can read a single source of truth.

var (
	// AutoMigrateManifest is the manifest written into snapshot envelopes
	// when migrating a legacy class-named snapshot to a manifest-coded one.
	// HOCON: pekko.persistence.snapshot-store.auto-migrate-manifest
	// Default: "pekko".
	autoMigrateManifest atomic.Value // string

	// statePluginFallbackRecoveryTimeout caps how long the durable-state
	// plugin fallback waits for recovery before failing.
	// HOCON: pekko.persistence.state-plugin-fallback.recovery-timeout
	// Default: 30s.
	statePluginFallbackRecoveryTimeoutNanos atomic.Int64

	// defaultFSMSnapshotAfter is the package-default snapshot-after value
	// for PersistentFSM. 0 disables auto-snapshot.
	// HOCON: pekko.persistence.fsm.snapshot-after.
	defaultFSMSnapshotAfter atomic.Int64
)

func init() {
	autoMigrateManifest.Store("pekko")
	statePluginFallbackRecoveryTimeoutNanos.Store(int64(30 * time.Second))
}

// SetAutoMigrateManifest installs the manifest used by the snapshot-store
// migration path. Callers should normally pass a non-empty string; setting
// "" reverts to the Pekko default ("pekko").
func SetAutoMigrateManifest(manifest string) {
	if manifest == "" {
		manifest = "pekko"
	}
	autoMigrateManifest.Store(manifest)
}

// GetAutoMigrateManifest returns the configured snapshot-migration manifest.
func GetAutoMigrateManifest() string {
	v, _ := autoMigrateManifest.Load().(string)
	if v == "" {
		return "pekko"
	}
	return v
}

// SetStatePluginFallbackRecoveryTimeout installs the recovery-timeout used
// by the durable-state plugin fallback path. Non-positive values are ignored.
func SetStatePluginFallbackRecoveryTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	statePluginFallbackRecoveryTimeoutNanos.Store(int64(d))
}

// GetStatePluginFallbackRecoveryTimeout returns the configured state-plugin
// fallback recovery timeout (defaults to 30s).
func GetStatePluginFallbackRecoveryTimeout() time.Duration {
	v := statePluginFallbackRecoveryTimeoutNanos.Load()
	if v <= 0 {
		return 30 * time.Second
	}
	return time.Duration(v)
}

// SetDefaultFSMSnapshotAfter installs the package-default snapshot-after value
// for PersistentFSM instances. n <= 0 disables auto-snapshot for new FSMs.
//
// HOCON: pekko.persistence.fsm.snapshot-after
func SetDefaultFSMSnapshotAfter(n int) {
	if n < 0 {
		n = 0
	}
	defaultFSMSnapshotAfter.Store(int64(n))
}

// GetDefaultFSMSnapshotAfter returns the package-default snapshot-after value.
// 0 means auto-snapshot is disabled.
func GetDefaultFSMSnapshotAfter() int {
	v := defaultFSMSnapshotAfter.Load()
	if v < 0 {
		return 0
	}
	return int(v)
}

// ── Auto-start helpers ──────────────────────────────────────────────────────

// AutoStartJournals eagerly instantiates a Journal for every name in names by
// looking up the registered JournalProvider and invoking it with cfg. The
// returned map is keyed by provider name.  Empty input returns (nil, nil).
//
// Errors from individual providers are accumulated and returned as a joined
// error; the map still contains successfully-instantiated journals.
func AutoStartJournals(names []string, cfg hocon.Config) (map[string]Journal, error) {
	if len(names) == 0 {
		return nil, nil
	}
	out := make(map[string]Journal, len(names))
	var firstErr error
	var seen sync.Mutex
	for _, name := range names {
		j, err := NewJournal(name, cfg)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("auto-start journal %q: %w", name, err)
			}
			continue
		}
		seen.Lock()
		out[name] = j
		seen.Unlock()
	}
	return out, firstErr
}

// AutoStartSnapshotStores is the snapshot-store sibling of AutoStartJournals.
func AutoStartSnapshotStores(names []string, cfg hocon.Config) (map[string]SnapshotStore, error) {
	if len(names) == 0 {
		return nil, nil
	}
	out := make(map[string]SnapshotStore, len(names))
	var firstErr error
	var seen sync.Mutex
	for _, name := range names {
		s, err := NewSnapshotStore(name, cfg)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("auto-start snapshot-store %q: %w", name, err)
			}
			continue
		}
		seen.Lock()
		out[name] = s
		seen.Unlock()
	}
	return out, firstErr
}
