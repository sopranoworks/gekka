/*
 * auto_start_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"sync/atomic"
	"testing"

	hocon "github.com/sopranoworks/gekka-config"
)

// TestAutoStartJournals_EagerlyInstantiatesNamedPlugins verifies that
// AutoStartJournals invokes the registered JournalProvider for every name
// passed in and returns the resulting Journal map.  Corresponds to
// pekko.persistence.journal.auto-start-journals.
func TestAutoStartJournals_EagerlyInstantiatesNamedPlugins(t *testing.T) {
	var calls atomic.Int32
	mkProvider := func(name string) JournalProvider {
		return func(cfg hocon.Config) (Journal, error) {
			calls.Add(1)
			return NewInMemoryJournal(), nil
		}
	}
	RegisterJournalProvider("auto-start-test-1", mkProvider("auto-start-test-1"))
	RegisterJournalProvider("auto-start-test-2", mkProvider("auto-start-test-2"))

	out, err := AutoStartJournals([]string{"auto-start-test-1", "auto-start-test-2"}, hocon.Config{})
	if err != nil {
		t.Fatalf("AutoStartJournals returned error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("provider call count = %d, want 2", got)
	}
	if _, ok := out["auto-start-test-1"]; !ok {
		t.Error("missing auto-start-test-1 in result map")
	}
	if _, ok := out["auto-start-test-2"]; !ok {
		t.Error("missing auto-start-test-2 in result map")
	}
}

// TestAutoStartJournals_EmptyListIsNoOp verifies that an empty input list
// returns (nil, nil) and does not invoke any provider.
func TestAutoStartJournals_EmptyListIsNoOp(t *testing.T) {
	out, err := AutoStartJournals(nil, hocon.Config{})
	if err != nil {
		t.Fatalf("AutoStartJournals(nil) returned error: %v", err)
	}
	if out != nil {
		t.Errorf("AutoStartJournals(nil) returned non-nil map: %v", out)
	}
}

// TestAutoStartSnapshotStores_EagerlyInstantiatesNamedPlugins is the
// snapshot-store sibling of TestAutoStartJournals_EagerlyInstantiatesNamedPlugins.
// Corresponds to pekko.persistence.snapshot-store.auto-start-snapshot-stores.
func TestAutoStartSnapshotStores_EagerlyInstantiatesNamedPlugins(t *testing.T) {
	var calls atomic.Int32
	provider := func(cfg hocon.Config) (SnapshotStore, error) {
		calls.Add(1)
		return NewInMemorySnapshotStore(), nil
	}
	RegisterSnapshotStoreProvider("auto-start-snap-1", provider)
	RegisterSnapshotStoreProvider("auto-start-snap-2", provider)

	out, err := AutoStartSnapshotStores([]string{"auto-start-snap-1", "auto-start-snap-2"}, hocon.Config{})
	if err != nil {
		t.Fatalf("AutoStartSnapshotStores returned error: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("provider call count = %d, want 2", got)
	}
	if len(out) != 2 {
		t.Errorf("result map size = %d, want 2", len(out))
	}
}
