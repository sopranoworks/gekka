/*
 * proxy_snapshot_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
)

// countingSnapshotStore wraps an InMemorySnapshotStore and records each
// call so tests can assert that proxy operations actually flowed through
// to the target.
type countingSnapshotStore struct {
	inner     *InMemorySnapshotStore
	loads     atomic.Int64
	saves     atomic.Int64
	deletes   atomic.Int64
	delMulti  atomic.Int64
}

func newCountingSnapshotStore() *countingSnapshotStore {
	return &countingSnapshotStore{inner: NewInMemorySnapshotStore()}
}

func (s *countingSnapshotStore) LoadSnapshot(ctx context.Context, id string, c SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	s.loads.Add(1)
	return s.inner.LoadSnapshot(ctx, id, c)
}

func (s *countingSnapshotStore) SaveSnapshot(ctx context.Context, m SnapshotMetadata, snap any) error {
	s.saves.Add(1)
	return s.inner.SaveSnapshot(ctx, m, snap)
}

func (s *countingSnapshotStore) DeleteSnapshot(ctx context.Context, m SnapshotMetadata) error {
	s.deletes.Add(1)
	return s.inner.DeleteSnapshot(ctx, m)
}

func (s *countingSnapshotStore) DeleteSnapshots(ctx context.Context, id string, c SnapshotSelectionCriteria) error {
	s.delMulti.Add(1)
	return s.inner.DeleteSnapshots(ctx, id, c)
}

var _ SnapshotStore = (*countingSnapshotStore)(nil)

// TestProxySnapshotStore_ForwardsToTarget exercises every SnapshotStore
// method and verifies the call lands on the target.
func TestProxySnapshotStore_ForwardsToTarget(t *testing.T) {
	target := newCountingSnapshotStore()
	proxy := NewProxySnapshotStoreForTarget(target)

	ctx := context.Background()
	meta := SnapshotMetadata{PersistenceID: "p-1", SequenceNr: 1, Timestamp: 100}
	if err := proxy.SaveSnapshot(ctx, meta, "hello"); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if got := target.saves.Load(); got != 1 {
		t.Errorf("target saves = %d, want 1", got)
	}

	loaded, err := proxy.LoadSnapshot(ctx, "p-1", LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded == nil || loaded.Snapshot != "hello" {
		t.Errorf("loaded = %+v, want {hello}", loaded)
	}
	if got := target.loads.Load(); got != 1 {
		t.Errorf("target loads = %d, want 1", got)
	}

	if err := proxy.DeleteSnapshot(ctx, meta); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if got := target.deletes.Load(); got != 1 {
		t.Errorf("target deletes = %d, want 1", got)
	}

	if err := proxy.DeleteSnapshots(ctx, "p-1", LatestSnapshotCriteria()); err != nil {
		t.Fatalf("DeleteSnapshots: %v", err)
	}
	if got := target.delMulti.Load(); got != 1 {
		t.Errorf("target delMulti = %d, want 1", got)
	}
}

// TestProxySnapshotStore_ResolvesViaRegistry verifies the HOCON-driven
// path: the "proxy" provider reads target-snapshot-store-plugin-id and
// resolves against the global registry.
func TestProxySnapshotStore_ResolvesViaRegistry(t *testing.T) {
	cfg, err := hocon.ParseString(`
target-snapshot-store-plugin-id = "in-memory"
init-timeout = 1s
`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}

	s, err := NewSnapshotStore("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewSnapshotStore(proxy): %v", err)
	}

	ctx := context.Background()
	meta := SnapshotMetadata{PersistenceID: "p-1", SequenceNr: 1, Timestamp: 1}
	if err := s.SaveSnapshot(ctx, meta, "v"); err != nil {
		t.Fatalf("save through registry-resolved proxy: %v", err)
	}
	loaded, err := s.LoadSnapshot(ctx, "p-1", LatestSnapshotCriteria())
	if err != nil || loaded == nil || loaded.Snapshot != "v" {
		t.Fatalf("loaded=%+v err=%v, want {v} / nil", loaded, err)
	}
}

// TestProxySnapshotStore_InitTimeoutFires verifies that an unresolvable
// target surfaces a clear timeout error rather than retrying forever.
func TestProxySnapshotStore_InitTimeoutFires(t *testing.T) {
	proxy, err := NewProxySnapshotStore("does-not-exist", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewProxySnapshotStore: %v", err)
	}

	ctx := context.Background()
	start := time.Now()
	werr := proxy.SaveSnapshot(ctx, SnapshotMetadata{PersistenceID: "x", SequenceNr: 1}, "x")
	elapsed := time.Since(start)

	if werr == nil {
		t.Fatal("expected init-timeout error, got nil")
	}
	if !strings.Contains(werr.Error(), "not resolvable within") {
		t.Errorf("error %q does not mention init-timeout", werr)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned too quickly (%s) — init-timeout should have retried", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("returned too slowly (%s) — init-timeout retry overshot", elapsed)
	}
}

// TestProxySnapshotStore_CachesFirstResolution verifies that repeated
// calls after a successful resolution reuse the cached target — the
// init-timeout is paid at most once.
func TestProxySnapshotStore_CachesFirstResolution(t *testing.T) {
	target := newCountingSnapshotStore()
	proxy := NewProxySnapshotStoreForTarget(target)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := proxy.LoadSnapshot(ctx, "p", LatestSnapshotCriteria()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := target.loads.Load(); got != 5 {
		t.Errorf("expected 5 forwarded LoadSnapshot calls, got %d", got)
	}
}

// TestProxySnapshotStore_RejectsRemoteMode verifies that
// start-target-snapshot-store=off (which would require Artery transport
// not yet implemented) is rejected at construction time with a clear
// error.
func TestProxySnapshotStore_RejectsRemoteMode(t *testing.T) {
	cfg, err := hocon.ParseString(`
target-snapshot-store-plugin-id = "in-memory"
start-target-snapshot-store = off
`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewSnapshotStore("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for start-target-snapshot-store=off, got nil")
	}
	if !strings.Contains(err.Error(), "start-target-snapshot-store=off") {
		t.Errorf("error %q does not mention the rejected option", err)
	}
}

// TestProxySnapshotStore_RejectsSelfTarget verifies the proxy refuses
// to target itself — guarding against an infinite-recursion misconfig.
func TestProxySnapshotStore_RejectsSelfTarget(t *testing.T) {
	cfg, err := hocon.ParseString(`target-snapshot-store-plugin-id = "proxy"`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewSnapshotStore("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for proxy targeting itself, got nil")
	}
	if !strings.Contains(err.Error(), "loop") {
		t.Errorf("error %q does not mention the loop guard", err)
	}
}

// TestProxySnapshotStore_RequiresTargetID verifies missing target id
// surfaces a descriptive error pointing to the HOCON path that needs
// setting.
func TestProxySnapshotStore_RequiresTargetID(t *testing.T) {
	cfg, err := hocon.ParseString(`init-timeout = 1s`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewSnapshotStore("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for missing target-snapshot-store-plugin-id, got nil")
	}
	if !strings.Contains(err.Error(), "target-snapshot-store-plugin-id") {
		t.Errorf("error %q does not mention the missing key", err)
	}
}

// TestProxySnapshotStore_HandlesZeroValueConfig verifies the safe-get
// helpers turn a zero hocon.Config{} into a clean error instead of a
// panic.
func TestProxySnapshotStore_HandlesZeroValueConfig(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero config panicked: %v", r)
		}
	}()
	_, err := NewSnapshotStore("proxy", hocon.Config{})
	if err == nil {
		t.Fatal("expected error for missing target-snapshot-store-plugin-id with zero config, got nil")
	}
	if !strings.Contains(err.Error(), "target-snapshot-store-plugin-id") {
		t.Errorf("error %q does not mention the missing key", err)
	}
}
