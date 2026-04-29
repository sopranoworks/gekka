/*
 * proxy_snapshot.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
)

// PersistencePluginProxy — snapshot-store half of
// pekko.persistence.snapshot-store.proxy.*.  Mirror of ProxyJournal: a
// thin SnapshotStore that forwards every operation to a target plugin
// resolved by name from the gekka persistence registry.  Pekko's
// reference implementation supports cross-process forwarding via
// Artery; this initial gekka port covers the in-process case
// end-to-end and rejects start-target-snapshot-store=off at
// construction time so misconfigurations fail fast.
//
// Configuration (under pekko.persistence.snapshot-store.proxy.*):
//   - target-snapshot-store-plugin-id (required) — registry name of
//     the real snapshot store the proxy delegates to.
//   - init-timeout (default 10s) — first-use resolution retry budget;
//     the proxy retries every 100 ms until the target is registered
//     or the budget is exhausted.  Once cached the resolution is
//     reused for every subsequent op.
//   - start-target-snapshot-store (default on) — only the in-process
//     value is implemented; off (cross-process Artery transport) is
//     rejected at construction time pending the transport.

// ProxySnapshotStore forwards SnapshotStore operations to a
// registry-resolved target.  Resolution is lazy and one-shot: the
// first operation that needs the target retries every 100 ms up to
// InitTimeout, after which the resolved reference is reused for the
// lifetime of the proxy.  All durability and selection semantics are
// inherited from the target.
type ProxySnapshotStore struct {
	// TargetPluginID is the registry name of the snapshot store the
	// proxy forwards to.
	TargetPluginID string

	// InitTimeout caps how long resolveTarget retries on first use.
	InitTimeout time.Duration

	// resolveOnce gates the single resolution attempt.
	resolveOnce sync.Once
	// target is set by resolveOnce; nil until resolution succeeds.
	target SnapshotStore
	// resolveErr captures the failure (if any) of the resolution
	// attempt and is returned to every operation thereafter.
	resolveErr error

	// targetOverride lets tests inject a pre-built target without
	// going through the registry.  Set by NewProxySnapshotStoreForTarget.
	// When non-nil, resolveTarget short-circuits to it.
	targetOverride SnapshotStore
}

// NewProxySnapshotStore returns a proxy that forwards to the registry
// plugin named targetPluginID, retrying resolution for up to
// initTimeout on first use.  Use this when wiring proxies
// programmatically; HOCON-configured proxies arrive through the
// registered "proxy" provider (see init below).
func NewProxySnapshotStore(targetPluginID string, initTimeout time.Duration) (*ProxySnapshotStore, error) {
	targetPluginID = strings.TrimSpace(targetPluginID)
	if targetPluginID == "" {
		return nil, fmt.Errorf("persistence: proxy snapshot store requires a non-empty target-snapshot-store-plugin-id")
	}
	if initTimeout <= 0 {
		initTimeout = 10 * time.Second
	}
	return &ProxySnapshotStore{
		TargetPluginID: targetPluginID,
		InitTimeout:    initTimeout,
	}, nil
}

// NewProxySnapshotStoreForTarget returns a proxy that always forwards
// to the supplied target, bypassing the plugin registry.  Useful in
// tests where wiring a real provider is overkill, and in code paths
// that already have a SnapshotStore in hand and want to layer the
// proxy indirection on top.
func NewProxySnapshotStoreForTarget(target SnapshotStore) *ProxySnapshotStore {
	return &ProxySnapshotStore{
		TargetPluginID: "<direct>",
		InitTimeout:    0,
		targetOverride: target,
	}
}

// resolveTarget performs the one-shot, retry-bounded lookup.  Returns
// nil on success and a permanent error on timeout — the same error is
// returned to every subsequent operation, so a misconfigured proxy
// fails loudly instead of trying again every call.
func (p *ProxySnapshotStore) resolveTarget(ctx context.Context) error {
	p.resolveOnce.Do(func() {
		if p.targetOverride != nil {
			p.target = p.targetOverride
			return
		}
		deadline := time.Now().Add(p.InitTimeout)
		var lastErr error
		for {
			t, err := NewSnapshotStore(p.TargetPluginID, hocon.Config{})
			if err == nil {
				p.target = t
				return
			}
			lastErr = err
			if !time.Now().Before(deadline) {
				p.resolveErr = fmt.Errorf("persistence: proxy snapshot store target %q not resolvable within %s: %w",
					p.TargetPluginID, p.InitTimeout, lastErr)
				return
			}
			select {
			case <-ctx.Done():
				p.resolveErr = ctx.Err()
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	return p.resolveErr
}

// LoadSnapshot forwards to the target snapshot store.
func (p *ProxySnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	if err := p.resolveTarget(ctx); err != nil {
		return nil, err
	}
	return p.target.LoadSnapshot(ctx, persistenceId, criteria)
}

// SaveSnapshot forwards to the target.
func (p *ProxySnapshotStore) SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.SaveSnapshot(ctx, metadata, snapshot)
}

// DeleteSnapshot forwards to the target.
func (p *ProxySnapshotStore) DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.DeleteSnapshot(ctx, metadata)
}

// DeleteSnapshots forwards to the target.
func (p *ProxySnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.DeleteSnapshots(ctx, persistenceId, criteria)
}

// Compile-time guard.
var _ SnapshotStore = (*ProxySnapshotStore)(nil)

// init registers the "proxy" SnapshotStoreProvider so HOCON-configured
// systems can wire the proxy without importing this file.  The
// provider reads target-snapshot-store-plugin-id, init-timeout, and
// start-target-snapshot-store from the HOCON sub-config at
// pekko.persistence.snapshot-store.proxy.*.
func init() {
	RegisterSnapshotStoreProvider("proxy", newProxySnapshotStoreFromConfig)
}

func newProxySnapshotStoreFromConfig(cfg hocon.Config) (SnapshotStore, error) {
	// hocon.Config{} (zero value) panics on Get* calls because its
	// internal value is nil.  Reuse the journal-side safeGet helpers
	// so a misconfigured or auto-started caller (AutoStartSnapshotStores
	// passes hocon.Config{} when no settings tree exists) gets a clean
	// error instead of a crash.
	target := strings.TrimSpace(safeGetString(cfg, "target-snapshot-store-plugin-id"))
	if target == "" {
		return nil, fmt.Errorf("persistence: proxy snapshot store requires 'target-snapshot-store-plugin-id' under pekko.persistence.snapshot-store.proxy")
	}
	if target == "proxy" {
		return nil, fmt.Errorf("persistence: proxy snapshot store target-snapshot-store-plugin-id cannot be 'proxy' (would loop)")
	}

	initTimeout := 10 * time.Second
	if d, ok := safeGetDuration(cfg, "init-timeout"); ok && d > 0 {
		initTimeout = d
	}

	startTarget := true
	switch strings.ToLower(strings.TrimSpace(safeGetString(cfg, "start-target-snapshot-store"))) {
	case "off", "false", "no", "0":
		startTarget = false
	}
	if !startTarget {
		return nil, fmt.Errorf("persistence: proxy snapshot store with start-target-snapshot-store=off requires Artery cross-process transport (not yet implemented in gekka)")
	}

	return NewProxySnapshotStore(target, initTimeout)
}
