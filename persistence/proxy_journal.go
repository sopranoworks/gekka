/*
 * proxy_journal.go
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

// PersistencePluginProxy — journal half of pekko.persistence.journal.proxy.*.
//
// The proxy is a thin Journal that forwards every operation to a target
// plugin resolved by name from the gekka persistence registry.  Pekko's
// reference implementation supports cross-process forwarding via Artery
// for "I want my actor to talk to another node's journal"; this initial
// gekka port covers the in-process case end-to-end.  Cross-process
// transport (start-target-journal = off) is rejected at construction
// time with a descriptive error so misconfigurations fail fast instead
// of silently routing to nothing.
//
// Configuration (under pekko.persistence.journal.proxy.*):
//   - target-journal-plugin-id (required) — the registry name of the
//     real journal the proxy delegates to (e.g. "in-memory", "postgres").
//   - init-timeout (default 10s) — how long the proxy will retry
//     resolving the target before failing the first request that needs
//     it.  Subsequent requests reuse the cached resolution.
//   - start-target-journal (default on) — when on, the proxy resolves
//     the target via the local plugin registry; when off, the proxy
//     would expect a remote target reachable through Artery.  The
//     remote path is not yet implemented and is rejected at startup.
//
// The proxy itself stores no state; it only forwards.  All durability,
// ordering, and replay semantics are inherited from the target.

// ProxyJournal forwards Journal operations to a registry-resolved target.
//
// Resolution is lazy: the first operation that needs the target triggers
// a one-shot lookup that retries every 100 ms up to InitTimeout.  This
// matches Pekko's behaviour where the proxy can be created before the
// target plugin's actor has finished starting, and the first user
// operation simply waits for the target to come online.  Once resolved
// the *Journal reference is reused for the lifetime of the proxy.
type ProxyJournal struct {
	// TargetPluginID is the registry name of the journal the proxy
	// forwards to.
	TargetPluginID string

	// InitTimeout caps how long resolveTarget retries on first use.
	InitTimeout time.Duration

	// resolveOnce gates the single resolution attempt.
	resolveOnce sync.Once
	// target is set by resolveOnce; nil until resolution succeeds.
	target Journal
	// resolveErr captures the failure (if any) of the resolution
	// attempt and is returned to every operation thereafter.
	resolveErr error

	// targetOverride lets tests inject a pre-built target without
	// going through the registry.  Set by NewProxyJournalForTarget.
	// When non-nil, resolveTarget short-circuits to it.
	targetOverride Journal
}

// NewProxyJournal returns a proxy that forwards to the registry plugin
// named targetPluginID, retrying resolution for up to initTimeout on
// first use.  Use this when wiring proxies programmatically; HOCON-
// configured proxies arrive through the registered "proxy" provider
// (see init below).
func NewProxyJournal(targetPluginID string, initTimeout time.Duration) (*ProxyJournal, error) {
	targetPluginID = strings.TrimSpace(targetPluginID)
	if targetPluginID == "" {
		return nil, fmt.Errorf("persistence: proxy journal requires a non-empty target-journal-plugin-id")
	}
	if initTimeout <= 0 {
		initTimeout = 10 * time.Second
	}
	return &ProxyJournal{
		TargetPluginID: targetPluginID,
		InitTimeout:    initTimeout,
	}, nil
}

// NewProxyJournalForTarget returns a proxy that always forwards to the
// supplied target, bypassing the plugin registry.  Useful in tests
// where wiring a real provider is overkill, and in code paths that
// already have a *Journal in hand and want to layer the proxy
// indirection on top.
func NewProxyJournalForTarget(target Journal) *ProxyJournal {
	return &ProxyJournal{
		TargetPluginID: "<direct>",
		InitTimeout:    0,
		targetOverride: target,
		// Mark resolution as a no-op-success so resolveTarget skips
		// the registry path.
	}
}

// resolveTarget performs the one-shot, retry-bounded lookup.  Returns
// nil on success and a permanent error on timeout — the same error is
// returned to every subsequent operation, so a misconfigured proxy
// fails loudly instead of trying again every call.
func (p *ProxyJournal) resolveTarget(ctx context.Context) error {
	p.resolveOnce.Do(func() {
		if p.targetOverride != nil {
			p.target = p.targetOverride
			return
		}
		deadline := time.Now().Add(p.InitTimeout)
		var lastErr error
		for {
			t, err := NewJournal(p.TargetPluginID, hocon.Config{})
			if err == nil {
				p.target = t
				return
			}
			lastErr = err
			if !time.Now().Before(deadline) {
				p.resolveErr = fmt.Errorf("persistence: proxy journal target %q not resolvable within %s: %w",
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

// AsyncWriteMessages forwards the batch to the target journal once
// resolution completes.
func (p *ProxyJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.AsyncWriteMessages(ctx, messages)
}

// AsyncDeleteMessagesTo forwards to the target.
func (p *ProxyJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.AsyncDeleteMessagesTo(ctx, persistenceId, toSequenceNr)
}

// ReplayMessages forwards to the target.
func (p *ProxyJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr, max uint64, callback func(PersistentRepr)) error {
	if err := p.resolveTarget(ctx); err != nil {
		return err
	}
	return p.target.ReplayMessages(ctx, persistenceId, fromSequenceNr, toSequenceNr, max, callback)
}

// ReadHighestSequenceNr forwards to the target.
func (p *ProxyJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	if err := p.resolveTarget(ctx); err != nil {
		return 0, err
	}
	return p.target.ReadHighestSequenceNr(ctx, persistenceId, fromSequenceNr)
}

// Compile-time guard.
var _ Journal = (*ProxyJournal)(nil)

// init registers the "proxy" JournalProvider so HOCON-configured
// systems can wire the proxy without importing this file.  The provider
// reads target-journal-plugin-id, init-timeout, and start-target-journal
// from the HOCON sub-config at pekko.persistence.journal.proxy.*.
func init() {
	RegisterJournalProvider("proxy", newProxyJournalFromConfig)
}

func newProxyJournalFromConfig(cfg hocon.Config) (Journal, error) {
	// hocon.Config{} (zero value) panics on Get* calls because its
	// internal value is nil.  Guard each access so a misconfigured or
	// auto-started caller (AutoStartJournals passes hocon.Config{} when
	// no settings tree exists) gets a clean error instead of a crash.
	target := safeGetString(cfg, "target-journal-plugin-id")
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, fmt.Errorf("persistence: proxy journal requires 'target-journal-plugin-id' under pekko.persistence.journal.proxy")
	}
	if target == "proxy" {
		return nil, fmt.Errorf("persistence: proxy journal target-journal-plugin-id cannot be 'proxy' (would loop)")
	}

	initTimeout := 10 * time.Second
	if d, ok := safeGetDuration(cfg, "init-timeout"); ok && d > 0 {
		initTimeout = d
	}

	startTarget := true
	switch strings.ToLower(strings.TrimSpace(safeGetString(cfg, "start-target-journal"))) {
	case "off", "false", "no", "0":
		startTarget = false
	}
	if !startTarget {
		return nil, fmt.Errorf("persistence: proxy journal with start-target-journal=off requires Artery cross-process transport (not yet implemented in gekka)")
	}

	return NewProxyJournal(target, initTimeout)
}

// safeGetString invokes cfg.GetString with a panic guard; the hocon
// library panics on a zero-value Config, which is what AutoStartJournals
// passes when no settings tree was configured.  Returning "" for both
// missing-key and zero-config lets callers treat absence uniformly.
func safeGetString(cfg hocon.Config, key string) string {
	defer func() { _ = recover() }()
	v, err := cfg.GetString(key)
	if err != nil {
		return ""
	}
	return v
}

// safeGetDuration is the duration counterpart to safeGetString.  ok is
// false when the key is missing, malformed, or the config is unset.
func safeGetDuration(cfg hocon.Config, key string) (time.Duration, bool) {
	var (
		d   time.Duration
		err error
	)
	func() {
		defer func() { _ = recover() }()
		d, err = cfg.GetDuration(key)
	}()
	if err != nil {
		return 0, false
	}
	return d, true
}
