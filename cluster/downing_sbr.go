/*
 * downing_sbr.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// Round-2 session 27 — F4 Downing: SBR registers as the default
// DowningProvider.  The SBR manager keeps owning its own decision
// loop; this file is the thin adapter that exposes it through the
// DowningProvider interface so cluster.go can resolve "split-brain-
// resolver" by name and call .Start uniformly across providers.
//
// Round-2 session 28 — consolidation: ResolveSBRConfigDefaults +
// BuildSBRDowningProvider were lifted out of gekka.NewCluster so the
// lease-majority sbrCfg-defaulting and adapter-wrapping live next to
// the SBR adapter itself.  gekka.NewCluster now hands raw config
// values to BuildSBRDowningProvider and gets back a ready-to-register
// provider — eliminating the only remaining direct SBRManager call
// site from the top-level wiring.

// SBRDowningProvider adapts SBRManager to the DowningProvider contract.
// Embeds nothing; only holds a pointer to the manager so the adapter
// stays cheap and re-creatable in tests.
type SBRDowningProvider struct {
	mgr *SBRManager
}

// NewSBRDowningProvider returns a provider that runs mgr's decision
// loop on Start.  mgr may be nil — in that case Start is a successful
// no-op so a cluster with `downing-provider-class = ...SplitBrainResolverProvider`
// but `split-brain-resolver.active-strategy = ""` still satisfies the
// "registered provider" expectation without throwing.
func NewSBRDowningProvider(mgr *SBRManager) *SBRDowningProvider {
	return &SBRDowningProvider{mgr: mgr}
}

// Name returns the registry short name for the bundled SBR provider.
// Stable across releases; mirrors Pekko's convention of registering
// SBR as the canonical downing provider.
func (p *SBRDowningProvider) Name() string { return DefaultDowningProviderName }

// Start runs the SBR decision loop.  When the provider was constructed
// without a manager (active-strategy="") Start returns immediately so
// the registry still resolves the canonical name without crashing on
// SBR-disabled clusters.
//
// SBRManager.Start blocks until ctx is cancelled, so we run it in a
// goroutine and return nil immediately — keeping the DowningProvider
// API simple for future providers that may launch one or many
// background tasks of their own.
func (p *SBRDowningProvider) Start(ctx context.Context) error {
	if p.mgr == nil {
		return nil
	}
	go p.mgr.Start(ctx)
	return nil
}

// Manager exposes the wrapped SBRManager for tests and for callers
// that need to access SBR-specific knobs (e.g. SetInfraProvider).
// Returns nil when the provider was constructed with a nil manager.
func (p *SBRDowningProvider) Manager() *SBRManager { return p.mgr }

// SBRDefaults captures the values gekka.NewCluster used to fill
// lease-majority blank fields on SBRConfig.  Round-2 session 28 lifts
// those defaults out of cluster.go and into ResolveSBRConfigDefaults
// so the only remaining SBRManager construction site is in this
// package.  Empty fields fall back to Pekko reference defaults
// (LeaseDuration = 120s, RetryInterval = 12s).
type SBRDefaults struct {
	// LeaseManager is the registry the lease-majority strategy
	// resolves provider names against.  Defaults to nil; the caller
	// must supply gekka.LeaseManager() when lease-majority is in play.
	LeaseManager *icluster.LeaseManager
	// LeaseProviderName is the registered LeaseProvider name to use
	// when SBRConfig.LeaseImplementation is empty.  Typically
	// lease.MemoryProviderName.
	LeaseProviderName string
	// SystemName / Host / Port build the conventional default
	// LeaseName ("<system>-pekko-sbr") and OwnerName ("host:port").
	SystemName string
	Host       string
	Port       uint32
	// LeaseDuration / RetryInterval default to the values pulled from
	// pekko.coordination.lease.heartbeat-{timeout,interval} when the
	// SBRConfig.LeaseSettings fields are zero.
	LeaseDuration time.Duration
	RetryInterval time.Duration
}

// ResolveSBRConfigDefaults fills the lease-majority blanks on cfg
// using d when the active strategy is "lease-majority".  Pure
// function: returns a new SBRConfig and never mutates the caller's
// copy.  No-op when the active strategy isn't lease-majority so non-
// lease strategies keep flowing through the same code path.
func ResolveSBRConfigDefaults(cfg SBRConfig, d SBRDefaults) SBRConfig {
	if !strings.EqualFold(strings.TrimSpace(cfg.ActiveStrategy), "lease-majority") {
		return cfg
	}
	if cfg.LeaseImplementation == "" {
		cfg.LeaseImplementation = d.LeaseProviderName
	}
	if cfg.LeaseManager == nil {
		cfg.LeaseManager = d.LeaseManager
	}
	if cfg.LeaseSettings.LeaseName == "" && d.SystemName != "" {
		cfg.LeaseSettings.LeaseName = d.SystemName + "-pekko-sbr"
	}
	if cfg.LeaseSettings.OwnerName == "" && d.Host != "" {
		cfg.LeaseSettings.OwnerName = fmt.Sprintf("%s:%d", d.Host, d.Port)
	}
	if cfg.LeaseSettings.LeaseDuration == 0 {
		dur := d.LeaseDuration
		if dur == 0 {
			dur = 120 * time.Second
		}
		cfg.LeaseSettings.LeaseDuration = dur
	}
	if cfg.LeaseSettings.RetryInterval == 0 {
		ri := d.RetryInterval
		if ri == 0 {
			ri = 12 * time.Second
		}
		cfg.LeaseSettings.RetryInterval = ri
	}
	return cfg
}

// BuildSBRDowningProvider is the one-shot factory gekka.NewCluster
// calls to obtain a ready-to-register downing provider.  It applies
// lease-majority defaults (via ResolveSBRConfigDefaults), constructs
// the underlying SBRManager (which itself returns nil when SBR is
// disabled), and wraps the result in SBRDowningProvider.  Round-2
// session 28: this consolidates the two-step "fill defaults +
// construct manager + wrap adapter" dance previously inlined in
// cluster.go.
func BuildSBRDowningProvider(cm *ClusterManager, cfg SBRConfig, d SBRDefaults) *SBRDowningProvider {
	cfg = ResolveSBRConfigDefaults(cfg, d)
	return NewSBRDowningProvider(NewSBRManager(cm, cfg))
}
