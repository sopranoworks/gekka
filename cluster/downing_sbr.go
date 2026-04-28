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
)

// Round-2 session 27 — F4 Downing: SBR registers as the default
// DowningProvider.  The SBR manager keeps owning its own decision
// loop; this file is the thin adapter that exposes it through the
// DowningProvider interface so cluster.go can resolve "split-brain-
// resolver" by name and call .Start uniformly across providers.
//
// We intentionally keep SBRManager construction outside this adapter:
// the gekka.NewCluster wiring already builds the SBRManager with
// lease-majority defaults filled in, and rebuilding it here would
// duplicate that path.  Instead the wiring code constructs the
// manager (or accepts a nil) and hands it to NewSBRDowningProvider.
//
// Session 28 will route ALL downing through provider.Start; for S27
// the cluster_manager call site still launches the manager directly
// in addition to registering it here, so the two paths run side by
// side as a safety net while the interface settles.

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
