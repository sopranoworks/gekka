/*
 * downing.go
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
	"sync"
)

// Round-2 session 27 — F4 Downing: plugin interface + registration.
//
// Pekko allows operators to swap the cluster's downing provider via
// `pekko.cluster.downing-provider-class`.  gekka mirrors that surface
// with a small Go-native registry: providers register by short name
// (Pekko ships `SplitBrainResolverProvider`; gekka ships its own SBR
// under the canonical short name `split-brain-resolver`), and the
// gekka top-level wiring resolves the configured name at startup.
//
// The registry purposely only handles registration + resolution.
// Decision-loop ownership stays with each provider — SBRDowningProvider
// (downing_sbr.go) wraps SBRManager.Start; future providers can ship
// their own goroutine.  Session 28 will move the cluster_manager call
// site behind this interface so ALL downing decisions flow through
// `provider.Start`; for now the interface and registration land first
// so that machinery is in place.
//
// FQCN inputs (`org.apache.pekko.cluster.sbr.SplitBrainResolverProvider`)
// are normalised to the gekka short name at parse time so configs
// imported verbatim from a Pekko deployment continue to resolve.

// DefaultDowningProviderName is the registry key that always points at
// gekka's bundled Split Brain Resolver implementation.  Operators who
// haven't customised `pekko.cluster.downing-provider-class` end up
// here.  Mirrors Pekko's default of registering SBR as the only known
// provider when the class name is empty.
const DefaultDowningProviderName = "split-brain-resolver"

// DowningProvider is the contract a downing implementation satisfies to
// be selectable via `pekko.cluster.downing-provider-class`.  The
// interface is intentionally narrow — providers own their own decision
// loop and lifecycle so future implementations (e.g. lease-driven
// downing, infra-confirmed downing) can plug in without reshaping the
// cluster manager.
//
// Lifecycle:
//   - Name() must return a stable short name; the registry uses it as
//     the lookup key and exposes it in logs.
//   - Start(ctx) is invoked once after the cluster manager is wired up.
//     The provider should run its own decision loop (typically as a
//     goroutine) and stop cleanly when ctx is cancelled.
//   - Start MAY return immediately when the provider has nothing to do
//     (e.g. SBR with ActiveStrategy="" returns nil).  A nil error is
//     therefore not a guarantee that work was actually scheduled.
type DowningProvider interface {
	// Name is the short identifier the registry stores the provider
	// under.  Conventionally lower-case-with-dashes; matches the value
	// HOCON's `downing-provider-class` resolves to after FQCN
	// normalisation.
	Name() string

	// Start runs the provider's decision loop.  Called exactly once
	// per gekka.NewCluster invocation, after ClusterManager is fully
	// constructed.  The provider must honour ctx cancellation.
	Start(ctx context.Context) error
}

// DowningProviderRegistry is the lookup table for DowningProvider
// implementations.  Mirrors LeaseManager: providers register by name
// at process boot, and Resolve answers the runtime "which provider did
// the operator pick" question.
//
// All operations are safe for concurrent use; in practice registration
// happens once per provider during init/wiring and Resolve fires once
// during NewCluster, so contention is theoretical.
type DowningProviderRegistry struct {
	mu        sync.RWMutex
	providers map[string]DowningProvider
}

// NewDowningProviderRegistry returns an empty registry.  gekka.NewCluster
// constructs one and pre-registers the default SBR provider on it.
func NewDowningProviderRegistry() *DowningProviderRegistry {
	return &DowningProviderRegistry{providers: make(map[string]DowningProvider)}
}

// Register stores p under p.Name().  Replaces any previous registration
// for that name; the last writer wins.  Tests rely on this to swap the
// real SBR provider with a fake without dancing around init order.
func (r *DowningProviderRegistry) Register(p DowningProvider) {
	if p == nil {
		return
	}
	r.mu.Lock()
	r.providers[p.Name()] = p
	r.mu.Unlock()
}

// Resolve returns the provider registered under name.  ok=false when
// the name is unknown — callers (typically gekka.NewCluster) translate
// that into a fallback to DefaultDowningProviderName with a warn log.
func (r *DowningProviderRegistry) Resolve(name string) (DowningProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[name]
	return p, ok
}

// Names returns the list of registered provider names sorted for stable
// log output.  Used by error messages so an operator who supplies a
// typo in `downing-provider-class` sees what's actually available.
func (r *DowningProviderRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.providers))
	for k := range r.providers {
		out = append(out, k)
	}
	return out
}

// NormalizeDowningProviderClass maps the HOCON
// `pekko.cluster.downing-provider-class` value to the registry short
// name.  Recognises both Pekko/Akka FQCNs and the gekka short name.
//
// The mapping rules:
//   - Empty / whitespace input → "" (caller falls back to default).
//   - Anything containing `SplitBrainResolverProvider` (Pekko or Akka
//     FQCN) → DefaultDowningProviderName.
//   - Else: trimmed input is returned as-is so future providers can
//     register under their own short name without code changes here.
//
// The function is exported so the HOCON parser and the join-time
// configuration-compatibility check use the same normalisation rule —
// otherwise a node could advertise one form on the wire and reject
// peers advertising the other.
func NormalizeDowningProviderClass(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if strings.Contains(s, "SplitBrainResolverProvider") {
		return DefaultDowningProviderName
	}
	return s
}

// FormatUnknownProviderError builds the error string used when a
// `downing-provider-class` value resolves to a name that no provider
// has registered under.  Centralised so the wording stays consistent
// between log messages and test assertions.
func FormatUnknownProviderError(name string, available []string) string {
	return fmt.Sprintf("downing: unknown provider %q; registered=%v", name, available)
}
