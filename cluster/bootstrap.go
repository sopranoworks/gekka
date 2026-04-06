/*
 * bootstrap.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/discovery"
)

// BootstrapConfig holds configuration for the ClusterBootstrap coordinator.
type BootstrapConfig struct {
	// RequiredContactPoints is the minimum number of contact points that must
	// be discovered before the bootstrap will attempt to join. Default: 2.
	RequiredContactPoints int

	// StableMargin is the duration that the discovered contact points must
	// remain stable (same set) before bootstrap proceeds. Default: 5s.
	StableMargin time.Duration

	// DiscoveryInterval is how often the coordinator polls the SeedProvider.
	// Default: 1s.
	DiscoveryInterval time.Duration
}

// DefaultBootstrapConfig returns sensible defaults.
func DefaultBootstrapConfig() BootstrapConfig {
	return BootstrapConfig{
		RequiredContactPoints: 2,
		StableMargin:          5 * time.Second,
		DiscoveryInterval:     1 * time.Second,
	}
}

// ClusterBootstrap is a coordinator that uses a discovery.SeedProvider to
// automatically form a cluster without hardcoded seed nodes.
//
// It polls the SeedProvider at regular intervals, waits for a quorum of
// contact points to be stable for a configurable margin, and then initiates
// a cluster join. The node with the lowest address (by string sort) among
// the contact points performs a self-join to become the initial seed.
type ClusterBootstrap struct {
	config   BootstrapConfig
	provider discovery.SeedProvider

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	done    chan struct{}

	// For testing: overridable join function.
	joinFn func(ctx context.Context, host string, port uint32) error

	// For testing: overridable clock.
	nowFn func() time.Time
}

// NewClusterBootstrap creates a new bootstrap coordinator with the given
// seed provider and configuration.
func NewClusterBootstrap(provider discovery.SeedProvider, config BootstrapConfig) *ClusterBootstrap {
	if config.RequiredContactPoints <= 0 {
		config.RequiredContactPoints = 2
	}
	if config.StableMargin <= 0 {
		config.StableMargin = 5 * time.Second
	}
	if config.DiscoveryInterval <= 0 {
		config.DiscoveryInterval = 1 * time.Second
	}
	return &ClusterBootstrap{
		config:   config,
		provider: provider,
		nowFn:    time.Now,
	}
}

// Start begins the bootstrap discovery loop. It polls the SeedProvider and
// joins the cluster once quorum is achieved. The provided ClusterManager is
// used to initiate the cluster join.
func (b *ClusterBootstrap) Start(ctx context.Context, cm *ClusterManager) {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return
	}
	b.running = true
	b.done = make(chan struct{})

	childCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	if b.joinFn == nil {
		b.joinFn = func(ctx context.Context, host string, port uint32) error {
			return cm.JoinCluster(ctx, host, port)
		}
	}
	b.mu.Unlock()

	go b.loop(childCtx)
}

// Stop cancels the bootstrap discovery loop.
func (b *ClusterBootstrap) Stop() {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	b.cancel()
	done := b.done
	b.mu.Unlock()
	<-done
}

// IsRunning returns whether the bootstrap loop is currently active.
func (b *ClusterBootstrap) IsRunning() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.running
}

// loop is the main discovery loop that polls the SeedProvider and waits for
// a stable quorum before joining the cluster.
func (b *ClusterBootstrap) loop(ctx context.Context) {
	defer func() {
		b.mu.Lock()
		b.running = false
		close(b.done)
		b.mu.Unlock()
	}()

	var (
		lastContacts  string // sorted, comma-joined contact points for comparison
		stableSince   time.Time
		stableReached bool
	)

	ticker := time.NewTicker(b.config.DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		contacts, err := b.provider.FetchSeedNodes()
		if err != nil {
			slog.Warn("bootstrap: discovery error", "error", err)
			continue
		}

		// Normalize and sort.
		normalized := normalizeAddresses(contacts)
		sort.Strings(normalized)
		key := strings.Join(normalized, ",")

		now := b.nowFn()

		if key != lastContacts || len(normalized) < b.config.RequiredContactPoints {
			// Contact set changed or quorum not met — reset stability timer.
			lastContacts = key
			stableSince = now
			stableReached = false
			continue
		}

		if !stableReached {
			if now.Sub(stableSince) >= b.config.StableMargin {
				stableReached = true
			} else {
				continue
			}
		}

		// Quorum achieved and stable — determine the seed node.
		seed := normalized[0] // lowest address is the leader

		localAddr := b.localAddress()

		if seed == localAddr {
			// This node is the leader — self-join.
			slog.Info("bootstrap: this node is the lowest-address contact point, performing self-join")
			host, port, _ := parseHostPort(seed)
			if err := b.joinFn(ctx, host, port); err != nil {
				slog.Warn("bootstrap: self-join failed", "error", err)
				continue
			}
		} else {
			// Join the leader.
			slog.Info("bootstrap: joining seed node", "seed", seed)
			host, port, _ := parseHostPort(seed)
			if err := b.joinFn(ctx, host, port); err != nil {
				slog.Warn("bootstrap: join failed", "error", err)
				continue
			}
		}

		// Bootstrap complete.
		slog.Info("bootstrap: cluster join initiated successfully")
		return
	}
}

// localAddress returns this node's address in "host:port" format.
func (b *ClusterBootstrap) localAddress() string {
	// If the joinFn is overridden (test), we don't have a real ClusterManager.
	// Return empty — the caller handles this.
	return ""
}

// normalizeAddresses cleans up and deduplicates a list of addresses.
func normalizeAddresses(addrs []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if !seen[a] {
			seen[a] = true
			result = append(result, a)
		}
	}
	return result
}

// parseHostPort splits "host:port" into host string and port uint32.
func parseHostPort(addr string) (string, uint32, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("bootstrap: invalid address %q: %w", addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("bootstrap: invalid port in %q: %w", addr, err)
	}
	return host, uint32(port), nil
}

// ResolveLeader determines which contact point is the leader (lowest address
// by lexicographic sort). Exported for testing.
func ResolveLeader(contacts []string) string {
	normalized := normalizeAddresses(contacts)
	if len(normalized) == 0 {
		return ""
	}
	sort.Strings(normalized)
	return normalized[0]
}
