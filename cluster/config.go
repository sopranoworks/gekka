/*
 * config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"strings"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// InternalSBRConfig holds configuration for the lightweight internal SBR
// primitives (icluster.Strategy).  It mirrors the relevant fields of SBRConfig
// but lives under the gekka.cluster.split-brain-resolver HOCON namespace.
type InternalSBRConfig struct {
	// ActiveStrategy selects the icluster.Strategy implementation.
	// Options: "off" (default), "static-quorum", "keep-oldest".
	// Corresponds to HOCON: gekka.cluster.split-brain-resolver.active-strategy
	ActiveStrategy string

	// StableAfter is the minimum duration the unreachable set must remain
	// non-empty before the strategy is consulted.
	// Corresponds to HOCON: gekka.cluster.split-brain-resolver.stable-after
	// Default: 20s
	StableAfter time.Duration

	// QuorumSize is the minimum reachable member count required by static-quorum.
	// Corresponds to HOCON: gekka.cluster.split-brain-resolver.static-quorum.size
	// Default: 1
	QuorumSize int

	// Role restricts the oldest-member election for keep-oldest.
	// Corresponds to HOCON: gekka.cluster.split-brain-resolver.keep-oldest.role
	Role string

	// DownIfAlone, for keep-oldest, downs the oldest member when it is the only
	// reachable member on its side.
	// Corresponds to HOCON: gekka.cluster.split-brain-resolver.keep-oldest.down-if-alone
	// Default: true
	DownIfAlone bool
}

// NewInternalSBRStrategy creates an icluster.Strategy from the given config.
// Returns nil when ActiveStrategy is "off", empty, or unrecognised, preserving
// the legacy (no-op) behaviour.
func NewInternalSBRStrategy(cfg InternalSBRConfig) icluster.Strategy {
	switch strings.ToLower(strings.TrimSpace(cfg.ActiveStrategy)) {
	case "static-quorum":
		size := cfg.QuorumSize
		if size <= 0 {
			size = 1
		}
		return &icluster.StaticQuorumStrategy{QuorumSize: size}

	case "keep-oldest":
		return &icluster.KeepOldestStrategy{
			Role:        cfg.Role,
			DownIfAlone: cfg.DownIfAlone,
		}

	default:
		// "off", "" or any unknown value → no strategy.
		return nil
	}
}

// ApplyDetectorConfig reconfigures cm.Fd using the supplied FailureDetectorConfig.
// Any zero field falls back to the safe defaults listed in FailureDetectorConfig.
// This is a no-op when cfg is entirely zero.
func ApplyDetectorConfig(cm *ClusterManager, cfg FailureDetectorConfig) {
	if cfg == (FailureDetectorConfig{}) {
		return // nothing to change
	}
	threshold := cfg.Threshold
	if threshold <= 0 {
		threshold = 10.0
	}
	maxSamples := cfg.MaxSampleSize
	if maxSamples <= 0 {
		maxSamples = 1000
	}
	minStdDev := cfg.MinStdDeviation
	if minStdDev <= 0 {
		minStdDev = 500 * time.Millisecond
	}
	cm.Fd.Reconfigure(threshold, maxSamples, minStdDev)
}

// ApplyInternalSBRConfig wires the icluster.Strategy and stable-after duration
// into cm from the given InternalSBRConfig.
func ApplyInternalSBRConfig(cm *ClusterManager, cfg InternalSBRConfig) {
	cm.SBRStrategy = NewInternalSBRStrategy(cfg)
	if cfg.StableAfter > 0 {
		cm.SBRStableAfter = cfg.StableAfter
	}
}
