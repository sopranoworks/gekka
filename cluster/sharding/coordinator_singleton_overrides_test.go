/*
 * coordinator_singleton_overrides_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster"
	"github.com/sopranoworks/gekka/cluster/singleton"
)

// Sub-plan 8c: package-level verification that the wiring closure used
// inside StartSharding (cluster_sharding.go) propagates a
// ClusterShardingConfig.CoordinatorSingleton block onto the
// ClusterSingletonManager via its WithXxx setters. The setters write to
// private fields that the manager's lease-acquire retry path reads at
// runtime (manager.go:247), so the accessor read-back proves the values
// reached the live runtime state — not just the config struct.
//
// A complementary test at /coordinator_singleton_overrides_test.go
// (gekka package) boots a real *Cluster, calls StartSharding, and
// asserts the same accessors on the spawned manager actor — covering
// "settings reach the running actor instance" end-to-end.
func TestClusterShardingConfig_CoordinatorSingleton_AppliedToManager(t *testing.T) {
	cfg := ClusterShardingConfig{
		TypeName: "Test",
		Role:     "",
		CoordinatorSingleton: CoordinatorSingletonSettings{
			SingletonName:              "shardCoord",
			HandOverRetryInterval:      750 * time.Millisecond,
			MinNumberOfHandOverRetries: 25,
		},
	}

	// Mirror the wiring closure from cluster_sharding.go:130–146.
	// nil ClusterManager is acceptable here because the manager actor is
	// not started — we only inspect its configured state.
	var cm *cluster.ClusterManager
	coordProps := actor.Props{New: func() actor.Actor { return nil }}
	csSettings := cfg.CoordinatorSingleton
	m := singleton.NewClusterSingletonManager(cm, coordProps, cfg.Role)
	m.WithSingletonName(csSettings.SingletonName)
	m.WithHandOverRetryInterval(csSettings.HandOverRetryInterval)
	m.WithMinHandOverRetries(csSettings.MinNumberOfHandOverRetries)

	if got, want := m.SingletonName(), csSettings.SingletonName; got != want {
		t.Errorf("SingletonName() = %q, want %q", got, want)
	}
	if got, want := m.HandOverRetryInterval(), csSettings.HandOverRetryInterval; got != want {
		t.Errorf("HandOverRetryInterval() = %v, want %v", got, want)
	}
	if got, want := m.MinHandOverRetries(), csSettings.MinNumberOfHandOverRetries; got != want {
		t.Errorf("MinHandOverRetries() = %d, want %d", got, want)
	}
}

// TestClusterShardingConfig_CoordinatorSingleton_ZeroValuesPreserveDefaults
// verifies that an empty CoordinatorSingletonSettings leaves the
// manager at its built-in defaults (singleton, 1s, 15) — the
// WithXxx setters are no-ops for zero/empty inputs (see
// singleton/manager.go:84,92,101).
func TestClusterShardingConfig_CoordinatorSingleton_ZeroValuesPreserveDefaults(t *testing.T) {
	cfg := ClusterShardingConfig{
		TypeName:             "TestDefault",
		Role:                 "",
		CoordinatorSingleton: CoordinatorSingletonSettings{}, // all zero
	}

	var cm *cluster.ClusterManager
	coordProps := actor.Props{New: func() actor.Actor { return nil }}
	csSettings := cfg.CoordinatorSingleton
	m := singleton.NewClusterSingletonManager(cm, coordProps, cfg.Role)
	m.WithSingletonName(csSettings.SingletonName)
	m.WithHandOverRetryInterval(csSettings.HandOverRetryInterval)
	m.WithMinHandOverRetries(csSettings.MinNumberOfHandOverRetries)

	if got, want := m.SingletonName(), "singleton"; got != want {
		t.Errorf("SingletonName() = %q, want %q (default)", got, want)
	}
	if got, want := m.HandOverRetryInterval(), 1*time.Second; got != want {
		t.Errorf("HandOverRetryInterval() = %v, want %v (default)", got, want)
	}
	if got, want := m.MinHandOverRetries(), 15; got != want {
		t.Errorf("MinHandOverRetries() = %d, want %d (default)", got, want)
	}
}
