/*
 * cluster_sharding_ddata_overrides_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
)

// Sub-plan 8b: end-to-end verification that the three sharding-side
// distributed-data overrides parsed from HOCON
// (`pekko.cluster.sharding.distributed-data.{majority-min-cap,
// max-delta-elements, prefer-oldest}`) are threaded onto the shared
// `cluster.repl` Replicator, where they are consumed by the gossip path
// (selectGossipTargets + gossipAll's delta budget).
func TestSharding_DDataOverrides_AppliedToSharedReplicator(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
      max-delta-elements = 1000
      prefer-oldest = off
    }
    sharding {
      distributed-data {
        majority-min-cap   = 7
        max-delta-elements = 42
        prefer-oldest      = on
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestSystem"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer node.Shutdown()

	repl := node.Replicator()
	if repl == nil {
		t.Fatal("Replicator() returned nil")
	}

	// MajorityMinCap is sharding-only — it has no global sibling — so the
	// sharding override flows through directly.
	if got, want := repl.MajorityMinCap, 7; got != want {
		t.Errorf("repl.MajorityMinCap = %d, want %d (sharding override)", got, want)
	}

	// MaxDeltaElements: sharding's 42 is stricter than global 1000, so it
	// wins under the "smaller-wins" merge rule documented in cluster.go.
	if got, want := repl.MaxDeltaElements, 42; got != want {
		t.Errorf("repl.MaxDeltaElements = %d, want %d (sharding override is stricter)", got, want)
	}

	// PreferOldest: global=off, sharding=on; OR-merge → on.
	if !repl.PreferOldest {
		t.Errorf("repl.PreferOldest = false, want true (sharding OR-merge)")
	}
}

// TestSharding_DDataOverrides_GlobalWinsWhenStricter verifies the
// "smaller-wins" merge rule for max-delta-elements: when the global value
// is smaller than the sharding override, the global value sticks.
func TestSharding_DDataOverrides_GlobalWinsWhenStricter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
      max-delta-elements = 17
    }
    sharding {
      distributed-data {
        max-delta-elements = 100
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestSystem"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer node.Shutdown()

	if got, want := node.Replicator().MaxDeltaElements, 17; got != want {
		t.Errorf("repl.MaxDeltaElements = %d, want %d (global is stricter, sharding override skipped)", got, want)
	}
}
