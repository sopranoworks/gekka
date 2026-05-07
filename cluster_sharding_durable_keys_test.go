/*
 * cluster_sharding_durable_keys_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Phase 7.3 — pekko.cluster.sharding.distributed-data.durable.keys
// end-to-end test: the sharding-side durable-key glob list must be merged
// into the shared Replicator's DurableKeys filter so writes to keys that
// only match the sharding patterns are persisted.

package gekka

import (
	"reflect"
	"sort"
	"testing"
)

// TestSharding_DurableKeys_MergedIntoReplicator verifies that an explicit
// `pekko.cluster.sharding.distributed-data.durable.keys` list is appended
// to the parent durable.keys list on the shared Replicator.  Without this
// wiring, writes to "shard-foo-*" keys bypass the durable store entirely.
func TestSharding_DurableKeys_MergedIntoReplicator(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
      durable {
        enabled = on
        keys    = ["counter-*"]
        lmdb.dir = "` + t.TempDir() + `"
      }
    }
    sharding {
      distributed-data {
        durable {
          keys = ["shard-foo-*", "shard-bar-*"]
        }
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

	got := append([]string(nil), repl.DurableKeys...)
	sort.Strings(got)
	want := []string{"counter-*", "shard-bar-*", "shard-foo-*"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("repl.DurableKeys = %v, want %v (parent ∪ sharding)", got, want)
	}
}

// TestSharding_DurableKeys_DefaultsToShardStarWhenUnset verifies that when
// the sharding-side durable.keys list is absent from HOCON, gekka falls
// back to Pekko's reference-conf default ["shard-*"] — so out-of-the-box
// any key prefixed with "shard-" is persisted whenever the durable backend
// is enabled.
func TestSharding_DurableKeys_DefaultsToShardStarWhenUnset(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
      durable {
        enabled = on
        keys    = ["counter-*"]
        lmdb.dir = "` + t.TempDir() + `"
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
	got := append([]string(nil), repl.DurableKeys...)
	sort.Strings(got)
	want := []string{"counter-*", "shard-*"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("repl.DurableKeys = %v, want %v (parent ∪ Pekko sharding default)", got, want)
	}
}

// TestSharding_DurableKeys_NoMergeWhenDurableDisabled verifies that the
// sharding durable-keys merge is a no-op when the parent durable backend
// is off.  Without parent enablement there is no durable store to write
// to, so the merge would only confuse downstream code.
func TestSharding_DurableKeys_NoMergeWhenDurableDisabled(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
    }
    sharding {
      distributed-data {
        durable {
          keys = ["shard-foo-*"]
        }
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
	if repl.DurableEnabled {
		t.Errorf("DurableEnabled = true, want false (durable.enabled is off)")
	}
	if len(repl.DurableKeys) != 0 {
		t.Errorf("DurableKeys = %v, want empty (durable disabled)", repl.DurableKeys)
	}
}

// TestSharding_DurableKeys_DedupesOverlap verifies the merge dedupes
// glob patterns that appear in both parent and sharding lists so the
// IsDurableKey loop does not pay for the same comparison twice.
func TestSharding_DurableKeys_DedupesOverlap(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    distributed-data {
      enabled = on
      durable {
        enabled = on
        keys    = ["shard-*", "counter-*"]
        lmdb.dir = "` + t.TempDir() + `"
      }
    }
    sharding {
      distributed-data {
        durable {
          keys = ["shard-*", "shard-extra-*"]
        }
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
	got := append([]string(nil), repl.DurableKeys...)
	sort.Strings(got)
	want := []string{"counter-*", "shard-*", "shard-extra-*"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("repl.DurableKeys = %v, want %v (dedupe shard-*)", got, want)
	}
}
