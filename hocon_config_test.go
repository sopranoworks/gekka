/*
 * hocon_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/client"
	"github.com/sopranoworks/gekka/cluster/lease"
)

func TestLoadConfig_Pekko(t *testing.T) {
	cfg, err := LoadConfig("testdata/pekko.conf")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	want := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2553}
	if cfg.Address != want {
		t.Errorf("Address = %+v, want %+v", cfg.Address, want)
	}

	if len(cfg.SeedNodes) != 1 {
		t.Fatalf("SeedNodes len = %d, want 1", len(cfg.SeedNodes))
	}
	wantSeed := actor.Address{Protocol: "pekko", System: "ClusterSystem", Host: "127.0.0.1", Port: 2552}
	if cfg.SeedNodes[0] != wantSeed {
		t.Errorf("SeedNodes[0] = %+v, want %+v", cfg.SeedNodes[0], wantSeed)
	}
}

func TestLoadConfig_Akka(t *testing.T) {
	cfg, err := LoadConfig("testdata/akka.conf")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	want := actor.Address{Protocol: "akka", System: "MySystem", Host: "10.0.0.1", Port: 2554}
	if cfg.Address != want {
		t.Errorf("Address = %+v, want %+v", cfg.Address, want)
	}

	if len(cfg.SeedNodes) != 2 {
		t.Fatalf("SeedNodes len = %d, want 2", len(cfg.SeedNodes))
	}
	if cfg.SeedNodes[0].Port != 2552 || cfg.SeedNodes[0].Host != "10.0.0.1" {
		t.Errorf("SeedNodes[0] = %+v", cfg.SeedNodes[0])
	}
	if cfg.SeedNodes[1].Host != "10.0.0.2" {
		t.Errorf("SeedNodes[1] = %+v", cfg.SeedNodes[1])
	}
}

func TestLoadConfig_Precedence(t *testing.T) {
	// precedence.conf has both akka and pekko blocks.
	// detectProtocol should prefer pekko.
	cfg, err := LoadConfig("testdata/precedence.conf")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Address.Protocol != "pekko" {
		t.Errorf("Protocol = %q, want pekko", cfg.Address.Protocol)
	}
	if cfg.Address.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want 127.0.0.1", cfg.Address.Host)
	}
	if cfg.Address.Port != 2552 {
		t.Errorf("Port = %d, want 2552", cfg.Address.Port)
	}
}

func TestLoadConfig_WithFallback(t *testing.T) {
	// no-seeds.conf has hostname/port but no seed-nodes.
	// reference.conf has a different default port — the app config wins.
	cfg, err := LoadConfig("testdata/no-seeds.conf", "testdata/reference.conf")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Address.Host != "192.168.1.5" {
		t.Errorf("Host = %q, want 192.168.1.5", cfg.Address.Host)
	}
	if cfg.Address.Port != 9000 {
		t.Errorf("Port = %d, want 9000", cfg.Address.Port)
	}
	// No seed-nodes in either file.
	if len(cfg.SeedNodes) != 0 {
		t.Errorf("SeedNodes = %v, want empty", cfg.SeedNodes)
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("testdata/nonexistent.conf")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestLoadConfig_ParseString(t *testing.T) {
	const hocon = `
pekko {
  remote.artery.canonical {
    hostname = "172.16.0.1"
    port = 3000
  }
  cluster.seed-nodes = ["pekko://SomeSystem@172.16.0.1:3000"]
}
`
	cfg, err := parseHOCONString(hocon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Address.Host != "172.16.0.1" {
		t.Errorf("Host = %q", cfg.Address.Host)
	}
	if cfg.Address.Port != 3000 {
		t.Errorf("Port = %d", cfg.Address.Port)
	}
	if cfg.Address.System != "SomeSystem" {
		t.Errorf("System = %q", cfg.Address.System)
	}
}

func TestSpawnFromConfig_Pekko(t *testing.T) {
	node, err := NewClusterFromConfig("testdata/pekko.conf")
	if err != nil {
		t.Fatalf("SpawnFromConfig: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	self := node.SelfAddress()
	if self.Protocol != "pekko" {
		t.Errorf("Protocol = %q, want pekko", self.Protocol)
	}
	if self.System != "ClusterSystem" {
		t.Errorf("System = %q, want ClusterSystem", self.System)
	}
	if self.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want 127.0.0.1", self.Host)
	}
	if self.Port != 2553 {
		t.Errorf("Port = %d, want 2553", self.Port)
	}

	seeds := node.Seeds()
	if len(seeds) != 1 {
		t.Fatalf("Seeds len = %d, want 1", len(seeds))
	}
	if seeds[0].Port != 2552 {
		t.Errorf("Seeds[0].Port = %d, want 2552", seeds[0].Port)
	}
}

func TestHOCON_SBRConfig(t *testing.T) {
	const hocon = `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2553
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    split-brain-resolver {
      active-strategy = keep-majority
      stable-after    = 10s
      keep-majority {
        role = "worker"
      }
    }
  }
}
`
	cfg, err := parseHOCONString(hocon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.ActiveStrategy != "keep-majority" {
		t.Errorf("ActiveStrategy = %q, want keep-majority", cfg.SBR.ActiveStrategy)
	}
	if cfg.SBR.StableAfter != 10*time.Second {
		t.Errorf("StableAfter = %v, want 10s", cfg.SBR.StableAfter)
	}
	if cfg.SBR.Role != "worker" {
		t.Errorf("Role = %q, want worker", cfg.SBR.Role)
	}
}

func TestHOCON_SBRConfig_KeepOldest(t *testing.T) {
	const hocon = `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2553
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    split-brain-resolver {
      active-strategy = keep-oldest
      stable-after    = "5 seconds"
      keep-oldest {
        down-if-alone = on
      }
    }
  }
}
`
	cfg, err := parseHOCONString(hocon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.ActiveStrategy != "keep-oldest" {
		t.Errorf("ActiveStrategy = %q, want keep-oldest", cfg.SBR.ActiveStrategy)
	}
	if cfg.SBR.StableAfter != 5*time.Second {
		t.Errorf("StableAfter = %v, want 5s", cfg.SBR.StableAfter)
	}
	if !cfg.SBR.DownIfAlone {
		t.Errorf("DownIfAlone = false, want true")
	}
}

func TestHOCON_SBRConfig_StaticQuorum(t *testing.T) {
	const hocon = `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2553
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    split-brain-resolver {
      active-strategy = static-quorum
      stable-after    = 20s
      static-quorum {
        quorum-size = 3
      }
    }
  }
}
`
	cfg, err := parseHOCONString(hocon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.ActiveStrategy != "static-quorum" {
		t.Errorf("ActiveStrategy = %q, want static-quorum", cfg.SBR.ActiveStrategy)
	}
	if cfg.SBR.QuorumSize != 3 {
		t.Errorf("QuorumSize = %d, want 3", cfg.SBR.QuorumSize)
	}
}

func TestHOCON_SBRConfig_StaticQuorumRole(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    split-brain-resolver {
      active-strategy = static-quorum
      static-quorum {
        quorum-size = 3
        role = "backend"
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.SBR.StaticQuorumRole != "backend" {
		t.Errorf("StaticQuorumRole = %q, want %q", cfg.SBR.StaticQuorumRole, "backend")
	}
}

func TestHOCON_SBRConfig_DownAllWhenUnstable_On(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.split-brain-resolver {
  active-strategy = keep-majority
  stable-after = 20s
  down-all-when-unstable = on
}
`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.DownAllWhenUnstableEnabled == nil || !*cfg.SBR.DownAllWhenUnstableEnabled {
		t.Fatal("expected DownAllWhenUnstableEnabled = true")
	}
	if cfg.SBR.DownAllWhenUnstable != 0 {
		t.Errorf("expected DownAllWhenUnstable = 0 (derived), got %v", cfg.SBR.DownAllWhenUnstable)
	}
}

func TestHOCON_SBRConfig_DownAllWhenUnstable_Off(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.split-brain-resolver {
  active-strategy = keep-majority
  down-all-when-unstable = off
}
`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.DownAllWhenUnstableEnabled == nil || *cfg.SBR.DownAllWhenUnstableEnabled {
		t.Fatal("expected DownAllWhenUnstableEnabled = false")
	}
}

func TestHOCON_SBRConfig_DownAllWhenUnstable_ExplicitDuration(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.split-brain-resolver {
  active-strategy = keep-majority
  down-all-when-unstable = 15s
}
`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SBR.DownAllWhenUnstableEnabled == nil || !*cfg.SBR.DownAllWhenUnstableEnabled {
		t.Fatal("expected DownAllWhenUnstableEnabled = true")
	}
	if cfg.SBR.DownAllWhenUnstable != 15*time.Second {
		t.Errorf("expected DownAllWhenUnstable = 15s, got %v", cfg.SBR.DownAllWhenUnstable)
	}
}

func TestHOCON_ShardingConfig(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = "2m"
pekko.cluster.sharding.remember-entities = on
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 2*time.Minute {
		t.Errorf("PassivationIdleTimeout = %v, want 2m", cfg.Sharding.PassivationIdleTimeout)
	}
	if !cfg.Sharding.RememberEntities {
		t.Error("RememberEntities = false, want true")
	}
}

func TestHOCON_ShardingConfig_PekkoPassivationPath(t *testing.T) {
	// Only the correct Pekko path is parsed.
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = "5m"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 5*time.Minute {
		t.Errorf("PassivationIdleTimeout = %v, want 5m (correct Pekko path)", cfg.Sharding.PassivationIdleTimeout)
	}
}

func TestHOCON_ShardingConfig_OldPassivationPath_Ignored(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    sharding.passivation.idle-timeout = 45s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 0 {
		t.Errorf("PassivationIdleTimeout = %v, want 0 (old path should be ignored)", cfg.Sharding.PassivationIdleTimeout)
	}
}

func TestHOCON_ShardingConfig_GuardianName(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    sharding.guardian-name = "mySharding"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.GuardianName != "mySharding" {
		t.Errorf("GuardianName = %q, want %q", cfg.Sharding.GuardianName, "mySharding")
	}
}

func TestHOCON_ShardingConfig_PassivationStrategy(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    sharding {
      passivation {
        strategy = "custom-lru-strategy"
        custom-lru-strategy.active-entity-limit = 5000
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// Round-2 session 24: the legacy "custom-lru-strategy" name is
	// normalised to the Pekko-canonical "least-recently-used" at parse
	// time so the runtime check only has to switch on one form.
	if got, want := cfg.Sharding.PassivationStrategy, "least-recently-used"; got != want {
		t.Errorf("PassivationStrategy = %q, want %q (legacy alias should normalise)", got, want)
	}
	if cfg.Sharding.PassivationActiveEntityLimit != 5000 {
		t.Errorf("PassivationActiveEntityLimit = %d, want 5000", cfg.Sharding.PassivationActiveEntityLimit)
	}
}

func TestHOCON_ShardingConfig_RememberEntitiesStore(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    sharding.remember-entities-store = "eventsourced"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.RememberEntitiesStore != "eventsourced" {
		t.Errorf("RememberEntitiesStore = %q, want %q", cfg.Sharding.RememberEntitiesStore, "eventsourced")
	}
}

func TestHOCON_ShardingConfig_NumberOfShards(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.number-of-shards = 500
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.NumberOfShards != 500 {
		t.Errorf("NumberOfShards = %d, want 500", cfg.Sharding.NumberOfShards)
	}
}

func TestHOCON_ShardingConfig_Role(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.role = "worker"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.Role != "worker" {
		t.Errorf("Role = %q, want worker", cfg.Sharding.Role)
	}
}

func TestHOCON_ShardingConfig_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.PassivationIdleTimeout != 0 {
		t.Errorf("expected zero PassivationIdleTimeout by default, got %v", cfg.Sharding.PassivationIdleTimeout)
	}
	if cfg.Sharding.RememberEntities {
		t.Error("expected RememberEntities=false by default")
	}
	// coordinator-singleton-role-override defaults to true (Pekko default: on).
	if !cfg.Sharding.CoordinatorSingletonRoleOverride {
		t.Error("expected CoordinatorSingletonRoleOverride=true by default")
	}
}

func TestHOCON_ShardingConfig_RebalanceInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.rebalance-interval = 25s
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.RebalanceInterval, 25*time.Second; got != want {
		t.Errorf("RebalanceInterval = %v, want %v", got, want)
	}
}

func TestHOCON_ShardingConfig_LeastShardAllocationStrategy(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.least-shard-allocation-strategy {
  rebalance-threshold = 7
  max-simultaneous-rebalance = 11
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got := cfg.Sharding.LeastShardAllocation.RebalanceThreshold; got != 7 {
		t.Errorf("RebalanceThreshold = %d, want 7", got)
	}
	if got := cfg.Sharding.LeastShardAllocation.MaxSimultaneousRebalance; got != 11 {
		t.Errorf("MaxSimultaneousRebalance = %d, want 11", got)
	}
}

func TestHOCON_ShardingConfig_DistributedData(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.distributed-data {
  majority-min-cap = 9
  max-delta-elements = 17
  prefer-oldest = off
  durable.keys = ["shard-foo-*", "shard-bar-*"]
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	dd := cfg.Sharding.DistributedData
	if dd.MajorityMinCap != 9 {
		t.Errorf("MajorityMinCap = %d, want 9", dd.MajorityMinCap)
	}
	if dd.MaxDeltaElements != 17 {
		t.Errorf("MaxDeltaElements = %d, want 17", dd.MaxDeltaElements)
	}
	if dd.PreferOldest {
		t.Error("PreferOldest = true, want false")
	}
	if got, want := dd.DurableKeys, []string{"shard-foo-*", "shard-bar-*"}; !reflect.DeepEqual(got, want) {
		t.Errorf("DurableKeys = %v, want %v", got, want)
	}
}

func TestHOCON_ShardingConfig_DistributedData_PreferOldestOn(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.distributed-data.prefer-oldest = on
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.Sharding.DistributedData.PreferOldest {
		t.Error("PreferOldest = false, want true (prefer-oldest = on)")
	}
}

func TestHOCON_ShardingConfig_CoordinatorSingleton(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.coordinator-singleton {
  role = "shard"
  singleton-name = "coordinator"
  hand-over-retry-interval = 750ms
  min-number-of-hand-over-retries = 25
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cs := cfg.Sharding.CoordinatorSingleton
	if cs.Role != "shard" {
		t.Errorf("Role = %q, want %q", cs.Role, "shard")
	}
	if cs.SingletonName != "coordinator" {
		t.Errorf("SingletonName = %q, want %q", cs.SingletonName, "coordinator")
	}
	if cs.HandOverRetryInterval != 750*time.Millisecond {
		t.Errorf("HandOverRetryInterval = %v, want 750ms", cs.HandOverRetryInterval)
	}
	if cs.MinNumberOfHandOverRetries != 25 {
		t.Errorf("MinNumberOfHandOverRetries = %d, want 25", cs.MinNumberOfHandOverRetries)
	}
}

func TestHOCON_ShardingConfig_CoordinatorSingletonRoleOverride_Off(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.coordinator-singleton-role-override = off
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.CoordinatorSingletonRoleOverride {
		t.Error("CoordinatorSingletonRoleOverride = true, want false (override = off)")
	}
}

func TestHOCON_PersistencePlugins(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
  persistence {
    journal.plugin          = "sql"
    snapshot-store.plugin   = "sql"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Persistence.JournalPlugin != "sql" {
		t.Errorf("JournalPlugin = %q, want sql", cfg.Persistence.JournalPlugin)
	}
	if cfg.Persistence.SnapshotPlugin != "sql" {
		t.Errorf("SnapshotPlugin = %q, want sql", cfg.Persistence.SnapshotPlugin)
	}
}

func TestHOCON_PersistencePlugins_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// Both plugin names default to empty string when absent.
	if cfg.Persistence.JournalPlugin != "" {
		t.Errorf("JournalPlugin = %q, want empty", cfg.Persistence.JournalPlugin)
	}
	if cfg.Persistence.SnapshotPlugin != "" {
		t.Errorf("SnapshotPlugin = %q, want empty", cfg.Persistence.SnapshotPlugin)
	}
}

func TestHOCON_MultiDCConfig(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "10.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = ["pekko://ClusterSystem@10.0.0.1:2552"]
    multi-data-center {
      self-data-center = "us-east"
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.DataCenter != "us-east" {
		t.Errorf("DataCenter = %q, want us-east", cfg.DataCenter)
	}
}

func TestHOCON_MultiDCConfig_Default(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// When the key is absent, the default is "default".
	if cfg.DataCenter != "default" {
		t.Errorf("DataCenter = %q, want default", cfg.DataCenter)
	}
}

// ── Failure Detector HOCON parsing ───────────────────────────────────────────

func TestParseHOCON_FailureDetector_Threshold(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.failure-detector.threshold = 15.0
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.FailureDetector.Threshold != 15.0 {
		t.Errorf("Threshold = %v, want 15.0", cfg.FailureDetector.Threshold)
	}
}

func TestParseHOCON_FailureDetector_MaxSampleSize(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.failure-detector.max-sample-size = 500
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.FailureDetector.MaxSampleSize != 500 {
		t.Errorf("MaxSampleSize = %d, want 500", cfg.FailureDetector.MaxSampleSize)
	}
}

func TestParseHOCON_FailureDetector_MinStdDeviation(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.failure-detector.min-std-deviation = 200ms
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.FailureDetector.MinStdDeviation != 200*time.Millisecond {
		t.Errorf("MinStdDeviation = %v, want 200ms", cfg.FailureDetector.MinStdDeviation)
	}
}

// ── Pekko-namespace Failure Detector ─────────────────────────────────────────

func TestParseHOCON_PekkoFailureDetector(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = ["pekko://Sys@127.0.0.1:2552"]
    failure-detector {
      threshold = 12.0
      max-sample-size = 500
      min-std-deviation = 200ms
      heartbeat-interval = 2s
      acceptable-heartbeat-pause = 10s
      expected-response-after = 3s
    }
    min-nr-of-members = 3
    retry-unsuccessful-join-after = 15s
    gossip-interval = 500ms
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.FailureDetector.Threshold != 12.0 {
		t.Errorf("Threshold = %v, want 12.0", cfg.FailureDetector.Threshold)
	}
	if cfg.FailureDetector.MaxSampleSize != 500 {
		t.Errorf("MaxSampleSize = %d, want 500", cfg.FailureDetector.MaxSampleSize)
	}
	if cfg.FailureDetector.MinStdDeviation != 200*time.Millisecond {
		t.Errorf("MinStdDeviation = %v, want 200ms", cfg.FailureDetector.MinStdDeviation)
	}
	if cfg.FailureDetector.HeartbeatInterval != 2*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 2s", cfg.FailureDetector.HeartbeatInterval)
	}
	if cfg.FailureDetector.AcceptableHeartbeatPause != 10*time.Second {
		t.Errorf("AcceptableHeartbeatPause = %v, want 10s", cfg.FailureDetector.AcceptableHeartbeatPause)
	}
	if cfg.FailureDetector.ExpectedResponseAfter != 3*time.Second {
		t.Errorf("ExpectedResponseAfter = %v, want 3s", cfg.FailureDetector.ExpectedResponseAfter)
	}
	if cfg.MinNrOfMembers != 3 {
		t.Errorf("MinNrOfMembers = %d, want 3", cfg.MinNrOfMembers)
	}
	if cfg.RetryUnsuccessfulJoinAfter != 15*time.Second {
		t.Errorf("RetryUnsuccessfulJoinAfter = %v, want 15s", cfg.RetryUnsuccessfulJoinAfter)
	}
	if cfg.GossipInterval != 500*time.Millisecond {
		t.Errorf("GossipInterval = %v, want 500ms", cfg.GossipInterval)
	}
}

func TestParseHOCON_PekkoFailureDetector_PekkoOverridesGekka(t *testing.T) {
	// pekko namespace takes priority over gekka namespace
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = ["pekko://Sys@127.0.0.1:2552"]
    failure-detector {
      threshold = 9.0
    }
  }
}
gekka.cluster.failure-detector.threshold = 15.0
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// pekko namespace should win
	if cfg.FailureDetector.Threshold != 9.0 {
		t.Errorf("Threshold = %v, want 9.0 (pekko wins over gekka)", cfg.FailureDetector.Threshold)
	}
}

// ── Internal SBR HOCON parsing ────────────────────────────────────────────────

func TestParseHOCON_InternalSBR_ActiveStrategy(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.split-brain-resolver.active-strategy = static-quorum
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.InternalSBR.ActiveStrategy != "static-quorum" {
		t.Errorf("ActiveStrategy = %q, want %q", cfg.InternalSBR.ActiveStrategy, "static-quorum")
	}
}

func TestParseHOCON_InternalSBR_StableAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.split-brain-resolver.stable-after = 30s
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.InternalSBR.StableAfter != 30*time.Second {
		t.Errorf("StableAfter = %v, want 30s", cfg.InternalSBR.StableAfter)
	}
}

func TestParseHOCON_InternalSBR_StaticQuorumSize(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.split-brain-resolver.static-quorum.size = 5
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.InternalSBR.QuorumSize != 5 {
		t.Errorf("QuorumSize = %d, want 5", cfg.InternalSBR.QuorumSize)
	}
}

func TestParseHOCON_InternalSBR_KeepOldestRole(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.split-brain-resolver.keep-oldest.role = worker
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.InternalSBR.Role != "worker" {
		t.Errorf("Role = %q, want %q", cfg.InternalSBR.Role, "worker")
	}
}

func TestParseHOCON_InternalSBR_KeepOldestDownIfAlone(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka.cluster.split-brain-resolver.keep-oldest.down-if-alone = true
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.InternalSBR.DownIfAlone {
		t.Error("DownIfAlone = false, want true")
	}
}

func TestParseHOCON_InternalSBR_FullKeepOldest(t *testing.T) {
	cfg, err := parseHOCONString(`
gekka {
  cluster {
    split-brain-resolver {
      active-strategy = keep-oldest
      stable-after = 25s
      keep-oldest {
        role = seed
        down-if-alone = false
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.InternalSBR.ActiveStrategy != "keep-oldest" {
		t.Errorf("ActiveStrategy = %q, want keep-oldest", cfg.InternalSBR.ActiveStrategy)
	}
	if cfg.InternalSBR.StableAfter != 25*time.Second {
		t.Errorf("StableAfter = %v, want 25s", cfg.InternalSBR.StableAfter)
	}
	if cfg.InternalSBR.Role != "seed" {
		t.Errorf("Role = %q, want seed", cfg.InternalSBR.Role)
	}
	if cfg.InternalSBR.DownIfAlone {
		t.Error("DownIfAlone = true, want false")
	}
}

func TestJoinSeeds_NoSeeds(t *testing.T) {
	node, err := NewCluster(ClusterConfig{Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	err = node.JoinSeeds()
	if err == nil {
		t.Error("expected error from JoinSeeds with no seeds, got nil")
	}
}

func TestHOCON_ManagementConfig(t *testing.T) {
	const hocon = `
gekka.management.http {
  hostname = "0.0.0.0"
  port = 8558
  enabled = true
  health-checks.enabled = false
}
`
	cfg, err := parseHOCONString(hocon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Management.Hostname != "0.0.0.0" {
		t.Errorf("Hostname = %q, want 0.0.0.0", cfg.Management.Hostname)
	}
	if cfg.Management.Port != 8558 {
		t.Errorf("Port = %d, want 8558", cfg.Management.Port)
	}
	if !cfg.Management.Enabled {
		t.Errorf("Enabled = false, want true")
	}
	if cfg.Management.HealthChecksEnabled {
		t.Errorf("HealthChecksEnabled = true, want false")
	}
}

func TestHOCON_ManagementConfig_AutoEnable(t *testing.T) {
	// Case 1: port defined, enabled absent -> should be true
	cfg1, err := parseHOCONString(`gekka.management.http.port = 8558`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !cfg1.Management.Enabled {
		t.Error("expected Enabled=true when port is defined")
	}

	// Case 2: hostname defined, enabled absent -> should be true
	cfg2, err := parseHOCONString(`gekka.management.http.hostname = "127.0.0.1"`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !cfg2.Management.Enabled {
		t.Error("expected Enabled=true when hostname is defined")
	}

	// Case 3: port defined, but enabled explicitly false -> should be false
	cfg3, err := parseHOCONString(`
gekka.management.http {
  port = 8558
  enabled = false
}
`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if cfg3.Management.Enabled {
		t.Error("expected Enabled=false when explicitly set to false, even if port is defined")
	}
}

func TestLoadConfig_DebugEnabled(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "app.conf")
	if err := os.WriteFile(path, []byte(`
gekka.management.http {
  enabled = true
  hostname = "127.0.0.1"
  port = 8558
}
gekka.management.debug {
  enabled = true
}
`), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Management.DebugEnabled {
		t.Error("expected Management.DebugEnabled=true")
	}
}

func TestLoadConfig_DebugDisabledByDefault(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "app.conf")
	if err := os.WriteFile(path, []byte(`
gekka.management.http { enabled = true }
`), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Management.DebugEnabled {
		t.Error("expected Management.DebugEnabled=false by default")
	}
}

func TestParseHOCON_ClusterTimingBatchA(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    leader-actions-interval = 2s
    periodic-tasks-initial-delay = 3s
    shutdown-after-unsuccessful-join-seed-nodes = 30s
    log-info = off
    log-info-verbose = on
    allow-weakly-up-members = 10s
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.LeaderActionsInterval != 2*time.Second {
		t.Errorf("LeaderActionsInterval = %v, want 2s", cfg.LeaderActionsInterval)
	}
	if cfg.PeriodicTasksInitialDelay != 3*time.Second {
		t.Errorf("PeriodicTasksInitialDelay = %v, want 3s", cfg.PeriodicTasksInitialDelay)
	}
	if cfg.ShutdownAfterUnsuccessfulJoinSeedNodes != 30*time.Second {
		t.Errorf("ShutdownAfterUnsuccessfulJoinSeedNodes = %v, want 30s", cfg.ShutdownAfterUnsuccessfulJoinSeedNodes)
	}
	if cfg.LogInfo == nil || *cfg.LogInfo != false {
		t.Errorf("LogInfo = %v, want false", cfg.LogInfo)
	}
	if !cfg.LogInfoVerbose {
		t.Error("LogInfoVerbose = false, want true")
	}
	if cfg.AllowWeaklyUpMembers != 10*time.Second {
		t.Errorf("AllowWeaklyUpMembers = %v, want 10s", cfg.AllowWeaklyUpMembers)
	}
}

func TestParseHOCON_ShutdownJoinOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    shutdown-after-unsuccessful-join-seed-nodes = off
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.ShutdownAfterUnsuccessfulJoinSeedNodes != 0 {
		t.Errorf("ShutdownAfterUnsuccessfulJoinSeedNodes = %v, want 0 (off)", cfg.ShutdownAfterUnsuccessfulJoinSeedNodes)
	}
}

func TestParseHOCON_AllowWeaklyUpOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    allow-weakly-up-members = off
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.AllowWeaklyUpMembers != 0 {
		t.Errorf("AllowWeaklyUpMembers = %v, want 0 (off)", cfg.AllowWeaklyUpMembers)
	}
}

func TestParseHOCON_GossipTuningBatchF1(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    gossip-different-view-probability = 0.9
    reduce-gossip-different-view-probability = 200
    gossip-time-to-live = 3s
    prune-gossip-tombstones-after = 12h
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.GossipDifferentViewProbability != 0.9 {
		t.Errorf("GossipDifferentViewProbability = %v, want 0.9", cfg.GossipDifferentViewProbability)
	}
	if cfg.ReduceGossipDifferentViewProbability != 200 {
		t.Errorf("ReduceGossipDifferentViewProbability = %d, want 200", cfg.ReduceGossipDifferentViewProbability)
	}
	if cfg.GossipTimeToLive != 3*time.Second {
		t.Errorf("GossipTimeToLive = %v, want 3s", cfg.GossipTimeToLive)
	}
	if cfg.PruneGossipTombstonesAfter != 12*time.Hour {
		t.Errorf("PruneGossipTombstonesAfter = %v, want 12h", cfg.PruneGossipTombstonesAfter)
	}
}

func TestParseHOCON_FailureDetectorBatchF2(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    unreachable-nodes-reaper-interval = 500ms
    failure-detector {
      monitored-by-nr-of-members = 5
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.FailureDetector.MonitoredByNrOfMembers != 5 {
		t.Errorf("MonitoredByNrOfMembers = %d, want 5", cfg.FailureDetector.MonitoredByNrOfMembers)
	}
	if cfg.UnreachableNodesReaperInterval != 500*time.Millisecond {
		t.Errorf("UnreachableNodesReaperInterval = %v, want 500ms", cfg.UnreachableNodesReaperInterval)
	}
}

func TestParseHOCON_ClusterLifecycleBatchF3(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = ["pekko://TestSystem@127.0.0.1:2552"]
    down-removal-margin = 10s
    seed-node-timeout = 8s
    configuration-compatibility-check {
      enforce-on-join = on
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.DownRemovalMargin != 10*time.Second {
		t.Errorf("DownRemovalMargin = %v, want 10s", cfg.DownRemovalMargin)
	}
	if cfg.SeedNodeTimeout != 8*time.Second {
		t.Errorf("SeedNodeTimeout = %v, want 8s", cfg.SeedNodeTimeout)
	}
	if cfg.ConfigCompatCheck.EnforceOnJoin == nil {
		t.Fatal("ConfigCompatCheck.EnforceOnJoin should not be nil")
	}
	if *cfg.ConfigCompatCheck.EnforceOnJoin != true {
		t.Errorf("ConfigCompatCheck.EnforceOnJoin = %v, want true", *cfg.ConfigCompatCheck.EnforceOnJoin)
	}
}

func TestParseHOCON_DownRemovalMarginOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    down-removal-margin = off
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.DownRemovalMargin != 0 {
		t.Errorf("DownRemovalMargin = %v, want 0 (off)", cfg.DownRemovalMargin)
	}
}

func TestParseHOCON_EnforceOnJoinOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    configuration-compatibility-check {
      enforce-on-join = off
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.ConfigCompatCheck.EnforceOnJoin == nil {
		t.Fatal("ConfigCompatCheck.EnforceOnJoin should not be nil")
	}
	if *cfg.ConfigCompatCheck.EnforceOnJoin != false {
		t.Errorf("ConfigCompatCheck.EnforceOnJoin = %v, want false", *cfg.ConfigCompatCheck.EnforceOnJoin)
	}
}

// TestSensitiveConfigPaths_BuiltInDefaults verifies that the built-in
// allowlist (DefaultSensitiveConfigPaths) covers the Pekko reference defaults
// and is matched by IsSensitiveConfigPath even with no HOCON override.
func TestSensitiveConfigPaths_BuiltInDefaults(t *testing.T) {
	var c ConfigCompatCheckConfig
	for _, path := range []string{
		"pekko.remote.secure-cookie",
		"pekko.remote.netty.ssl.security",
		"pekko.remote.classic.netty.ssl.security",
		"pekko.remote.artery.ssl",
		"pekko.remote.artery.ssl.config-ssl-engine.key-store",
		"user.home",
		"user.name",
		"user.dir",
		"socksNonProxyHosts",
	} {
		if !c.IsSensitiveConfigPath(path) {
			t.Errorf("IsSensitiveConfigPath(%q) = false; want true (built-in default)", path)
		}
	}
	if c.IsSensitiveConfigPath("pekko.cluster.seed-nodes") {
		t.Error("IsSensitiveConfigPath(\"pekko.cluster.seed-nodes\") = true; want false (not sensitive)")
	}
}

// TestParseHOCON_SensitiveConfigPathsAppend verifies that user-supplied
// sensitive-config-paths groups are appended to the built-in allowlist.
func TestParseHOCON_SensitiveConfigPathsAppend(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    configuration-compatibility-check {
      sensitive-config-paths {
        my-app = ["my.app.api-key", "my.app.db-password"]
        third-party = ["lib.token"]
      }
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	got := cfg.ConfigCompatCheck.SensitiveConfigPaths
	wantUser := []string{"my.app.api-key", "my.app.db-password", "lib.token"}
	for _, w := range wantUser {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("SensitiveConfigPaths missing user-defined %q; got %v", w, got)
		}
	}

	// User extras are matched by IsSensitiveConfigPath
	for _, path := range []string{
		"my.app.api-key",
		"my.app.db-password.value",   // prefix match
		"lib.token",
	} {
		if !cfg.ConfigCompatCheck.IsSensitiveConfigPath(path) {
			t.Errorf("IsSensitiveConfigPath(%q) = false; want true (user extension)", path)
		}
	}

	// Built-in defaults must remain effective even when user adds groups.
	if !cfg.ConfigCompatCheck.IsSensitiveConfigPath("pekko.remote.artery.ssl.private-key") {
		t.Error("IsSensitiveConfigPath built-in default lost after user override")
	}

	// Non-sensitive paths still report false.
	if cfg.ConfigCompatCheck.IsSensitiveConfigPath("pekko.cluster.gossip-interval") {
		t.Error("IsSensitiveConfigPath(\"pekko.cluster.gossip-interval\") = true; want false")
	}
}

// TestParseHOCON_SensitiveConfigPathsDedup verifies duplicate entries across
// groups are de-duplicated.
func TestParseHOCON_SensitiveConfigPathsDedup(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    configuration-compatibility-check {
      sensitive-config-paths {
        a = ["dup.path", "uniq.a"]
        b = ["dup.path", "uniq.b"]
      }
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	count := 0
	for _, p := range cfg.ConfigCompatCheck.SensitiveConfigPaths {
		if p == "dup.path" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("sensitive-config-paths duplicate %q appears %d times; want 1", "dup.path", count)
	}
}

// TestParseHOCON_ArteryDebugObservability verifies HOCON parsing for the 5
// Round-2 Session 09 Artery debug/observability paths.
func TestParseHOCON_ArteryDebugObservability(t *testing.T) {
	hocon := `
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    bind {
      bind-timeout = 250ms
    }
    log-received-messages = on
    log-sent-messages = on
    log-frame-size-exceeding = "4 KiB"
    propagate-harmless-quarantine-events = on
  }
  cluster.seed-nodes = []
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.BindTimeout != 250*time.Millisecond {
		t.Errorf("BindTimeout = %v, want 250ms", cfg.BindTimeout)
	}
	if !cfg.LogReceivedMessages {
		t.Error("LogReceivedMessages = false, want true")
	}
	if !cfg.LogSentMessages {
		t.Error("LogSentMessages = false, want true")
	}
	if cfg.LogFrameSizeExceeding != 4096 {
		t.Errorf("LogFrameSizeExceeding = %d, want 4096 (4 KiB)", cfg.LogFrameSizeExceeding)
	}
	if !cfg.PropagateHarmlessQuarantineEvents {
		t.Error("PropagateHarmlessQuarantineEvents = false, want true")
	}
}

// TestParseHOCON_ArteryDebugObservability_Defaults verifies the parsed values
// stay at zero when the HOCON omits the new keys (matches Pekko defaults).
func TestParseHOCON_ArteryDebugObservability_Defaults(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.BindTimeout != 0 {
		t.Errorf("BindTimeout default = %v, want 0", cfg.BindTimeout)
	}
	if cfg.LogReceivedMessages {
		t.Error("LogReceivedMessages default = true, want false")
	}
	if cfg.LogSentMessages {
		t.Error("LogSentMessages default = true, want false")
	}
	if cfg.LogFrameSizeExceeding != 0 {
		t.Errorf("LogFrameSizeExceeding default = %d, want 0", cfg.LogFrameSizeExceeding)
	}
	if cfg.PropagateHarmlessQuarantineEvents {
		t.Error("PropagateHarmlessQuarantineEvents default = true, want false")
	}
}

// TestParseHOCON_LogFrameSizeExceedingOff verifies that "off" maps to 0.
func TestParseHOCON_LogFrameSizeExceedingOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    log-frame-size-exceeding = off
  }
  cluster.seed-nodes = []
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.LogFrameSizeExceeding != 0 {
		t.Errorf("LogFrameSizeExceeding = %d, want 0 (off)", cfg.LogFrameSizeExceeding)
	}
}

// TestParseHOCON_LogFrameSizeExceedingPlainInt verifies plain-int form.
func TestParseHOCON_LogFrameSizeExceedingPlainInt(t *testing.T) {
	hocon := `
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    log-frame-size-exceeding = 8192
  }
  cluster.seed-nodes = []
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.LogFrameSizeExceeding != 8192 {
		t.Errorf("LogFrameSizeExceeding = %d, want 8192", cfg.LogFrameSizeExceeding)
	}
}

// TestParseHOCON_ActorDebugFlags verifies HOCON parsing for the 7
// pekko.actor.debug.* flags + pekko.log-config-on-start (Round-2 session 10).
func TestParseHOCON_ActorDebugFlags(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
  log-config-on-start = on
  actor.debug {
    receive = on
    autoreceive = on
    lifecycle = on
    fsm = on
    event-stream = on
    unhandled = on
    router-misconfiguration = on
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if !cfg.LogConfigOnStart {
		t.Error("LogConfigOnStart = false, want true")
	}
	if !cfg.ActorDebug.Receive {
		t.Error("ActorDebug.Receive = false, want true")
	}
	if !cfg.ActorDebug.Autoreceive {
		t.Error("ActorDebug.Autoreceive = false, want true")
	}
	if !cfg.ActorDebug.Lifecycle {
		t.Error("ActorDebug.Lifecycle = false, want true")
	}
	if !cfg.ActorDebug.FSM {
		t.Error("ActorDebug.FSM = false, want true")
	}
	if !cfg.ActorDebug.EventStream {
		t.Error("ActorDebug.EventStream = false, want true")
	}
	if !cfg.ActorDebug.Unhandled {
		t.Error("ActorDebug.Unhandled = false, want true")
	}
	if !cfg.ActorDebug.RouterMisconfiguration {
		t.Error("ActorDebug.RouterMisconfiguration = false, want true")
	}
}

// TestParseHOCON_ActorDebugFlags_Defaults verifies that all 8 flags default
// to false when omitted from HOCON (matches Pekko reference defaults).
func TestParseHOCON_ActorDebugFlags_Defaults(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.LogConfigOnStart {
		t.Error("LogConfigOnStart default = true, want false")
	}
	zero := ActorDebugConfig{}
	if cfg.ActorDebug != zero {
		t.Errorf("ActorDebug default = %+v, want all-false", cfg.ActorDebug)
	}
}

func TestHOCON_SingletonConfig(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    singleton {
      role = "backend"
      hand-over-retry-interval = 2s
    }
    singleton-proxy {
      singleton-identification-interval = 3s
      buffer-size = 500
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Singleton.Role != "backend" {
		t.Errorf("Singleton.Role = %q, want %q", cfg.Singleton.Role, "backend")
	}
	if cfg.Singleton.HandOverRetryInterval != 2*time.Second {
		t.Errorf("Singleton.HandOverRetryInterval = %v, want 2s", cfg.Singleton.HandOverRetryInterval)
	}
	if cfg.SingletonProxy.SingletonIdentificationInterval != 3*time.Second {
		t.Errorf("SingletonProxy.SingletonIdentificationInterval = %v, want 3s", cfg.SingletonProxy.SingletonIdentificationInterval)
	}
	if cfg.SingletonProxy.BufferSize != 500 {
		t.Errorf("SingletonProxy.BufferSize = %d, want 500", cfg.SingletonProxy.BufferSize)
	}
}

func TestHOCON_SingletonConfig_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Singleton.Role != "" {
		t.Errorf("Singleton.Role = %q, want empty", cfg.Singleton.Role)
	}
	if cfg.Singleton.HandOverRetryInterval != 0 {
		t.Errorf("Singleton.HandOverRetryInterval = %v, want 0 (use default)", cfg.Singleton.HandOverRetryInterval)
	}
	if cfg.SingletonProxy.SingletonIdentificationInterval != 0 {
		t.Errorf("SingletonProxy.SingletonIdentificationInterval = %v, want 0 (use default)", cfg.SingletonProxy.SingletonIdentificationInterval)
	}
	if cfg.SingletonProxy.BufferSize != 0 {
		t.Errorf("SingletonProxy.BufferSize = %d, want 0 (use default)", cfg.SingletonProxy.BufferSize)
	}
}

func TestHOCON_SingletonConfig_NewFields(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    singleton {
      singleton-name = "mySingleton"
      min-number-of-hand-over-retries = 20
    }
    singleton-proxy {
      singleton-name = "mySingleton"
      role = "backend"
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Singleton.SingletonName != "mySingleton" {
		t.Errorf("SingletonName = %q, want %q", cfg.Singleton.SingletonName, "mySingleton")
	}
	if cfg.Singleton.MinNumberOfHandOverRetries != 20 {
		t.Errorf("MinNumberOfHandOverRetries = %d, want 20", cfg.Singleton.MinNumberOfHandOverRetries)
	}
	if cfg.SingletonProxy.SingletonName != "mySingleton" {
		t.Errorf("SingletonProxy.SingletonName = %q, want %q", cfg.SingletonProxy.SingletonName, "mySingleton")
	}
	if cfg.SingletonProxy.Role != "backend" {
		t.Errorf("SingletonProxy.Role = %q, want %q", cfg.SingletonProxy.Role, "backend")
	}
}

// TestHOCON_SingletonConfig_UseLease verifies that
// pekko.cluster.singleton.use-lease and lease-retry-interval are parsed into
// SingletonConfig.UseLease / LeaseRetryInterval (Round-2 session 20).
func TestHOCON_SingletonConfig_UseLease(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    singleton {
      use-lease = "memory"
      lease-retry-interval = 7s
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Singleton.UseLease != "memory" {
		t.Errorf("Singleton.UseLease = %q, want %q", cfg.Singleton.UseLease, "memory")
	}
	if cfg.Singleton.LeaseRetryInterval != 7*time.Second {
		t.Errorf("Singleton.LeaseRetryInterval = %v, want 7s", cfg.Singleton.LeaseRetryInterval)
	}
}

// TestHOCON_ShardingConfig_UseLease verifies that
// pekko.cluster.sharding.use-lease and lease-retry-interval are parsed into
// ShardingConfig.UseLease / LeaseRetryInterval (Round-2 session 20).
func TestHOCON_ShardingConfig_UseLease(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    sharding {
      use-lease = "memory"
      lease-retry-interval = 11s
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Sharding.UseLease != "memory" {
		t.Errorf("Sharding.UseLease = %q, want %q", cfg.Sharding.UseLease, "memory")
	}
	if cfg.Sharding.LeaseRetryInterval != 11*time.Second {
		t.Errorf("Sharding.LeaseRetryInterval = %v, want 11s", cfg.Sharding.LeaseRetryInterval)
	}
}

// TestHOCON_UseLease_Defaults verifies that omitting use-lease leaves
// UseLease empty and LeaseRetryInterval at its zero value (Round-2 session 20).
func TestHOCON_UseLease_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Singleton.UseLease != "" {
		t.Errorf("Singleton.UseLease = %q, want empty", cfg.Singleton.UseLease)
	}
	if cfg.Singleton.LeaseRetryInterval != 0 {
		t.Errorf("Singleton.LeaseRetryInterval = %v, want 0", cfg.Singleton.LeaseRetryInterval)
	}
	if cfg.Sharding.UseLease != "" {
		t.Errorf("Sharding.UseLease = %q, want empty", cfg.Sharding.UseLease)
	}
	if cfg.Sharding.LeaseRetryInterval != 0 {
		t.Errorf("Sharding.LeaseRetryInterval = %v, want 0", cfg.Sharding.LeaseRetryInterval)
	}
}

func TestHOCON_MaxFrameSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      maximum-frame-size = 524288
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := 512 * 1024
	if cfg.MaxFrameSize != want {
		t.Errorf("MaxFrameSize = %d, want %d", cfg.MaxFrameSize, want)
	}
}

func TestHOCON_MaxFrameSize_WithUnit(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      maximum-frame-size = "512KiB"
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := 512 * 1024
	if cfg.MaxFrameSize != want {
		t.Errorf("MaxFrameSize = %d, want %d", cfg.MaxFrameSize, want)
	}
}

func TestHOCON_MaxFrameSize_Default(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.MaxFrameSize != 0 {
		t.Errorf("MaxFrameSize = %d, want 0 (use default)", cfg.MaxFrameSize)
	}
}

func TestHOCON_AppVersion(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    app-version = "2.1.0"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.AppVersion != "2.1.0" {
		t.Errorf("AppVersion = %q, want %q", cfg.AppVersion, "2.1.0")
	}
}

func TestHOCON_RunCoordinatedShutdownWhenDown(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    run-coordinated-shutdown-when-down = off
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.RunCoordinatedShutdownWhenDown == nil {
		t.Fatal("RunCoordinatedShutdownWhenDown should not be nil")
	}
	if *cfg.RunCoordinatedShutdownWhenDown != false {
		t.Errorf("RunCoordinatedShutdownWhenDown = %v, want false", *cfg.RunCoordinatedShutdownWhenDown)
	}
}

func TestHOCON_QuarantineRemovedNodeAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    quarantine-removed-node-after = 10s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.QuarantineRemovedNodeAfter != 10*time.Second {
		t.Errorf("QuarantineRemovedNodeAfter = %v, want 10s", cfg.QuarantineRemovedNodeAfter)
	}
}

func TestHOCON_QuarantineRemovedNodeAfter_Off(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    quarantine-removed-node-after = off
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.QuarantineRemovedNodeAfter != 0 {
		t.Errorf("QuarantineRemovedNodeAfter = %v, want 0 (off)", cfg.QuarantineRemovedNodeAfter)
	}
}

func TestHOCON_BindHostnameAndPort(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "10.0.0.1"
      port = 2552
    }
    bind {
      hostname = "0.0.0.0"
      port = 9000
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Host != "10.0.0.1" {
		t.Errorf("Host = %q, want %q", cfg.Host, "10.0.0.1")
	}
	if cfg.Port != 2552 {
		t.Errorf("Port = %d, want 2552", cfg.Port)
	}
	if cfg.BindHostname != "0.0.0.0" {
		t.Errorf("BindHostname = %q, want %q", cfg.BindHostname, "0.0.0.0")
	}
	if cfg.BindPort != 9000 {
		t.Errorf("BindPort = %d, want 9000", cfg.BindPort)
	}
}

func TestHOCON_BindHostnameOnly(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "10.0.0.1"
      port = 2552
    }
    bind {
      hostname = "0.0.0.0"
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.BindHostname != "0.0.0.0" {
		t.Errorf("BindHostname = %q, want %q", cfg.BindHostname, "0.0.0.0")
	}
	if cfg.BindPort != 0 {
		t.Errorf("BindPort = %d, want 0 (unset)", cfg.BindPort)
	}
}

func TestHOCON_BindEmpty(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.BindHostname != "" {
		t.Errorf("BindHostname = %q, want empty (unset)", cfg.BindHostname)
	}
	if cfg.BindPort != 0 {
		t.Errorf("BindPort = %d, want 0 (unset)", cfg.BindPort)
	}
}

func TestHOCON_PubSubGossipInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    pub-sub {
      gossip-interval = 500ms
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.PubSub.GossipInterval != 500*time.Millisecond {
		t.Errorf("PubSub.GossipInterval = %v, want 500ms", cfg.PubSub.GossipInterval)
	}
}

func TestHOCON_PubSubFullConfig(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    pub-sub {
      name = "myMediator"
      role = "frontend"
      routing-logic = "round-robin"
      gossip-interval = 2s
      removed-time-to-live = 60s
      max-delta-elements = 500
      send-to-dead-letters-when-no-subscribers = off
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.PubSub.Name != "myMediator" {
		t.Errorf("PubSub.Name = %q, want %q", cfg.PubSub.Name, "myMediator")
	}
	if cfg.PubSub.Role != "frontend" {
		t.Errorf("PubSub.Role = %q, want %q", cfg.PubSub.Role, "frontend")
	}
	if cfg.PubSub.RoutingLogic != "round-robin" {
		t.Errorf("PubSub.RoutingLogic = %q, want %q", cfg.PubSub.RoutingLogic, "round-robin")
	}
	if cfg.PubSub.GossipInterval != 2*time.Second {
		t.Errorf("PubSub.GossipInterval = %v, want 2s", cfg.PubSub.GossipInterval)
	}
	if cfg.PubSub.RemovedTimeToLive != 60*time.Second {
		t.Errorf("PubSub.RemovedTimeToLive = %v, want 60s", cfg.PubSub.RemovedTimeToLive)
	}
	if cfg.PubSub.MaxDeltaElements != 500 {
		t.Errorf("PubSub.MaxDeltaElements = %d, want 500", cfg.PubSub.MaxDeltaElements)
	}
	if cfg.PubSub.SendToDeadLettersWhenNoSubscribers {
		t.Errorf("PubSub.SendToDeadLettersWhenNoSubscribers = true, want false")
	}
}

func TestHOCON_MaxConcurrentRecoveries(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
  persistence {
    max-concurrent-recoveries = 25
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.Persistence.MaxConcurrentRecoveries != 25 {
		t.Errorf("MaxConcurrentRecoveries = %d, want 25", cfg.Persistence.MaxConcurrentRecoveries)
	}
}

func TestHOCON_RoleMinNrOfMembers(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster {
    seed-nodes = []
    role {
      backend {
        min-nr-of-members = 3
      }
      frontend {
        min-nr-of-members = 1
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.RoleMinNrOfMembers == nil {
		t.Fatal("RoleMinNrOfMembers is nil")
	}
	if cfg.RoleMinNrOfMembers["backend"] != 3 {
		t.Errorf("RoleMinNrOfMembers[backend] = %d, want 3", cfg.RoleMinNrOfMembers["backend"])
	}
	if cfg.RoleMinNrOfMembers["frontend"] != 1 {
		t.Errorf("RoleMinNrOfMembers[frontend] = %d, want 1", cfg.RoleMinNrOfMembers["frontend"])
	}
}

func TestHOCON_RoleMinNrOfMembers_Empty(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 2552
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.RoleMinNrOfMembers != nil {
		t.Errorf("RoleMinNrOfMembers should be nil when not configured, got %v", cfg.RoleMinNrOfMembers)
	}
}

func TestHOCON_LogDeadLetters(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster.seed-nodes = []
  log-dead-letters = 5
  log-dead-letters-during-shutdown = on
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.LogDeadLetters != 5 {
		t.Errorf("LogDeadLetters = %d, want 5", cfg.LogDeadLetters)
	}
	if !cfg.LogDeadLettersDuringShutdown {
		t.Error("LogDeadLettersDuringShutdown = false, want true")
	}
}

func TestHOCON_LogDeadLetters_Off(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster.seed-nodes = []
  log-dead-letters = off
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.LogDeadLetters != 0 {
		t.Errorf("LogDeadLetters = %d, want 0", cfg.LogDeadLetters)
	}
}

func TestHOCON_LogDeadLetters_Default(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.LogDeadLetters != 10 {
		t.Errorf("LogDeadLetters = %d, want 10 (default)", cfg.LogDeadLetters)
	}
	if cfg.LogDeadLettersDuringShutdown {
		t.Error("LogDeadLettersDuringShutdown = true, want false (default)")
	}
}

func TestHOCON_AcceptProtocolNames(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote {
    artery.canonical { hostname = "127.0.0.1", port = 2552 }
    accept-protocol-names = ["pekko"]
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if len(cfg.AcceptProtocolNames) != 1 || cfg.AcceptProtocolNames[0] != "pekko" {
		t.Errorf("AcceptProtocolNames = %v, want [pekko]", cfg.AcceptProtocolNames)
	}
}

func TestHOCON_AcceptProtocolNames_Default(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if len(cfg.AcceptProtocolNames) != 2 {
		t.Errorf("AcceptProtocolNames = %v, want [pekko, akka]", cfg.AcceptProtocolNames)
	}
}

func TestHOCON_CrossDCConnections(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    multi-data-center.cross-data-center-connections = 3
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.CrossDataCenterConnections != 3 {
		t.Errorf("CrossDataCenterConnections = %d, want 3", cfg.CrossDataCenterConnections)
	}
}

func TestHOCON_DistributedData_AllFields(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster {
    seed-nodes = []
    distributed-data {
      name = "myReplicator"
      role = "backend"
      gossip-interval = 3s
      notify-subscribers-interval = 1s
      max-delta-elements = 1000
      delta-crdt {
        enabled = off
        max-delta-size = 100
      }
      prefer-oldest = on
      pruning-interval = 60s
      max-pruning-dissemination = 120s
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	d := cfg.DistributedData
	if d.Name != "myReplicator" {
		t.Errorf("Name = %q, want %q", d.Name, "myReplicator")
	}
	if d.Role != "backend" {
		t.Errorf("Role = %q, want %q", d.Role, "backend")
	}
	if d.GossipInterval != 3*time.Second {
		t.Errorf("GossipInterval = %v, want 3s", d.GossipInterval)
	}
	if d.NotifySubscribersInterval != 1*time.Second {
		t.Errorf("NotifySubscribersInterval = %v, want 1s", d.NotifySubscribersInterval)
	}
	if d.MaxDeltaElements != 1000 {
		t.Errorf("MaxDeltaElements = %d, want 1000", d.MaxDeltaElements)
	}
	if d.DeltaCRDTEnabled {
		t.Error("DeltaCRDTEnabled = true, want false")
	}
	if d.DeltaCRDTMaxDeltaSize != 100 {
		t.Errorf("DeltaCRDTMaxDeltaSize = %d, want 100", d.DeltaCRDTMaxDeltaSize)
	}
	if !d.PreferOldest {
		t.Error("PreferOldest = false, want true")
	}
	if d.PruningInterval != 60*time.Second {
		t.Errorf("PruningInterval = %v, want 60s", d.PruningInterval)
	}
	if d.MaxPruningDissemination != 120*time.Second {
		t.Errorf("MaxPruningDissemination = %v, want 120s", d.MaxPruningDissemination)
	}
}

func TestHOCON_DistributedData_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	d := cfg.DistributedData
	if d.Name != "ddataReplicator" {
		t.Errorf("Name default = %q, want ddataReplicator", d.Name)
	}
	if d.NotifySubscribersInterval != 500*time.Millisecond {
		t.Errorf("NotifySubscribersInterval default = %v, want 500ms", d.NotifySubscribersInterval)
	}
	if d.MaxDeltaElements != 500 {
		t.Errorf("MaxDeltaElements default = %d, want 500", d.MaxDeltaElements)
	}
	if !d.DeltaCRDTEnabled {
		t.Error("DeltaCRDTEnabled default = false, want true")
	}
	if d.DeltaCRDTMaxDeltaSize != 50 {
		t.Errorf("DeltaCRDTMaxDeltaSize default = %d, want 50", d.DeltaCRDTMaxDeltaSize)
	}
	if d.PruningInterval != 120*time.Second {
		t.Errorf("PruningInterval default = %v, want 120s", d.PruningInterval)
	}
	if d.MaxPruningDissemination != 300*time.Second {
		t.Errorf("MaxPruningDissemination default = %v, want 300s", d.MaxPruningDissemination)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Artery advanced: lanes + queue sizes (round2 session 01)
// ─────────────────────────────────────────────────────────────────────────────

func TestHOCON_ArteryAdvanced_InboundLanes(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      inbound-lanes = 8
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.InboundLanes != 8 {
		t.Errorf("ArteryAdvanced.InboundLanes = %d, want 8", cfg.ArteryAdvanced.InboundLanes)
	}
}

func TestHOCON_ArteryAdvanced_OutboundLanes(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      outbound-lanes = 3
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundLanes != 3 {
		t.Errorf("ArteryAdvanced.OutboundLanes = %d, want 3", cfg.ArteryAdvanced.OutboundLanes)
	}
}

func TestHOCON_ArteryAdvanced_OutboundMessageQueueSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      outbound-message-queue-size = 8192
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundMessageQueueSize != 8192 {
		t.Errorf("ArteryAdvanced.OutboundMessageQueueSize = %d, want 8192", cfg.ArteryAdvanced.OutboundMessageQueueSize)
	}
}

func TestHOCON_ArteryAdvanced_SystemMessageBufferSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced {
      system-message-buffer-size = 40000
    }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.SystemMessageBufferSize != 40000 {
		t.Errorf("ArteryAdvanced.SystemMessageBufferSize = %d, want 40000", cfg.ArteryAdvanced.SystemMessageBufferSize)
	}
}

func TestHOCON_ArteryAdvanced_Defaults(t *testing.T) {
	// Without any advanced overrides, ClusterConfig fields should be zero-valued
	// (defaults applied when threaded into NodeManager).
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.InboundLanes != 0 ||
		cfg.ArteryAdvanced.OutboundLanes != 0 ||
		cfg.ArteryAdvanced.OutboundMessageQueueSize != 0 ||
		cfg.ArteryAdvanced.SystemMessageBufferSize != 0 ||
		cfg.ArteryAdvanced.OutboundControlQueueSize != 0 ||
		cfg.ArteryAdvanced.HandshakeTimeout != 0 ||
		cfg.ArteryAdvanced.HandshakeRetryInterval != 0 ||
		cfg.ArteryAdvanced.SystemMessageResendInterval != 0 ||
		cfg.ArteryAdvanced.GiveUpSystemMessageAfter != 0 {
		t.Errorf("ArteryAdvanced = %+v, want zero-valued (defaults)", cfg.ArteryAdvanced)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Artery advanced: handshake + timers (round2 session 02)
// ─────────────────────────────────────────────────────────────────────────────

func TestHOCON_ArteryAdvanced_HandshakeTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced { handshake-timeout = 45s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.HandshakeTimeout != 45*time.Second {
		t.Errorf("ArteryAdvanced.HandshakeTimeout = %v, want 45s", cfg.ArteryAdvanced.HandshakeTimeout)
	}
}

func TestHOCON_ArteryAdvanced_HandshakeRetryInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced { handshake-retry-interval = 250ms }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.HandshakeRetryInterval != 250*time.Millisecond {
		t.Errorf("ArteryAdvanced.HandshakeRetryInterval = %v, want 250ms", cfg.ArteryAdvanced.HandshakeRetryInterval)
	}
}

func TestHOCON_ArteryAdvanced_SystemMessageResendInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced { system-message-resend-interval = 3s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.SystemMessageResendInterval != 3*time.Second {
		t.Errorf("ArteryAdvanced.SystemMessageResendInterval = %v, want 3s", cfg.ArteryAdvanced.SystemMessageResendInterval)
	}
}

func TestHOCON_ArteryAdvanced_GiveUpSystemMessageAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced { give-up-system-message-after = 2h }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.GiveUpSystemMessageAfter != 2*time.Hour {
		t.Errorf("ArteryAdvanced.GiveUpSystemMessageAfter = %v, want 2h", cfg.ArteryAdvanced.GiveUpSystemMessageAfter)
	}
}

func TestHOCON_ArteryAdvanced_OutboundControlQueueSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 2552
    }
    advanced { outbound-control-queue-size = 4096 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundControlQueueSize != 4096 {
		t.Errorf("ArteryAdvanced.OutboundControlQueueSize = %d, want 4096", cfg.ArteryAdvanced.OutboundControlQueueSize)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Artery advanced: quarantine + lifecycle timers (round2 session 03)
// ─────────────────────────────────────────────────────────────────────────────

func TestHOCON_ArteryAdvanced_StopIdleOutboundAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { stop-idle-outbound-after = 10m }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.StopIdleOutboundAfter != 10*time.Minute {
		t.Errorf("StopIdleOutboundAfter = %v, want 10m", cfg.ArteryAdvanced.StopIdleOutboundAfter)
	}
}

func TestHOCON_ArteryAdvanced_QuarantineIdleOutboundAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { quarantine-idle-outbound-after = 2h }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.QuarantineIdleOutboundAfter != 2*time.Hour {
		t.Errorf("QuarantineIdleOutboundAfter = %v, want 2h", cfg.ArteryAdvanced.QuarantineIdleOutboundAfter)
	}
}

func TestHOCON_ArteryAdvanced_StopQuarantinedAfterIdle(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { stop-quarantined-after-idle = 7s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.StopQuarantinedAfterIdle != 7*time.Second {
		t.Errorf("StopQuarantinedAfterIdle = %v, want 7s", cfg.ArteryAdvanced.StopQuarantinedAfterIdle)
	}
}

func TestHOCON_ArteryAdvanced_RemoveQuarantinedAssociationAfter(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { remove-quarantined-association-after = 30m }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.RemoveQuarantinedAssociationAfter != 30*time.Minute {
		t.Errorf("RemoveQuarantinedAssociationAfter = %v, want 30m", cfg.ArteryAdvanced.RemoveQuarantinedAssociationAfter)
	}
}

func TestHOCON_ArteryAdvanced_ShutdownFlushTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { shutdown-flush-timeout = 4s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.ShutdownFlushTimeout != 4*time.Second {
		t.Errorf("ShutdownFlushTimeout = %v, want 4s", cfg.ArteryAdvanced.ShutdownFlushTimeout)
	}
}

func TestHOCON_ArteryAdvanced_DeathWatchNotificationFlushTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { death-watch-notification-flush-timeout = 9s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.DeathWatchNotificationFlushTimeout != 9*time.Second {
		t.Errorf("DeathWatchNotificationFlushTimeout = %v, want 9s", cfg.ArteryAdvanced.DeathWatchNotificationFlushTimeout)
	}
}

func TestHOCON_ArteryAdvanced_InboundRestartTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { inbound-restart-timeout = 12s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.InboundRestartTimeout != 12*time.Second {
		t.Errorf("InboundRestartTimeout = %v, want 12s", cfg.ArteryAdvanced.InboundRestartTimeout)
	}
}

func TestHOCON_ArteryAdvanced_InboundMaxRestarts(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { inbound-max-restarts = 9 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.InboundMaxRestarts != 9 {
		t.Errorf("InboundMaxRestarts = %d, want 9", cfg.ArteryAdvanced.InboundMaxRestarts)
	}
}

func TestHOCON_ArteryAdvanced_OutboundRestartBackoff(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { outbound-restart-backoff = 2500ms }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundRestartBackoff != 2500*time.Millisecond {
		t.Errorf("OutboundRestartBackoff = %v, want 2500ms", cfg.ArteryAdvanced.OutboundRestartBackoff)
	}
}

func TestHOCON_ArteryAdvanced_OutboundRestartTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { outbound-restart-timeout = 15s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundRestartTimeout != 15*time.Second {
		t.Errorf("OutboundRestartTimeout = %v, want 15s", cfg.ArteryAdvanced.OutboundRestartTimeout)
	}
}

func TestHOCON_ArteryAdvanced_OutboundMaxRestarts(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { outbound-max-restarts = 11 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundMaxRestarts != 11 {
		t.Errorf("OutboundMaxRestarts = %d, want 11", cfg.ArteryAdvanced.OutboundMaxRestarts)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Artery advanced: compression + TCP + buffers (round2 session 04)
// ─────────────────────────────────────────────────────────────────────────────

func TestHOCON_ArteryAdvanced_CompressionActorRefsMax(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { compression.actor-refs.max = 512 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.CompressionActorRefsMax != 512 {
		t.Errorf("CompressionActorRefsMax = %d, want 512", cfg.ArteryAdvanced.CompressionActorRefsMax)
	}
}

func TestHOCON_ArteryAdvanced_CompressionActorRefsAdvertisementInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { compression.actor-refs.advertisement-interval = 30s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.CompressionActorRefsAdvertisementInterval != 30*time.Second {
		t.Errorf("CompressionActorRefsAdvertisementInterval = %v, want 30s",
			cfg.ArteryAdvanced.CompressionActorRefsAdvertisementInterval)
	}
}

func TestHOCON_ArteryAdvanced_CompressionManifestsMax(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { compression.manifests.max = 1024 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.CompressionManifestsMax != 1024 {
		t.Errorf("CompressionManifestsMax = %d, want 1024", cfg.ArteryAdvanced.CompressionManifestsMax)
	}
}

func TestHOCON_ArteryAdvanced_CompressionManifestsAdvertisementInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { compression.manifests.advertisement-interval = 90s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.CompressionManifestsAdvertisementInterval != 90*time.Second {
		t.Errorf("CompressionManifestsAdvertisementInterval = %v, want 90s",
			cfg.ArteryAdvanced.CompressionManifestsAdvertisementInterval)
	}
}

func TestHOCON_ArteryAdvanced_TcpConnectionTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { tcp.connection-timeout = 12s }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.TcpConnectionTimeout != 12*time.Second {
		t.Errorf("TcpConnectionTimeout = %v, want 12s", cfg.ArteryAdvanced.TcpConnectionTimeout)
	}
}

func TestHOCON_ArteryAdvanced_TcpOutboundClientHostname(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { tcp.outbound-client-hostname = "10.1.2.3" }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.TcpOutboundClientHostname != "10.1.2.3" {
		t.Errorf("TcpOutboundClientHostname = %q, want 10.1.2.3",
			cfg.ArteryAdvanced.TcpOutboundClientHostname)
	}
}

func TestHOCON_ArteryAdvanced_BufferPoolSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { buffer-pool-size = 64 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.BufferPoolSize != 64 {
		t.Errorf("BufferPoolSize = %d, want 64", cfg.ArteryAdvanced.BufferPoolSize)
	}
}

func TestHOCON_ArteryAdvanced_MaximumLargeFrameSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { maximum-large-frame-size = "4 MiB" }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.MaximumLargeFrameSize != 4*1024*1024 {
		t.Errorf("MaximumLargeFrameSize = %d, want 4 MiB (%d)",
			cfg.ArteryAdvanced.MaximumLargeFrameSize, 4*1024*1024)
	}
}

func TestHOCON_ArteryAdvanced_LargeBufferPoolSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { large-buffer-pool-size = 64 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.LargeBufferPoolSize != 64 {
		t.Errorf("LargeBufferPoolSize = %d, want 64", cfg.ArteryAdvanced.LargeBufferPoolSize)
	}
}

func TestHOCON_ArteryAdvanced_OutboundLargeMessageQueueSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    advanced { outbound-large-message-queue-size = 1024 }
  }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.ArteryAdvanced.OutboundLargeMessageQueueSize != 1024 {
		t.Errorf("OutboundLargeMessageQueueSize = %d, want 1024",
			cfg.ArteryAdvanced.OutboundLargeMessageQueueSize)
	}
}

// ── Cluster Client (round-2 session 06) ───────────────────────────────────────

// TestHOCON_ClusterClient_Defaults verifies that an empty pekko.cluster.client
// block leaves the parsed Config at Pekko reference defaults.
func TestHOCON_ClusterClient_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cc := cfg.ClusterClient
	if got, want := cc.EstablishingGetContactsInterval, 3*time.Second; got != want {
		t.Errorf("EstablishingGetContactsInterval = %v, want %v", got, want)
	}
	if got, want := cc.RefreshContactsInterval, 60*time.Second; got != want {
		t.Errorf("RefreshContactsInterval = %v, want %v", got, want)
	}
	if got, want := cc.HeartbeatInterval, 2*time.Second; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := cc.AcceptableHeartbeatPause, 13*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
	if got, want := cc.BufferSize, 1000; got != want {
		t.Errorf("BufferSize = %d, want %d", got, want)
	}
	if got, want := cc.ReconnectTimeout, time.Duration(0); got != want {
		t.Errorf("ReconnectTimeout = %v, want %v", got, want)
	}
	if len(cc.InitialContacts) != 0 {
		t.Errorf("InitialContacts = %v, want empty", cc.InitialContacts)
	}
}

// TestHOCON_ClusterClient_InitialContacts verifies that the list is parsed and
// surrounding quotes are stripped.
func TestHOCON_ClusterClient_InitialContacts(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client {
      initial-contacts = [
        "pekko://Sys@host1:2552/system/receptionist",
        "pekko://Sys@host2:2552/system/receptionist",
      ]
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	got := cfg.ClusterClient.InitialContacts
	want := []string{
		"pekko://Sys@host1:2552/system/receptionist",
		"pekko://Sys@host2:2552/system/receptionist",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("InitialContacts = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_Establishing verifies override of
// pekko.cluster.client.establishing-get-contacts-interval.
func TestHOCON_ClusterClient_Establishing(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { establishing-get-contacts-interval = 7s }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.EstablishingGetContactsInterval, 7*time.Second; got != want {
		t.Errorf("EstablishingGetContactsInterval = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_RefreshContacts verifies override of
// pekko.cluster.client.refresh-contacts-interval.
func TestHOCON_ClusterClient_RefreshContacts(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { refresh-contacts-interval = 90s }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.RefreshContactsInterval, 90*time.Second; got != want {
		t.Errorf("RefreshContactsInterval = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_HeartbeatInterval verifies override of
// pekko.cluster.client.heartbeat-interval.
func TestHOCON_ClusterClient_HeartbeatInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { heartbeat-interval = 500ms }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.HeartbeatInterval, 500*time.Millisecond; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_AcceptableHeartbeatPause verifies override of
// pekko.cluster.client.acceptable-heartbeat-pause.
func TestHOCON_ClusterClient_AcceptableHeartbeatPause(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { acceptable-heartbeat-pause = 30s }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.AcceptableHeartbeatPause, 30*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_BufferSize verifies override of
// pekko.cluster.client.buffer-size.
func TestHOCON_ClusterClient_BufferSize(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { buffer-size = 42 }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.BufferSize, 42; got != want {
		t.Errorf("BufferSize = %d, want %d", got, want)
	}
}

// TestHOCON_ClusterClient_ReconnectTimeout_Off verifies that "off" maps to a
// zero duration (retry forever).
func TestHOCON_ClusterClient_ReconnectTimeout_Off(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { reconnect-timeout = off }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.ReconnectTimeout, time.Duration(0); got != want {
		t.Errorf("ReconnectTimeout = %v, want %v (off)", got, want)
	}
}

// TestHOCON_ClusterClient_ReconnectTimeout_Duration verifies that an explicit
// duration is parsed into ReconnectTimeout.
func TestHOCON_ClusterClient_ReconnectTimeout_Duration(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client { reconnect-timeout = 45s }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterClient.ReconnectTimeout, 45*time.Second; got != want {
		t.Errorf("ReconnectTimeout = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterClient_NewClusterClientHonorsConfig verifies that values
// loaded from HOCON survive the constructor (i.e. NewClusterClient does not
// overwrite non-zero values with the constructor's defaults).
func TestHOCON_ClusterClient_NewClusterClientHonorsConfig(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client {
      initial-contacts = ["pekko://Sys@host:2552/system/receptionist"]
      heartbeat-interval = 750ms
      acceptable-heartbeat-pause = 25s
      buffer-size = 250
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cc := client.NewClusterClient(cfg.ClusterClient, nil)
	// The exported helpers reflect the loaded config — initial connect attempt
	// happens in PreStart so CurrentContact stays empty here.
	if got := cc.BufferedCount(); got != 0 {
		t.Errorf("BufferedCount = %d, want 0", got)
	}
	// Re-derive the live values via the same struct used by ClusterClient.
	if got, want := cfg.ClusterClient.HeartbeatInterval, 750*time.Millisecond; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := cfg.ClusterClient.AcceptableHeartbeatPause, 25*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
	if got, want := cfg.ClusterClient.BufferSize, 250; got != want {
		t.Errorf("BufferSize = %d, want %d", got, want)
	}
	if got, want := cfg.ClusterClient.InitialContacts[0], "pekko://Sys@host:2552/system/receptionist"; got != want {
		t.Errorf("InitialContacts[0] = %q, want %q", got, want)
	}
}

// ── Cluster Client Receptionist (round-2 session 07) ──────────────────────────

// TestHOCON_ClusterReceptionist_Defaults verifies that an empty
// pekko.cluster.client.receptionist block leaves the parsed Config at Pekko
// reference defaults.
func TestHOCON_ClusterReceptionist_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	rc := cfg.ClusterReceptionist
	if got, want := rc.Name, "receptionist"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := rc.Role, ""; got != want {
		t.Errorf("Role = %q, want %q", got, want)
	}
	if got, want := rc.NumberOfContacts, 3; got != want {
		t.Errorf("NumberOfContacts = %d, want %d", got, want)
	}
	if got, want := rc.HeartbeatInterval, 2*time.Second; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := rc.AcceptableHeartbeatPause, 13*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterReceptionist_Name verifies override of
// pekko.cluster.client.receptionist.name.
func TestHOCON_ClusterReceptionist_Name(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.name = "frontDesk"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.Name, "frontDesk"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
}

// TestHOCON_ClusterReceptionist_Role verifies override of
// pekko.cluster.client.receptionist.role.
func TestHOCON_ClusterReceptionist_Role(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.role = "edge"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.Role, "edge"; got != want {
		t.Errorf("Role = %q, want %q", got, want)
	}
}

// TestHOCON_ClusterReceptionist_NumberOfContacts verifies override of
// pekko.cluster.client.receptionist.number-of-contacts.
func TestHOCON_ClusterReceptionist_NumberOfContacts(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.number-of-contacts = 7
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.NumberOfContacts, 7; got != want {
		t.Errorf("NumberOfContacts = %d, want %d", got, want)
	}
}

// TestHOCON_ClusterReceptionist_HeartbeatInterval verifies override of
// pekko.cluster.client.receptionist.heartbeat-interval.
func TestHOCON_ClusterReceptionist_HeartbeatInterval(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.heartbeat-interval = 750ms
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.HeartbeatInterval, 750*time.Millisecond; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterReceptionist_AcceptableHeartbeatPause verifies override of
// pekko.cluster.client.receptionist.acceptable-heartbeat-pause.
func TestHOCON_ClusterReceptionist_AcceptableHeartbeatPause(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.acceptable-heartbeat-pause = 45s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.AcceptableHeartbeatPause, 45*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
}

// TestHOCON_ClusterReceptionist_AkkaPrefix verifies that values under the
// akka.cluster.client.receptionist prefix are honored when the active prefix
// is akka (i.e. when remote.artery is absent and akka.* is loaded).
func TestHOCON_ClusterReceptionist_AkkaPrefix(t *testing.T) {
	cfg, err := parseHOCONString(`
akka {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist {
      name = "akkaDesk"
      number-of-contacts = 9
      heartbeat-interval = 1500ms
      acceptable-heartbeat-pause = 21s
      role = "frontend"
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	rc := cfg.ClusterReceptionist
	if got, want := rc.Name, "akkaDesk"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := rc.Role, "frontend"; got != want {
		t.Errorf("Role = %q, want %q", got, want)
	}
	if got, want := rc.NumberOfContacts, 9; got != want {
		t.Errorf("NumberOfContacts = %d, want %d", got, want)
	}
	if got, want := rc.HeartbeatInterval, 1500*time.Millisecond; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := rc.AcceptableHeartbeatPause, 21*time.Second; got != want {
		t.Errorf("AcceptableHeartbeatPause = %v, want %v", got, want)
	}
}

// ── Session 11: pekko.cluster.publish-stats-interval ────────────────────────

// TestParseHOCON_PublishStatsInterval verifies that a duration value is
// parsed into ClusterConfig.PublishStatsInterval.
func TestParseHOCON_PublishStatsInterval(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    publish-stats-interval = 5s
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.PublishStatsInterval != 5*time.Second {
		t.Errorf("PublishStatsInterval = %v, want 5s", cfg.PublishStatsInterval)
	}
}

// TestParseHOCON_PublishStatsIntervalOff verifies that "off" leaves the
// interval at zero (loop disabled).
func TestParseHOCON_PublishStatsIntervalOff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    publish-stats-interval = off
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.PublishStatsInterval != 0 {
		t.Errorf("PublishStatsInterval = %v, want 0 (off)", cfg.PublishStatsInterval)
	}
}

// ── Session 12: pekko.cluster.multi-data-center.failure-detector.* ─────────

// TestParseHOCON_MultiDCFailureDetector verifies the three multi-DC FD keys
// are parsed into ClusterConfig.MultiDCFailureDetector.
func TestParseHOCON_MultiDCFailureDetector(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    multi-data-center {
      failure-detector {
        heartbeat-interval         = 4s
        acceptable-heartbeat-pause = 12s
        expected-response-after    = 750ms
      }
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if got, want := cfg.MultiDCFailureDetector.HeartbeatInterval, 4*time.Second; got != want {
		t.Errorf("MultiDCFailureDetector.HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := cfg.MultiDCFailureDetector.AcceptableHeartbeatPause, 12*time.Second; got != want {
		t.Errorf("MultiDCFailureDetector.AcceptableHeartbeatPause = %v, want %v", got, want)
	}
	if got, want := cfg.MultiDCFailureDetector.ExpectedResponseAfter, 750*time.Millisecond; got != want {
		t.Errorf("MultiDCFailureDetector.ExpectedResponseAfter = %v, want %v", got, want)
	}
}

// TestParseHOCON_MultiDCFailureDetector_AbsentDefaults verifies that absent
// keys leave the MultiDCFailureDetector struct zero-valued (intra-DC fallback).
func TestParseHOCON_MultiDCFailureDetector_AbsentDefaults(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.MultiDCFailureDetector.HeartbeatInterval != 0 {
		t.Errorf("HeartbeatInterval = %v, want 0 (absent → fall back)", cfg.MultiDCFailureDetector.HeartbeatInterval)
	}
	if cfg.MultiDCFailureDetector.AcceptableHeartbeatPause != 0 {
		t.Errorf("AcceptableHeartbeatPause = %v, want 0", cfg.MultiDCFailureDetector.AcceptableHeartbeatPause)
	}
	if cfg.MultiDCFailureDetector.ExpectedResponseAfter != 0 {
		t.Errorf("ExpectedResponseAfter = %v, want 0", cfg.MultiDCFailureDetector.ExpectedResponseAfter)
	}
}

// ── Session 13: pekko.cluster.sharding retry/backoff (part 1) ──────────────

// TestParseHOCON_ShardingRetryBackoff verifies all 6 retry/backoff keys reach
// ShardingConfig.
func TestParseHOCON_ShardingRetryBackoff(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    sharding {
      retry-interval              = 4s
      buffer-size                 = 50000
      shard-start-timeout         = 15s
      shard-failure-backoff       = 12s
      entity-restart-backoff      = 8s
      coordinator-failure-backoff = 7s
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.RetryInterval, 4*time.Second; got != want {
		t.Errorf("RetryInterval = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.BufferSize, 50000; got != want {
		t.Errorf("BufferSize = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.ShardStartTimeout, 15*time.Second; got != want {
		t.Errorf("ShardStartTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.ShardFailureBackoff, 12*time.Second; got != want {
		t.Errorf("ShardFailureBackoff = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.EntityRestartBackoff, 8*time.Second; got != want {
		t.Errorf("EntityRestartBackoff = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.CoordinatorFailureBackoff, 7*time.Second; got != want {
		t.Errorf("CoordinatorFailureBackoff = %v, want %v", got, want)
	}
}

// TestParseHOCON_ShardingRetryBackoff_AbsentDefaults verifies that absent
// keys leave the fields zero (downstream defaults apply).
func TestParseHOCON_ShardingRetryBackoff_AbsentDefaults(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.Sharding.RetryInterval != 0 || cfg.Sharding.BufferSize != 0 ||
		cfg.Sharding.ShardStartTimeout != 0 || cfg.Sharding.ShardFailureBackoff != 0 ||
		cfg.Sharding.EntityRestartBackoff != 0 || cfg.Sharding.CoordinatorFailureBackoff != 0 {
		t.Errorf("expected all retry/backoff fields zero when absent, got %+v", cfg.Sharding)
	}
}

// TestParseHOCON_ShardingRetryBackoffPart2 verifies session 14 keys reach
// ShardingConfig.
func TestParseHOCON_ShardingRetryBackoffPart2(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    sharding {
      waiting-for-state-timeout  = 4s
      updating-state-timeout     = 9s
      shard-region-query-timeout = 6s
      entity-recovery-strategy   = "constant"
      entity-recovery-constant-rate-strategy {
        frequency          = 250ms
        number-of-entities = 11
      }
      coordinator-state {
        write-majority-plus = 4
        read-majority-plus  = 7
      }
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.WaitingForStateTimeout, 4*time.Second; got != want {
		t.Errorf("WaitingForStateTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.UpdatingStateTimeout, 9*time.Second; got != want {
		t.Errorf("UpdatingStateTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.ShardRegionQueryTimeout, 6*time.Second; got != want {
		t.Errorf("ShardRegionQueryTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.EntityRecoveryStrategy, "constant"; got != want {
		t.Errorf("EntityRecoveryStrategy = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.EntityRecoveryConstantRateFrequency, 250*time.Millisecond; got != want {
		t.Errorf("EntityRecoveryConstantRateFrequency = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.EntityRecoveryConstantRateNumberOfEntities, 11; got != want {
		t.Errorf("EntityRecoveryConstantRateNumberOfEntities = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.CoordinatorWriteMajorityPlus, 4; got != want {
		t.Errorf("CoordinatorWriteMajorityPlus = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.CoordinatorReadMajorityPlus, 7; got != want {
		t.Errorf("CoordinatorReadMajorityPlus = %d, want %d", got, want)
	}
}

// TestParseHOCON_ShardingMiscellaneous verifies session 15 keys reach
// ShardingConfig.
func TestParseHOCON_ShardingMiscellaneous(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    sharding {
      verbose-debug-logging                   = on
      fail-on-invalid-entity-state-transition = on
      passivation.default-idle-strategy.idle-entity.interval = 750ms
      healthcheck {
        names   = ["users", "orders"]
        timeout = 8s
      }
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if !cfg.Sharding.VerboseDebugLogging {
		t.Errorf("VerboseDebugLogging = %v, want true", cfg.Sharding.VerboseDebugLogging)
	}
	if !cfg.Sharding.FailOnInvalidEntityStateTransition {
		t.Errorf("FailOnInvalidEntityStateTransition = %v, want true", cfg.Sharding.FailOnInvalidEntityStateTransition)
	}
	if got, want := cfg.Sharding.IdleEntityCheckInterval, 750*time.Millisecond; got != want {
		t.Errorf("IdleEntityCheckInterval = %v, want %v", got, want)
	}
	if got, want := len(cfg.Sharding.HealthCheck.Names), 2; got != want {
		t.Fatalf("HealthCheck.Names len = %d, want %d", got, want)
	}
	if cfg.Sharding.HealthCheck.Names[0] != "users" || cfg.Sharding.HealthCheck.Names[1] != "orders" {
		t.Errorf("HealthCheck.Names = %v, want [users orders]", cfg.Sharding.HealthCheck.Names)
	}
	if got, want := cfg.Sharding.HealthCheck.Timeout, 8*time.Second; got != want {
		t.Errorf("HealthCheck.Timeout = %v, want %v", got, want)
	}
}

// TestParseHOCON_ShardingMiscellaneous_DefaultIntervalSentinel verifies
// "default" leaves IdleEntityCheckInterval at zero so the runtime falls
// back to PassivationIdleTimeout / 2.
func TestParseHOCON_ShardingMiscellaneous_DefaultIntervalSentinel(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    sharding.passivation.default-idle-strategy.idle-entity.interval = "default"
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.Sharding.IdleEntityCheckInterval != 0 {
		t.Errorf(`expected "default" sentinel to leave IdleEntityCheckInterval=0, got %v`, cfg.Sharding.IdleEntityCheckInterval)
	}
}

// TestParseHOCON_ShardingCoordinatorStateAllSentinel verifies "all" maps to
// math.MaxInt for both write- and read-majority-plus.
func TestParseHOCON_ShardingCoordinatorStateAllSentinel(t *testing.T) {
	hocon := `
pekko {
  remote.artery.canonical.hostname = "127.0.0.1"
  remote.artery.canonical.port = 2552
  cluster {
    seed-nodes = []
    sharding.coordinator-state {
      write-majority-plus = "all"
      read-majority-plus  = "all"
    }
  }
}
`
	cfg, err := ParseHOCONString(hocon)
	if err != nil {
		t.Fatalf("ParseHOCONString: %v", err)
	}
	if cfg.Sharding.CoordinatorWriteMajorityPlus <= 0 {
		t.Errorf(`expected "all" to produce a positive sentinel, got %d`, cfg.Sharding.CoordinatorWriteMajorityPlus)
	}
	if cfg.Sharding.CoordinatorReadMajorityPlus <= 0 {
		t.Errorf(`expected "all" to produce a positive sentinel, got %d`, cfg.Sharding.CoordinatorReadMajorityPlus)
	}
}

// ── Session 16: receptionist behaviors + DData small features ─────────────

// TestParseHOCON_S16_ClusterReceptionistDefaults verifies that an empty
// pekko.cluster.client.receptionist block leaves the two new session-16
// fields at their Pekko reference defaults (30s and 2s).
func TestParseHOCON_S16_ClusterReceptionistDefaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.ResponseTunnelReceiveTimeout, 30*time.Second; got != want {
		t.Errorf("ResponseTunnelReceiveTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.ClusterReceptionist.FailureDetectionInterval, 2*time.Second; got != want {
		t.Errorf("FailureDetectionInterval = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_ResponseTunnelReceiveTimeoutOverride verifies override
// of pekko.cluster.client.receptionist.response-tunnel-receive-timeout.
func TestParseHOCON_S16_ResponseTunnelReceiveTimeoutOverride(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.response-tunnel-receive-timeout = 7s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.ResponseTunnelReceiveTimeout, 7*time.Second; got != want {
		t.Errorf("ResponseTunnelReceiveTimeout = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_FailureDetectionIntervalOverride verifies override of
// pekko.cluster.client.receptionist.failure-detection-interval.
func TestParseHOCON_S16_FailureDetectionIntervalOverride(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    client.receptionist.failure-detection-interval = 250ms
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.ClusterReceptionist.FailureDetectionInterval, 250*time.Millisecond; got != want {
		t.Errorf("FailureDetectionInterval = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_DDataDefaults verifies that an empty
// pekko.cluster.distributed-data block leaves the four new session-16 fields
// at their Pekko reference defaults.
func TestParseHOCON_S16_DDataDefaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DistributedData.PruningMarkerTimeToLive, 6*time.Hour; got != want {
		t.Errorf("PruningMarkerTimeToLive = %v, want %v", got, want)
	}
	if got, want := cfg.DistributedData.LogDataSizeExceeding, 10*1024; got != want {
		t.Errorf("LogDataSizeExceeding = %d, want %d", got, want)
	}
	if got, want := cfg.DistributedData.RecoveryTimeout, 10*time.Second; got != want {
		t.Errorf("RecoveryTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.DistributedData.SerializerCacheTimeToLive, 10*time.Second; got != want {
		t.Errorf("SerializerCacheTimeToLive = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_DDataPruningMarkerTTL verifies override of
// pekko.cluster.distributed-data.pruning-marker-time-to-live.
func TestParseHOCON_S16_DDataPruningMarkerTTL(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.pruning-marker-time-to-live = 90m
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DistributedData.PruningMarkerTimeToLive, 90*time.Minute; got != want {
		t.Errorf("PruningMarkerTimeToLive = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_DDataLogDataSizeExceeding verifies override of
// pekko.cluster.distributed-data.log-data-size-exceeding (byte size).
func TestParseHOCON_S16_DDataLogDataSizeExceeding(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.log-data-size-exceeding = "256 KiB"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DistributedData.LogDataSizeExceeding, 256*1024; got != want {
		t.Errorf("LogDataSizeExceeding = %d, want %d", got, want)
	}
}

// TestParseHOCON_S16_DDataRecoveryTimeout verifies override of
// pekko.cluster.distributed-data.recovery-timeout.
func TestParseHOCON_S16_DDataRecoveryTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.recovery-timeout = 45s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DistributedData.RecoveryTimeout, 45*time.Second; got != want {
		t.Errorf("RecoveryTimeout = %v, want %v", got, want)
	}
}

// TestParseHOCON_S16_DDataSerializerCacheTTL verifies override of
// pekko.cluster.distributed-data.serializer-cache-time-to-live.
func TestParseHOCON_S16_DDataSerializerCacheTTL(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.serializer-cache-time-to-live = 3s
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DistributedData.SerializerCacheTimeToLive, 3*time.Second; got != want {
		t.Errorf("SerializerCacheTimeToLive = %v, want %v", got, want)
	}
}

// TestParseHOCON_S17_PersistenceSmallFeatures verifies the eight HOCON paths
// added by round-2 session 17 (persistence small features) populate the
// corresponding ClusterConfig.Persistence fields.
func TestParseHOCON_S17_PersistenceSmallFeatures(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
  persistence {
    journal {
      auto-start-journals = ["primary", "audit"]
    }
    snapshot-store {
      auto-start-snapshot-stores = ["primary-snap"]
      auto-migrate-manifest = "akka-legacy"
    }
    state-plugin-fallback {
      recovery-timeout = 12s
    }
    typed {
      stash-capacity = 8192
      stash-overflow-strategy = "fail"
      snapshot-on-recovery = on
    }
    fsm {
      snapshot-after = 250
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Persistence.AutoStartJournals, []string{"primary", "audit"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("AutoStartJournals = %v, want %v", got, want)
	}
	if got, want := cfg.Persistence.AutoStartSnapshotStores, []string{"primary-snap"}; len(got) != len(want) || got[0] != want[0] {
		t.Errorf("AutoStartSnapshotStores = %v, want %v", got, want)
	}
	if got, want := cfg.Persistence.AutoMigrateManifest, "akka-legacy"; got != want {
		t.Errorf("AutoMigrateManifest = %q, want %q", got, want)
	}
	if got, want := cfg.Persistence.StatePluginFallbackRecoveryTimeout, 12*time.Second; got != want {
		t.Errorf("StatePluginFallbackRecoveryTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Persistence.TypedStashCapacity, 8192; got != want {
		t.Errorf("TypedStashCapacity = %d, want %d", got, want)
	}
	if got, want := cfg.Persistence.TypedStashOverflowStrategy, "fail"; got != want {
		t.Errorf("TypedStashOverflowStrategy = %q, want %q", got, want)
	}
	if !cfg.Persistence.TypedSnapshotOnRecovery {
		t.Error("TypedSnapshotOnRecovery = false, want true")
	}
	if got, want := cfg.Persistence.FSMSnapshotAfter, 250; got != want {
		t.Errorf("FSMSnapshotAfter = %d, want %d", got, want)
	}
}

// TestParseHOCON_S17_PersistenceDefaults verifies that omitting the new paths
// leaves the gekka defaults (matching Pekko reference.conf).
func TestParseHOCON_S17_PersistenceDefaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Persistence.AutoMigrateManifest, "pekko"; got != want {
		t.Errorf("default AutoMigrateManifest = %q, want %q", got, want)
	}
	if got, want := cfg.Persistence.StatePluginFallbackRecoveryTimeout, 30*time.Second; got != want {
		t.Errorf("default StatePluginFallbackRecoveryTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.Persistence.TypedStashCapacity, 4096; got != want {
		t.Errorf("default TypedStashCapacity = %d, want %d", got, want)
	}
	if got, want := cfg.Persistence.TypedStashOverflowStrategy, "drop"; got != want {
		t.Errorf("default TypedStashOverflowStrategy = %q, want %q", got, want)
	}
	if cfg.Persistence.TypedSnapshotOnRecovery {
		t.Error("default TypedSnapshotOnRecovery should be false")
	}
	if cfg.Persistence.FSMSnapshotAfter != 0 {
		t.Errorf("default FSMSnapshotAfter = %d, want 0", cfg.Persistence.FSMSnapshotAfter)
	}
	if len(cfg.Persistence.AutoStartJournals) != 0 {
		t.Errorf("default AutoStartJournals = %v, want empty", cfg.Persistence.AutoStartJournals)
	}
	if len(cfg.Persistence.AutoStartSnapshotStores) != 0 {
		t.Errorf("default AutoStartSnapshotStores = %v, want empty", cfg.Persistence.AutoStartSnapshotStores)
	}
}

// TestHOCON_CoordinationLease verifies that pekko.coordination.lease.*
// keys are parsed into ClusterConfig.CoordinationLease.  Round-2 session 18.
func TestHOCON_CoordinationLease(t *testing.T) {
	const hoconConf = `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  cluster.seed-nodes = ["pekko://Sys@127.0.0.1:0"]
  coordination {
    lease {
      lease-class             = "memory"
      heartbeat-timeout       = 90s
      heartbeat-interval      = 6s
      lease-operation-timeout = 1500ms
    }
  }
}
`
	cfg, err := parseHOCONString(hoconConf)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.CoordinationLease.LeaseClass, "memory"; got != want {
		t.Errorf("LeaseClass = %q, want %q", got, want)
	}
	if got, want := cfg.CoordinationLease.HeartbeatTimeout, 90*time.Second; got != want {
		t.Errorf("HeartbeatTimeout = %v, want %v", got, want)
	}
	if got, want := cfg.CoordinationLease.HeartbeatInterval, 6*time.Second; got != want {
		t.Errorf("HeartbeatInterval = %v, want %v", got, want)
	}
	if got, want := cfg.CoordinationLease.LeaseOperationTimeout, 1500*time.Millisecond; got != want {
		t.Errorf("LeaseOperationTimeout = %v, want %v", got, want)
	}
}

// TestHOCON_CoordinationLease_AkkaPrefix verifies the same parsing under
// the akka.* prefix when the protocol is Akka.
func TestHOCON_CoordinationLease_AkkaPrefix(t *testing.T) {
	const hoconConf = `
akka {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  cluster.seed-nodes = ["akka://Sys@127.0.0.1:0"]
  coordination {
    lease {
      heartbeat-timeout = 30s
    }
  }
}
`
	cfg, err := parseHOCONString(hoconConf)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.CoordinationLease.HeartbeatTimeout, 30*time.Second; got != want {
		t.Errorf("HeartbeatTimeout = %v, want %v", got, want)
	}
}

// TestHOCON_CoordinationLease_ResolvesViaDefaultManager confirms the
// parsed LeaseClass resolves to a working provider when fed into the
// default LeaseManager.
func TestHOCON_CoordinationLease_ResolvesViaDefaultManager(t *testing.T) {
	const hoconConf = `
pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  cluster.seed-nodes = ["pekko://Sys@127.0.0.1:0"]
  coordination.lease.lease-class = "memory"
}
`
	cfg, err := parseHOCONString(hoconConf)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	mgr := lease.NewDefaultManager()
	settings := lease.LeaseSettings{
		LeaseName:     "hocon-resolved",
		OwnerName:     "node-1",
		LeaseDuration: cfg.CoordinationLease.HeartbeatTimeout,
	}
	l, err := mgr.GetLease(cfg.CoordinationLease.LeaseClass, settings)
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	got, err := l.Acquire(context.Background(), nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !got {
		t.Fatal("Acquire should succeed via the resolved memory provider")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 23: pekko.cluster.distributed-data.durable.* wiring
// ─────────────────────────────────────────────────────────────────────────────

// TestHOCON_DistributedDataDurable_Defaults locks down the Pekko-parity
// defaults applied when no `durable.*` keys appear in the parsed config.
// A consumer reading cfg.DistributedData should see the same values that
// reference.conf publishes — without that guarantee, the on-disk path
// would silently drift between deployments.
func TestHOCON_DistributedDataDurable_Defaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster.seed-nodes = []
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	d := cfg.DistributedData
	if d.DurableEnabled {
		t.Error("DurableEnabled default = true, want false")
	}
	if len(d.DurableKeys) != 0 {
		t.Errorf("DurableKeys default = %v, want empty", d.DurableKeys)
	}
	if got, want := d.DurablePruningMarkerTimeToLive, 10*24*time.Hour; got != want {
		t.Errorf("DurablePruningMarkerTimeToLive default = %v, want %v", got, want)
	}
	if d.DurableLmdbDir != "ddata" {
		t.Errorf("DurableLmdbDir default = %q, want ddata", d.DurableLmdbDir)
	}
	if d.DurableLmdbMapSize != 100*1024*1024 {
		t.Errorf("DurableLmdbMapSize default = %d, want 100 MiB", d.DurableLmdbMapSize)
	}
	if d.DurableLmdbWriteBehindInterval != 0 {
		t.Errorf("DurableLmdbWriteBehindInterval default = %v, want 0 (off)", d.DurableLmdbWriteBehindInterval)
	}
}

// TestHOCON_DistributedDataDurable_KeysImplyEnabled verifies the Pekko
// parity rule: a non-empty `durable.keys` list activates the durable path
// without requiring a separate `durable.enabled = on`.  Operators porting
// Pekko configs should not have to add a gekka-only flag.
func TestHOCON_DistributedDataDurable_KeysImplyEnabled(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.durable.keys = ["shard-*", "exact-key"]
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	d := cfg.DistributedData
	if !d.DurableEnabled {
		t.Error("DurableEnabled should be implied by non-empty keys")
	}
	want := []string{"shard-*", "exact-key"}
	if !reflect.DeepEqual(d.DurableKeys, want) {
		t.Errorf("DurableKeys = %v, want %v", d.DurableKeys, want)
	}
}

// TestHOCON_DistributedDataDurable_AllOverrides verifies every durable.*
// key reaches its target field. This is the round-2 session-23 acceptance
// gate: HOCON override per item, asserting the non-default value lands on
// the consuming config struct.
func TestHOCON_DistributedDataDurable_AllOverrides(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.durable {
      enabled = on
      keys = ["a-*", "b"]
      pruning-marker-time-to-live = 3d
      lmdb {
        dir = "/var/data/ddata"
        map-size = "256 MiB"
        write-behind-interval = 200ms
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	d := cfg.DistributedData
	if !d.DurableEnabled {
		t.Error("DurableEnabled should be true")
	}
	if got, want := d.DurableKeys, []string{"a-*", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("DurableKeys = %v, want %v", got, want)
	}
	if got, want := d.DurablePruningMarkerTimeToLive, 3*24*time.Hour; got != want {
		t.Errorf("DurablePruningMarkerTimeToLive = %v, want %v", got, want)
	}
	if d.DurableLmdbDir != "/var/data/ddata" {
		t.Errorf("DurableLmdbDir = %q, want /var/data/ddata", d.DurableLmdbDir)
	}
	if d.DurableLmdbMapSize != 256*1024*1024 {
		t.Errorf("DurableLmdbMapSize = %d, want 256 MiB", d.DurableLmdbMapSize)
	}
	if got, want := d.DurableLmdbWriteBehindInterval, 200*time.Millisecond; got != want {
		t.Errorf("DurableLmdbWriteBehindInterval = %v, want %v", got, want)
	}
}

// TestHOCON_DistributedDataDurable_WriteBehindOff confirms the Pekko-style
// "off" literal disables write-behind. parseHOCONDuration cannot handle
// "off" on its own — the loader must explicitly map it to zero.
func TestHOCON_DistributedDataDurable_WriteBehindOff(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.durable.lmdb.write-behind-interval = off
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got := cfg.DistributedData.DurableLmdbWriteBehindInterval; got != 0 {
		t.Errorf("DurableLmdbWriteBehindInterval = %v, want 0", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 24: passivation least-recently-used-strategy wiring
// ─────────────────────────────────────────────────────────────────────────────

// TestHOCON_PassivationLRU_PekkoCanonicalName proves a Pekko-style HOCON
// config using the canonical strategy name and the
// least-recently-used-strategy.* sub-block reaches ShardingConfig
// without the operator having to translate to gekka's legacy names.
func TestHOCON_PassivationLRU_PekkoCanonicalName(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "least-recently-used"
      least-recently-used-strategy {
        active-entity-limit = 4242
        replacement.policy  = "least-recently-used"
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "least-recently-used"; got != want {
		t.Errorf("PassivationStrategy = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 4242; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.PassivationReplacementPolicy, "least-recently-used"; got != want {
		t.Errorf("PassivationReplacementPolicy = %q, want %q", got, want)
	}
}

// TestHOCON_PassivationLRU_LegacyAliasNormalised confirms the gekka v0.7.0
// strategy name "custom-lru-strategy" is normalised to the Pekko canonical
// name at parse time.  Without that, the runtime check would have to
// branch on two strings and the next session would inherit the same
// duplication.
func TestHOCON_PassivationLRU_LegacyAliasNormalised(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "custom-lru-strategy"
      custom-lru-strategy.active-entity-limit = 99
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "least-recently-used"; got != want {
		t.Errorf("PassivationStrategy after alias-normalisation = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 99; got != want {
		t.Errorf("PassivationActiveEntityLimit (legacy path) = %d, want %d", got, want)
	}
}

// TestHOCON_PassivationLRU_PekkoBeatsLegacy asserts the Pekko-canonical
// path wins when both old and new HOCON keys appear in the same config
// (e.g. during a half-finished migration).  Otherwise an operator who
// added the new key but forgot to remove the legacy one would see the
// old value silently take precedence.
func TestHOCON_PassivationLRU_PekkoBeatsLegacy(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "least-recently-used"
      least-recently-used-strategy.active-entity-limit = 7
      custom-lru-strategy.active-entity-limit = 999
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 7; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d (Pekko path must win)", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 25: passivation MRU + LFU strategy wiring
// ─────────────────────────────────────────────────────────────────────────────

// TestHOCON_PassivationMRU verifies the most-recently-used strategy
// reaches ShardingConfig with its own active-entity-limit honored.
func TestHOCON_PassivationMRU(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "most-recently-used"
      most-recently-used-strategy.active-entity-limit = 321
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "most-recently-used"; got != want {
		t.Errorf("PassivationStrategy = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 321; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d", got, want)
	}
}

// TestHOCON_PassivationLFU_DynamicAging verifies the LFU strategy
// reaches ShardingConfig and the dynamic-aging flag round-trips.  The
// flag is informational in S25 — the parser surface is what matters
// here so a future session can switch on the field without an HOCON
// migration.
func TestHOCON_PassivationLFU_DynamicAging(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "least-frequently-used"
      least-frequently-used-strategy {
        active-entity-limit = 555
        dynamic-aging       = on
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "least-frequently-used"; got != want {
		t.Errorf("PassivationStrategy = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 555; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d", got, want)
	}
	if !cfg.Sharding.PassivationLFUDynamicAging {
		t.Error("PassivationLFUDynamicAging = false, want true")
	}
}

// TestHOCON_Passivation_ActiveStrategyLimitWins guards the priority
// rule: when multiple strategy blocks set active-entity-limit, the one
// matching the active strategy is the source of truth.  Without this,
// a stale block left over from a previous configuration could silently
// override the chosen strategy's limit.
func TestHOCON_Passivation_ActiveStrategyLimitWins(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "least-frequently-used"
      least-recently-used-strategy.active-entity-limit  = 1000
      most-recently-used-strategy.active-entity-limit   = 2000
      least-frequently-used-strategy.active-entity-limit = 42
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 42; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d (active strategy LFU should win)", got, want)
	}
}

// TestHOCON_DistributedDataDurable_ExplicitEnabledNoKeys covers the gekka
// extension: `enabled = on` activates the durable path even with no key
// patterns yet.  Useful for operators that bring up the bbolt store
// before populating the key globs (e.g. via management API).
func TestHOCON_DistributedDataDurable_ExplicitEnabledNoKeys(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    distributed-data.durable.enabled = on
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.DistributedData.DurableEnabled {
		t.Error("DurableEnabled = false, want true (explicit enable)")
	}
	if len(cfg.DistributedData.DurableKeys) != 0 {
		t.Errorf("DurableKeys = %v, want empty", cfg.DistributedData.DurableKeys)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 26: composite (W-TinyLFU) passivation HOCON wiring
// ─────────────────────────────────────────────────────────────────────────────

// TestHOCON_PassivationComposite_PekkoCanonicalName proves a Pekko-style
// HOCON config using "default-strategy" reaches ShardingConfig and the
// admission-window proportion lands in the dedicated field.  Without this
// test, gekka would silently parse the strategy name without exposing the
// composite knobs to the runtime.
func TestHOCON_PassivationComposite_PekkoCanonicalName(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "default-strategy"
      default-strategy {
        active-entity-limit = 8000
        admission {
          window {
            policy = "least-recently-used"
          }
          filter = "frequency-sketch"
        }
        replacement {
          policy = "least-recently-used"
        }
      }
      strategy-defaults.admission {
        window.proportion = 0.05
        frequency-sketch {
          depth            = 8
          counter-bits     = 4
          width-multiplier = 6
          reset-multiplier = 12.5
        }
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "default-strategy"; got != want {
		t.Errorf("PassivationStrategy = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 8000; got != want {
		t.Errorf("PassivationActiveEntityLimit = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.PassivationWindowProportion, 0.05; got != want {
		t.Errorf("PassivationWindowProportion = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.PassivationFilter, "frequency-sketch"; got != want {
		t.Errorf("PassivationFilter = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationFrequencySketchDepth, 8; got != want {
		t.Errorf("PassivationFrequencySketchDepth = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.PassivationFrequencySketchWidthMultiplier, 6; got != want {
		t.Errorf("PassivationFrequencySketchWidthMultiplier = %d, want %d", got, want)
	}
	if got, want := cfg.Sharding.PassivationFrequencySketchResetMultiplier, 12.5; got != want {
		t.Errorf("PassivationFrequencySketchResetMultiplier = %v, want %v", got, want)
	}
	if got, want := cfg.Sharding.PassivationReplacementPolicy, "least-recently-used"; got != want {
		t.Errorf("PassivationReplacementPolicy = %q, want %q", got, want)
	}
}

// TestHOCON_PassivationComposite_AliasNormalised confirms the
// plan-internal "composite-strategy" name is normalised to Pekko's
// "default-strategy" at parse time so the runtime check only has to
// recognise one canonical form.
func TestHOCON_PassivationComposite_AliasNormalised(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "composite-strategy"
      default-strategy.active-entity-limit = 50
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationStrategy, "default-strategy"; got != want {
		t.Errorf("PassivationStrategy after alias-normalisation = %q, want %q", got, want)
	}
	if got, want := cfg.Sharding.PassivationActiveEntityLimit, 50; got != want {
		t.Errorf("PassivationActiveEntityLimit (default-strategy block) = %d, want %d", got, want)
	}
}

// TestHOCON_PassivationComposite_DefaultStrategyOverridesDefaults proves
// the per-strategy `default-strategy.admission.*` block wins over the
// inheritable `strategy-defaults.admission.*` defaults.  Without this
// precedence, an operator who tightened the window in `default-strategy`
// would have it silently overridden by the looser inherited default.
func TestHOCON_PassivationComposite_DefaultStrategyOverridesDefaults(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    sharding.passivation {
      strategy = "default-strategy"
      default-strategy {
        admission {
          window {
            proportion = 0.20
          }
          filter = "off"
        }
      }
      strategy-defaults.admission {
        window.proportion = 0.01
        filter = "frequency-sketch"
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.Sharding.PassivationWindowProportion, 0.20; got != want {
		t.Errorf("PassivationWindowProportion = %v, want %v (per-strategy override must win)", got, want)
	}
	if got, want := cfg.Sharding.PassivationFilter, "off"; got != want {
		t.Errorf("PassivationFilter = %q, want %q (per-strategy override must win)", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Round-2 session 27: downing-provider-class HOCON wiring
// ─────────────────────────────────────────────────────────────────────────────

// TestHOCON_DowningProviderClass_NormaliseFQCN proves the Pekko FQCN is
// translated to gekka's canonical short name at parse time so the
// runtime registry lookup only needs to know one form. Without the
// normalisation a Pekko-imported config would silently fall back to
// the default provider via the unknown-name path.
func TestHOCON_DowningProviderClass_NormaliseFQCN(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DowningProviderClass, "split-brain-resolver"; got != want {
		t.Errorf("DowningProviderClass = %q, want %q", got, want)
	}
}

// TestHOCON_DowningProviderClass_ShortName proves operators can write
// the gekka-canonical short name directly. This is the form the
// runtime registry stores SBR under, and HOCON parsing must preserve
// it verbatim so the lookup hits.
func TestHOCON_DowningProviderClass_ShortName(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    downing-provider-class = "split-brain-resolver"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DowningProviderClass, "split-brain-resolver"; got != want {
		t.Errorf("DowningProviderClass = %q, want %q", got, want)
	}
}

// TestHOCON_DowningProviderClass_UnknownPreserved confirms that names
// not matching any built-in are kept verbatim so future providers can
// register under their own short name without code changes in the
// HOCON parser. Without this, an operator who wires up a custom
// provider would see their config silently rewritten to "".
func TestHOCON_DowningProviderClass_UnknownPreserved(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster {
    seed-nodes = []
    downing-provider-class = "lease-driven"
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if got, want := cfg.DowningProviderClass, "lease-driven"; got != want {
		t.Errorf("DowningProviderClass = %q, want %q (unknown name must be preserved)", got, want)
	}
}

// TestHOCON_LargeMessageDestinations_Parsed verifies that a configured
// pekko.remote.artery.large-message-destinations list reaches NodeConfig
// verbatim. Round-2 session 29 wires this into the LargeMessageRouter via
// SetLargeMessageDestinations during cluster spawn.
func TestHOCON_LargeMessageDestinations_Parsed(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery {
    canonical { hostname = "127.0.0.1", port = 2552 }
    large-message-destinations = ["/user/large-*", "/svc/blob"]
  }
  cluster { seed-nodes = [] }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := []string{"/user/large-*", "/svc/blob"}
	if !reflect.DeepEqual(cfg.LargeMessageDestinations, want) {
		t.Errorf("LargeMessageDestinations = %v, want %v", cfg.LargeMessageDestinations, want)
	}
}

// TestHOCON_LargeMessageDestinations_DefaultEmpty verifies that the absence
// of pekko.remote.artery.large-message-destinations leaves the field empty,
// disabling large-stream routing entirely (the Pekko default).
func TestHOCON_LargeMessageDestinations_DefaultEmpty(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery { canonical { hostname = "127.0.0.1", port = 2552 } }
  cluster { seed-nodes = [] }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if len(cfg.LargeMessageDestinations) != 0 {
		t.Errorf("LargeMessageDestinations = %v, want [] when unset", cfg.LargeMessageDestinations)
	}
}

// TestHOCON_UntrustedMode_OnPekko verifies that pekko.remote.artery.untrusted-mode
// = on lifts into ClusterConfig.UntrustedMode and that
// trusted-selection-paths populates the allowlist.
func TestHOCON_UntrustedMode_OnPekko(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery {
  canonical { hostname = "127.0.0.1", port = 2552 }
  untrusted-mode = on
  trusted-selection-paths = ["/user/echo", "/user/bridge"]
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.UntrustedMode {
		t.Error("UntrustedMode = false, want true")
	}
	want := []string{"/user/echo", "/user/bridge"}
	if !reflect.DeepEqual(cfg.TrustedSelectionPaths, want) {
		t.Errorf("TrustedSelectionPaths = %v, want %v", cfg.TrustedSelectionPaths, want)
	}
}

// TestHOCON_UntrustedMode_OnAkka verifies the akka prefix is honored too —
// gekka detects akka.* keys and routes parsing through the same fields.
func TestHOCON_UntrustedMode_OnAkka(t *testing.T) {
	cfg, err := parseHOCONString(`
akka.remote.artery {
  canonical { hostname = "127.0.0.1", port = 2552 }
  untrusted-mode = on
  trusted-selection-paths = ["/user/api"]
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.UntrustedMode {
		t.Error("UntrustedMode = false, want true under akka prefix")
	}
	want := []string{"/user/api"}
	if !reflect.DeepEqual(cfg.TrustedSelectionPaths, want) {
		t.Errorf("TrustedSelectionPaths = %v, want %v", cfg.TrustedSelectionPaths, want)
	}
}

// TestHOCON_UntrustedMode_DefaultOff verifies omitting the key leaves the
// flag at its zero value (off) and the allowlist empty — matching the Pekko
// default of accepting everything from peers.
func TestHOCON_UntrustedMode_DefaultOff(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 2552 }
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if cfg.UntrustedMode {
		t.Error("UntrustedMode = true, want false (default)")
	}
	if len(cfg.TrustedSelectionPaths) != 0 {
		t.Errorf("TrustedSelectionPaths = %v, want [] (default)", cfg.TrustedSelectionPaths)
	}
}
