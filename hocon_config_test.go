/*
 * hocon_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
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
pekko.cluster.sharding.passivation.idle-timeout = "2m"
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
	// The correct Pekko path takes priority over the legacy short path.
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = "5m"
pekko.cluster.sharding.passivation.idle-timeout = "2m"
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	// Should use the correct Pekko path (5m), not the fallback (2m).
	if cfg.Sharding.PassivationIdleTimeout != 5*time.Minute {
		t.Errorf("PassivationIdleTimeout = %v, want 5m (correct Pekko path)", cfg.Sharding.PassivationIdleTimeout)
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
