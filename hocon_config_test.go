/*
 * hocon_config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
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
