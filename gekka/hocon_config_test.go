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

	"gekka/gekka/actor"
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
	node, err := SpawnFromConfig("testdata/pekko.conf")
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

func TestJoinSeeds_NoSeeds(t *testing.T) {
	node, err := Spawn(NodeConfig{Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	err = node.JoinSeeds()
	if err == nil {
		t.Error("expected error from JoinSeeds with no seeds, got nil")
	}
}
