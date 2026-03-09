/*
 * provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"strings"
	"testing"
)

func TestProvider_ProtoString(t *testing.T) {
	if s := ProviderPekko.protoString(); s != "pekko" {
		t.Errorf("ProviderPekko.protoString() = %q, want %q", s, "pekko")
	}
	if s := ProviderAkka.protoString(); s != "akka" {
		t.Errorf("ProviderAkka.protoString() = %q, want %q", s, "akka")
	}
}

func TestProvider_Default(t *testing.T) {
	// Zero value of NodeConfig.Provider must be ProviderPekko.
	var cfg NodeConfig
	if cfg.Provider != ProviderPekko {
		t.Errorf("default Provider = %v, want ProviderPekko", cfg.Provider)
	}
	if cfg.Provider.protoString() != "pekko" {
		t.Errorf("default protoString = %q, want %q", cfg.Provider.protoString(), "pekko")
	}
}

func TestProvider_SpawnPekko_UsesCorrectProtocol(t *testing.T) {
	node, err := Spawn(NodeConfig{
		SystemName: "TestSystem",
		Host:       "127.0.0.1",
		Port:       0,
		Provider:   ProviderPekko,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	if got := node.localAddr.GetProtocol(); got != "pekko" {
		t.Errorf("localAddr.Protocol = %q, want %q", got, "pekko")
	}
	if got := node.cm.proto(); got != "pekko" {
		t.Errorf("cm.proto() = %q, want %q", got, "pekko")
	}
}

func TestProvider_SpawnAkka_UsesCorrectProtocol(t *testing.T) {
	node, err := Spawn(NodeConfig{
		SystemName: "TestSystem",
		Host:       "127.0.0.1",
		Port:       0,
		Provider:   ProviderAkka,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	if got := node.localAddr.GetProtocol(); got != "akka" {
		t.Errorf("localAddr.Protocol = %q, want %q", got, "akka")
	}
	if got := node.cm.proto(); got != "akka" {
		t.Errorf("cm.proto() = %q, want %q", got, "akka")
	}
}

func TestProvider_ClusterCorePath(t *testing.T) {
	tests := []struct {
		provider Provider
		want     string
	}{
		{ProviderPekko, "pekko://ClusterSystem@127.0.0.1:2552/system/cluster/core/daemon"},
		{ProviderAkka, "akka://ClusterSystem@127.0.0.1:2552/system/cluster/core/daemon"},
	}
	for _, tt := range tests {
		node, err := Spawn(NodeConfig{Provider: tt.provider, Port: 0})
		if err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		got := node.cm.clusterCorePath("ClusterSystem", "127.0.0.1", 2552)
		_ = node.Shutdown()
		if got != tt.want {
			t.Errorf("provider=%v clusterCorePath = %q, want %q", tt.provider, got, tt.want)
		}
	}
}

func TestProvider_HeartbeatPath(t *testing.T) {
	tests := []struct {
		provider Provider
		want     string
	}{
		{ProviderPekko, "pekko://ClusterSystem@127.0.0.1:2552/system/cluster/heartbeatReceiver"},
		{ProviderAkka, "akka://ClusterSystem@127.0.0.1:2552/system/cluster/heartbeatReceiver"},
	}
	for _, tt := range tests {
		node, err := Spawn(NodeConfig{Provider: tt.provider, Port: 0})
		if err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		got := node.cm.heartbeatPath("ClusterSystem", "127.0.0.1", 2552)
		_ = node.Shutdown()
		if got != tt.want {
			t.Errorf("provider=%v heartbeatPath = %q, want %q", tt.provider, got, tt.want)
		}
	}
}

func TestProvider_JoinUsesCorrectScheme(t *testing.T) {
	// When Join is called, it should log the cluster core path using the configured scheme.
	// We verify this by checking the seedAddr protocol that Join stores.
	node, err := Spawn(NodeConfig{
		SystemName: "ClusterSystem",
		Host:       "127.0.0.1",
		Port:       0,
		Provider:   ProviderAkka,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	// Join will fail (no Akka server), but seedAddr is set before the dial.
	// Use a background context that we cancel immediately to abort the dial fast.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = node.Join("127.0.0.1", 2552) // dial will fail; ignore error

	if node.seedAddr == nil {
		t.Fatal("seedAddr is nil after Join")
	}
	if got := node.seedAddr.GetProtocol(); got != "akka" {
		t.Errorf("seedAddr.Protocol = %q, want %q", got, "akka")
	}
	_ = ctx
}

func TestProvider_SendPath_PekkoVsAkka(t *testing.T) {
	// ParseActorPath must handle both "pekko://" and "akka://" schemes.
	paths := []string{
		"pekko://MySystem@127.0.0.1:2552/user/actor",
		"akka://MySystem@127.0.0.1:2552/user/actor",
	}
	for _, p := range paths {
		ap, err := ParseActorPath(p)
		if err != nil {
			t.Errorf("ParseActorPath(%q) error: %v", p, err)
			continue
		}
		if ap.System != "MySystem" {
			t.Errorf("path=%q System=%q, want MySystem", p, ap.System)
		}
		if ap.Host != "127.0.0.1" {
			t.Errorf("path=%q Host=%q, want 127.0.0.1", p, ap.Host)
		}
		if ap.Port != 2552 {
			t.Errorf("path=%q Port=%d, want 2552", p, ap.Port)
		}
		scheme := strings.SplitN(p, "://", 2)[0]
		if ap.Protocol != scheme {
			t.Errorf("path=%q Protocol=%q, want %q", p, ap.Protocol, scheme)
		}
	}
}
