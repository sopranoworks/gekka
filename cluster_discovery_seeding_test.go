/*
 * cluster_discovery_seeding_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"errors"
	"net"
	"sort"
	"testing"

	"github.com/sopranoworks/gekka/discovery"
)

// TestApplyDiscoveredSeeds_ConfigProvider proves the full HOCON → ClusterConfig →
// discovery.Get → FetchSeedNodes → cfg.SeedNodes chain wires for the
// pekko.discovery.config.* namespace.  This is the runtime-effect test for
// sub-plan 3 DoD #2: the discovered seeds must show up on the input
// ClusterConfig.SeedNodes.
func TestApplyDiscoveredSeeds_ConfigProvider(t *testing.T) {
	const text = `
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 2551 }
  discovery {
    method = "config"
    config {
      service-name = "myapp"
      default-port = 2551
      services {
        myapp {
          endpoints = [ "h1:1234", "h2" ]
        }
      }
    }
  }
}
`
	cfg, err := parseHOCONString(text)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	if !cfg.Discovery.Enabled || cfg.Discovery.Type != "config" {
		t.Fatalf("HOCON did not enable config discovery: %+v", cfg.Discovery)
	}

	if err := applyDiscoveredSeeds(&cfg, "pekko", "TestSystem"); err != nil {
		t.Fatalf("applyDiscoveredSeeds: %v", err)
	}

	gotHosts := make([]string, 0, len(cfg.SeedNodes))
	for _, addr := range cfg.SeedNodes {
		gotHosts = append(gotHosts, net.JoinHostPort(addr.Host, itoa(addr.Port)))
	}
	sort.Strings(gotHosts)
	want := []string{"h1:1234", "h2:2551"}
	if len(gotHosts) != len(want) || gotHosts[0] != want[0] || gotHosts[1] != want[1] {
		t.Errorf("SeedNodes hosts = %v, want %v", gotHosts, want)
	}
	for _, addr := range cfg.SeedNodes {
		if addr.System != "TestSystem" {
			t.Errorf("seed system = %q, want TestSystem", addr.System)
		}
		if addr.Protocol != "pekko" {
			t.Errorf("seed protocol = %q, want pekko", addr.Protocol)
		}
	}
}

// TestApplyDiscoveredSeeds_AggregateProvider exercises the aggregate
// factory's compose-children-via-registry path: two stub providers register
// under unique names and an aggregate over them lands their union on
// cfg.SeedNodes.
func TestApplyDiscoveredSeeds_AggregateProvider(t *testing.T) {
	discovery.Register("agg-runtime-a", func(_ discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
		return stubSeed{nodes: []string{"a:1"}}, nil
	})
	discovery.Register("agg-runtime-b", func(_ discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
		return stubSeed{nodes: []string{"b:2"}}, nil
	})
	t.Cleanup(func() {
		discovery.Deregister("agg-runtime-a")
		discovery.Deregister("agg-runtime-b")
	})

	cfg := ClusterConfig{
		Discovery: DiscoveryConfig{
			Enabled: true,
			Type:    "aggregate",
			Config: discovery.DiscoveryConfig{Config: map[string]any{
				"discovery-methods": []string{"agg-runtime-a", "agg-runtime-b"},
			}},
		},
	}
	if err := applyDiscoveredSeeds(&cfg, "pekko", "TestSystem"); err != nil {
		t.Fatalf("applyDiscoveredSeeds: %v", err)
	}
	gotHosts := make([]string, 0, len(cfg.SeedNodes))
	for _, addr := range cfg.SeedNodes {
		gotHosts = append(gotHosts, net.JoinHostPort(addr.Host, itoa(addr.Port)))
	}
	sort.Strings(gotHosts)
	want := []string{"a:1", "b:2"}
	if len(gotHosts) != len(want) || gotHosts[0] != want[0] || gotHosts[1] != want[1] {
		t.Errorf("SeedNodes hosts = %v, want %v", gotHosts, want)
	}
}

// TestApplyDiscoveredSeeds_ProviderError treats a FetchSeedNodes failure as
// non-fatal (matches the behaviour of the inline block before sub-plan 3
// extracted it; existing v0.9.0 contract).  The cluster still starts; no
// seeds are added.
func TestApplyDiscoveredSeeds_ProviderError(t *testing.T) {
	discovery.Register("seed-runtime-err", func(_ discovery.DiscoveryConfig) (discovery.SeedProvider, error) {
		return stubSeed{err: errors.New("boom")}, nil
	})
	t.Cleanup(func() { discovery.Deregister("seed-runtime-err") })

	cfg := ClusterConfig{
		Discovery: DiscoveryConfig{
			Enabled: true,
			Type:    "seed-runtime-err",
		},
	}
	if err := applyDiscoveredSeeds(&cfg, "pekko", "TestSystem"); err != nil {
		t.Fatalf("applyDiscoveredSeeds returned error for transient FetchSeedNodes failure: %v", err)
	}
	if len(cfg.SeedNodes) != 0 {
		t.Errorf("SeedNodes = %v, want empty after fetch failure", cfg.SeedNodes)
	}
}

// TestApplyDiscoveredSeeds_UnknownProviderIsFatal — configuration error is a
// hard failure (a typo'd discovery method must not silently no-op).
func TestApplyDiscoveredSeeds_UnknownProviderIsFatal(t *testing.T) {
	cfg := ClusterConfig{
		Discovery: DiscoveryConfig{
			Enabled: true,
			Type:    "definitely-not-registered-xyz",
		},
	}
	if err := applyDiscoveredSeeds(&cfg, "pekko", "TestSystem"); err == nil {
		t.Error("expected fatal error for unknown discovery method")
	}
}

type stubSeed struct {
	nodes []string
	err   error
}

func (s stubSeed) FetchSeedNodes() ([]string, error) {
	return s.nodes, s.err
}

func itoa(p int) string {
	if p == 0 {
		return "0"
	}
	neg := p < 0
	if neg {
		p = -p
	}
	var buf [12]byte
	i := len(buf)
	for p > 0 {
		i--
		buf[i] = byte('0' + p%10)
		p /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
