/*
 * config_provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"sort"
	"testing"
)

func TestConfigProvider_FetchSeedNodes(t *testing.T) {
	services := map[string]any{
		"svc": map[string]any{
			"endpoints": []any{"host1:1233", "host2"},
		},
	}
	p, err := NewConfigProvider("svc", services, 2551)
	if err != nil {
		t.Fatalf("NewConfigProvider: %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	sort.Strings(got)
	want := []string{"host1:1233", "host2:2551"}
	if len(got) != len(want) {
		t.Fatalf("seeds = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("seeds[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestConfigProvider_DropsPortlessWithoutDefault(t *testing.T) {
	services := map[string]any{
		"svc": map[string]any{
			"endpoints": []any{"host1:1233", "host2"},
		},
	}
	p, err := NewConfigProvider("svc", services, 0) // no default
	if err != nil {
		t.Fatalf("NewConfigProvider: %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	if len(got) != 1 || got[0] != "host1:1233" {
		t.Errorf("seeds = %v, want [host1:1233]", got)
	}
}

func TestConfigProvider_StringSliceForm(t *testing.T) {
	// Demonstrates the []string variant of endpoints (the intermediate
	// representation produced by hocon_config.go before being placed into
	// DiscoveryConfig.Config — verifies the coerceStringList path).
	services := map[string]any{
		"svc": map[string]any{
			"endpoints": []string{"a:1", "b:2"},
		},
	}
	p, err := NewConfigProvider("svc", services, 0)
	if err != nil {
		t.Fatalf("NewConfigProvider: %v", err)
	}
	got, _ := p.FetchSeedNodes()
	sort.Strings(got)
	want := []string{"a:1", "b:2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("seeds = %v, want %v", got, want)
	}
}

func TestConfigProvider_UnknownService(t *testing.T) {
	services := map[string]any{
		"svc": map[string]any{"endpoints": []any{"host:1"}},
	}
	p, err := NewConfigProvider("missing", services, 0)
	if err != nil {
		t.Fatalf("NewConfigProvider: %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("seeds = %v, want empty", got)
	}
}

func TestConfigProvider_FactoryRequiresServiceName(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"services": map[string]any{"svc": map[string]any{"endpoints": []any{"h:1"}}},
	}}
	if _, err := configFactory(cfg); err == nil {
		t.Error("expected error when service-name is missing")
	}
}

func TestConfigProvider_FactoryViaRegistry(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"service-name": "svc",
		"default-port": 2551,
		"services": map[string]any{
			"svc": map[string]any{"endpoints": []any{"a:1", "b"}},
		},
	}}
	p, err := Get("config", cfg)
	if err != nil {
		t.Fatalf("Get(config): %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	sort.Strings(got)
	want := []string{"a:1", "b:2551"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("seeds = %v, want %v", got, want)
	}
}

func TestConfigProvider_BadEndpointShape(t *testing.T) {
	services := map[string]any{
		"svc": map[string]any{
			"endpoints": []any{42},
		},
	}
	if _, err := NewConfigProvider("svc", services, 0); err == nil {
		t.Error("expected error when endpoints contains a non-string element")
	}
}

func TestSplitHostPort(t *testing.T) {
	cases := []struct {
		in       string
		host     string
		port     int
		wantErr  bool
		wantPort int
	}{
		{"h:1", "h", 1, false, 1},
		{"h", "h", 0, false, 0},
		{"h:", "h", 0, false, 0},
		{":1", "", 0, true, 0},
		{"h:abc", "", 0, true, 0},
	}
	for _, c := range cases {
		host, port, err := splitHostPort(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("splitHostPort(%q): expected error, got (%q, %d)", c.in, host, port)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitHostPort(%q): unexpected error: %v", c.in, err)
			continue
		}
		if host != c.host || port != c.port {
			t.Errorf("splitHostPort(%q) = (%q, %d), want (%q, %d)", c.in, host, port, c.host, c.port)
		}
	}
}
