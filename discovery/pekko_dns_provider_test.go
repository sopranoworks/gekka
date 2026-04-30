/*
 * pekko_dns_provider_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"errors"
	"net"
	"sort"
	"testing"
)

func TestPekkoDNSProvider_ResolvesSRV(t *testing.T) {
	p := NewPekkoDNSProvider("svc.local", 2551)
	p.lookupSRV = func(_, _, name string) (string, []*net.SRV, error) {
		if name != "svc.local" {
			t.Errorf("lookupSRV name = %q, want svc.local", name)
		}
		return "svc.local", []*net.SRV{
			{Target: "node1.svc.local", Port: 1234},
			{Target: "node2.svc.local", Port: 0},
		}, nil
	}
	p.lookupHost = func(host string) ([]string, error) {
		switch host {
		case "node1.svc.local":
			return []string{"10.0.0.1"}, nil
		case "node2.svc.local":
			return []string{"10.0.0.2"}, nil
		}
		return nil, errors.New("unknown host")
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	sort.Strings(got)
	want := []string{"10.0.0.1:1234", "10.0.0.2:2551"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("seeds = %v, want %v", got, want)
	}
}

func TestPekkoDNSProvider_NoSeedsIsError(t *testing.T) {
	p := NewPekkoDNSProvider("svc.local", 2551)
	p.lookupSRV = func(_, _, _ string) (string, []*net.SRV, error) {
		return "", nil, nil
	}
	if _, err := p.FetchSeedNodes(); err == nil {
		t.Error("expected error when SRV lookup returns no records")
	}
}

func TestPekkoDNSProvider_LookupErrorPropagated(t *testing.T) {
	p := NewPekkoDNSProvider("svc.local", 2551)
	p.lookupSRV = func(_, _, _ string) (string, []*net.SRV, error) {
		return "", nil, errors.New("dns down")
	}
	if _, err := p.FetchSeedNodes(); err == nil {
		t.Error("expected error when SRV lookup fails")
	}
}

func TestPekkoDNSProvider_FactoryRequiresServiceName(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{}}
	if _, err := pekkoDNSFactory(cfg); err == nil {
		t.Error("expected error when service-name is missing")
	}
}

func TestPekkoDNSProvider_FactoryViaRegistry(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"service-name": "svc.local",
		"port":         2551,
	}}
	p, err := Get("pekko-dns", cfg)
	if err != nil {
		t.Fatalf("Get(pekko-dns): %v", err)
	}
	if p == nil {
		t.Fatal("provider is nil")
	}
}

func TestPekkoDNSProvider_EmptyServiceNameAtFetch(t *testing.T) {
	// Bypass NewPekkoDNSProvider's prepopulated fields to assert FetchSeedNodes
	// guards on empty serviceName.
	p := &PekkoDNSProvider{}
	if _, err := p.FetchSeedNodes(); err == nil {
		t.Error("expected error when serviceName is empty")
	}
}
