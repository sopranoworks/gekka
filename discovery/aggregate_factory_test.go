/*
 * aggregate_factory_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package discovery

import (
	"errors"
	"sort"
	"testing"
)

// stubFactory installs a SeedProvider returning the given nodes/error under
// `name` for the duration of the test.
func stubFactory(t *testing.T, name string, nodes []string, fetchErr error) {
	t.Helper()
	Register(name, func(_ DiscoveryConfig) (SeedProvider, error) {
		return &mockProvider{nodes: nodes, err: fetchErr}, nil
	})
	t.Cleanup(func() { Deregister(name) })
}

func TestAggregateFactory_ResolvesChildren(t *testing.T) {
	stubFactory(t, "agg-test-a", []string{"a:1"}, nil)
	stubFactory(t, "agg-test-b", []string{"b:1"}, nil)

	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []string{"agg-test-a", "agg-test-b"},
	}}
	p, err := aggregateFactory(cfg)
	if err != nil {
		t.Fatalf("aggregateFactory: %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: %v", err)
	}
	sort.Strings(got)
	want := []string{"a:1", "b:1"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("seeds = %v, want %v", got, want)
	}
}

func TestAggregateFactory_AcceptsAnySliceForm(t *testing.T) {
	// hocon_config.go puts methods into Config.Config as []string after
	// Unmarshal, but the registry API contract is map[string]any so a []any
	// slice can also legitimately arrive here.  Both must be honoured.
	stubFactory(t, "agg-test-c", []string{"c:1"}, nil)

	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []any{"agg-test-c"},
	}}
	p, err := aggregateFactory(cfg)
	if err != nil {
		t.Fatalf("aggregateFactory: %v", err)
	}
	got, _ := p.FetchSeedNodes()
	if len(got) != 1 || got[0] != "c:1" {
		t.Errorf("seeds = %v, want [c:1]", got)
	}
}

func TestAggregateFactory_OneChildErrorOtherSucceeds(t *testing.T) {
	stubFactory(t, "agg-test-ok", []string{"ok:1"}, nil)
	stubFactory(t, "agg-test-bad", nil, errors.New("kaput"))

	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []string{"agg-test-bad", "agg-test-ok"},
	}}
	p, err := aggregateFactory(cfg)
	if err != nil {
		t.Fatalf("aggregateFactory: %v", err)
	}
	got, err := p.FetchSeedNodes()
	if err != nil {
		t.Fatalf("FetchSeedNodes: aggregator should tolerate one failing child, got: %v", err)
	}
	if len(got) != 1 || got[0] != "ok:1" {
		t.Errorf("seeds = %v, want [ok:1]", got)
	}
}

func TestAggregateFactory_EmptyMethodsRejected(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []string{},
	}}
	if _, err := aggregateFactory(cfg); err == nil {
		t.Error("expected error when discovery-methods is empty (Pekko parity)")
	}
}

func TestAggregateFactory_MissingMethodsRejected(t *testing.T) {
	if _, err := aggregateFactory(DiscoveryConfig{Config: map[string]any{}}); err == nil {
		t.Error("expected error when discovery-methods is missing")
	}
}

func TestAggregateFactory_RecursionRejected(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []string{"aggregate"},
	}}
	if _, err := aggregateFactory(cfg); err == nil {
		t.Error("expected error when discovery-methods contains \"aggregate\" (would recurse)")
	}
}

func TestAggregateFactory_UnknownChildRejected(t *testing.T) {
	cfg := DiscoveryConfig{Config: map[string]any{
		"discovery-methods": []string{"definitely-not-registered-xyz"},
	}}
	if _, err := aggregateFactory(cfg); err == nil {
		t.Error("expected error when child method is not in registry")
	}
}
