/*
 * aggregate_test.go
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

type mockProvider struct {
	nodes []string
	err   error
}

func (m *mockProvider) FetchSeedNodes() ([]string, error) {
	return m.nodes, m.err
}

func TestAggregateProvider_MergesResults(t *testing.T) {
	p1 := &mockProvider{nodes: []string{"host1:2551", "host2:2551"}}
	p2 := &mockProvider{nodes: []string{"host2:2551", "host3:2551"}}

	agg := NewAggregateProvider(p1, p2)
	nodes, err := agg.FetchSeedNodes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sort.Strings(nodes)
	expected := []string{"host1:2551", "host2:2551", "host3:2551"}
	if len(nodes) != len(expected) {
		t.Fatalf("got %v, want %v", nodes, expected)
	}
	for i, v := range nodes {
		if v != expected[i] {
			t.Errorf("nodes[%d] = %s, want %s", i, v, expected[i])
		}
	}
}

func TestAggregateProvider_OneFailsOtherWorks(t *testing.T) {
	p1 := &mockProvider{err: errors.New("dns failed")}
	p2 := &mockProvider{nodes: []string{"host1:2551"}}

	agg := NewAggregateProvider(p1, p2)
	nodes, err := agg.FetchSeedNodes()
	if err != nil {
		t.Fatalf("expected success when one provider works, got: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "host1:2551" {
		t.Errorf("got %v, want [host1:2551]", nodes)
	}
}

func TestAggregateProvider_AllFail(t *testing.T) {
	p1 := &mockProvider{err: errors.New("err1")}
	p2 := &mockProvider{err: errors.New("err2")}

	agg := NewAggregateProvider(p1, p2)
	_, err := agg.FetchSeedNodes()
	if err == nil {
		t.Fatal("expected error when all providers fail")
	}
}

func TestAggregateProvider_Empty(t *testing.T) {
	agg := NewAggregateProvider()
	nodes, err := agg.FetchSeedNodes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("expected empty result, got %v", nodes)
	}
}
