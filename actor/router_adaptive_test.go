/*
 * router_adaptive_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockClusterMetricsProvider struct {
	pressure map[string]NodePressure
}

func (m *mockClusterMetricsProvider) GetClusterPressure() map[string]NodePressure {
	return m.pressure
}

func TestRouter_Adaptive(t *testing.T) {
	// Setup mock routees
	w1 := &FunctionalMockRef{PathURI: "pekko://Sys@node1:2552/user/w1", Handler: func(any) {}}
	w2 := &FunctionalMockRef{PathURI: "pekko://Sys@node2:2552/user/w2", Handler: func(any) {}}

	routees := []Ref{w1, w2}

	// 1. High pressure on node1, low on node2
	mockProvider := &mockClusterMetricsProvider{
		pressure: map[string]NodePressure{
			"node1:2552": {Score: 0.9},
			"node2:2552": {Score: 0.1},
		},
	}
	SetClusterMetricsProvider(mockProvider)

	logic := &AdaptiveLoadBalancingRoutingLogic{}

	// Sample distribution
	const samples = 1000
	counts := make(map[string]int)
	for i := 0; i < samples; i++ {
		target := logic.Select("test", routees)
		counts[target.Path()]++
	}

	t.Logf("Counts: w1=%d, w2=%d", counts[w1.Path()], counts[w2.Path()])

	// w2 should have significantly more hits than w1
	assert.True(t, counts[w2.Path()] > counts[w1.Path()]*2, "w2 should have more traffic than w1")

	// 2. High pressure on node2, low on node1
	mockProvider.pressure = map[string]NodePressure{
		"node1:2552": {Score: 0.1},
		"node2:2552": {Score: 0.9},
	}

	counts = make(map[string]int)
	for i := 0; i < samples; i++ {
		target := logic.Select("test", routees)
		counts[target.Path()]++
	}

	t.Logf("Counts after swap: w1=%d, w2=%d", counts[w1.Path()], counts[w2.Path()])
	assert.True(t, counts[w1.Path()] > counts[w2.Path()]*2, "w1 should have more traffic than w2")
}

func TestRouter_AdaptiveFallback(t *testing.T) {
	SetClusterMetricsProvider(nil) // Ensure no provider

	w1 := &FunctionalMockRef{PathURI: "/user/w1", Handler: func(any) {}}
	w2 := &FunctionalMockRef{PathURI: "/user/w2", Handler: func(any) {}}
	routees := []Ref{w1, w2}

	logic := &AdaptiveLoadBalancingRoutingLogic{}

	// Should fallback to RoundRobin
	target1 := logic.Select("test", routees)
	target2 := logic.Select("test", routees)
	target3 := logic.Select("test", routees)

	assert.NotEqual(t, target1.Path(), target2.Path())
	assert.Equal(t, target1.Path(), target3.Path())
}
