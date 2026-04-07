/*
 * metrics_router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
)

type metricsRouterTestRef struct {
	path string
}

func (r *metricsRouterTestRef) Path() string                      { return r.path }
func (r *metricsRouterTestRef) Tell(msg any, sender ...actor.Ref) {}

func TestClusterMetricsRoutingLogic_SelectsLeastLoaded(t *testing.T) {
	repl := ddata.NewReplicator("test-node", nil)

	mg := NewMetricsGossip("test-node", repl, time.Minute)

	// Inject pressure data into the replicator.
	repl.PutInMap(MetricsMapKey, "host-a:2551", actor.NodePressure{Score: 0.9}, ddata.WriteLocal)
	repl.PutInMap(MetricsMapKey, "host-b:2552", actor.NodePressure{Score: 0.1}, ddata.WriteLocal)

	logic := NewClusterMetricsRoutingLogic(mg)

	routees := []actor.Ref{
		&metricsRouterTestRef{path: "pekko://System@host-a:2551/user/worker"},
		&metricsRouterTestRef{path: "pekko://System@host-b:2552/user/worker"},
	}

	// With a 0.9 vs 0.1 pressure, host-b should be selected much more often.
	counts := map[string]int{"host-a": 0, "host-b": 0}
	for i := 0; i < 1000; i++ {
		ref := logic.Select("msg", routees)
		if ref.Path() == routees[0].Path() {
			counts["host-a"]++
		} else {
			counts["host-b"]++
		}
	}

	// host-b (low pressure) should get significantly more traffic.
	assert.Greater(t, counts["host-b"], counts["host-a"],
		"low-pressure node should get more traffic: a=%d, b=%d", counts["host-a"], counts["host-b"])
	assert.Greater(t, counts["host-b"], 700,
		"host-b should get >70%% of traffic with 0.1 pressure")
}

func TestClusterMetricsRoutingLogic_FallsBackToRoundRobin(t *testing.T) {
	repl := ddata.NewReplicator("test-node", nil)
	mg := NewMetricsGossip("test-node", repl, time.Minute)
	// No pressure data in replicator → should fall back to round-robin.

	logic := NewClusterMetricsRoutingLogic(mg)

	routees := []actor.Ref{
		&metricsRouterTestRef{path: "pekko://System@host-a:2551/user/worker"},
		&metricsRouterTestRef{path: "pekko://System@host-b:2552/user/worker"},
	}

	ref := logic.Select("msg", routees)
	assert.NotNil(t, ref)
}

func TestClusterMetricsRoutingLogic_CustomWeightFunc(t *testing.T) {
	repl := ddata.NewReplicator("test-node", nil)
	mg := NewMetricsGossip("test-node", repl, time.Minute)

	repl.PutInMap(MetricsMapKey, "host-a:2551", actor.NodePressure{CPUUsage: 0.95, Score: 0.5}, ddata.WriteLocal)
	repl.PutInMap(MetricsMapKey, "host-b:2552", actor.NodePressure{CPUUsage: 0.05, Score: 0.5}, ddata.WriteLocal)

	// Use CPU-only weight function
	logic := NewClusterMetricsRoutingLogic(mg, CPUWeightFunc)

	routees := []actor.Ref{
		&metricsRouterTestRef{path: "pekko://System@host-a:2551/user/worker"},
		&metricsRouterTestRef{path: "pekko://System@host-b:2552/user/worker"},
	}

	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		ref := logic.Select("msg", routees)
		counts[ref.Path()]++
	}

	// host-b has low CPU → should get much more traffic with CPU weight func
	assert.Greater(t, counts[routees[1].Path()], counts[routees[0].Path()])
}

func TestClusterMetricsRoutingLogic_SingleRoutee(t *testing.T) {
	repl := ddata.NewReplicator("test-node", nil)
	mg := NewMetricsGossip("test-node", repl, time.Minute)
	logic := NewClusterMetricsRoutingLogic(mg)

	single := &metricsRouterTestRef{path: "pekko://System@host-a:2551/user/worker"}
	ref := logic.Select("msg", []actor.Ref{single})
	assert.Equal(t, single, ref)
}

func TestClusterMetricsRoutingLogic_EmptyRoutees(t *testing.T) {
	repl := ddata.NewReplicator("test-node", nil)
	mg := NewMetricsGossip("test-node", repl, time.Minute)
	logic := NewClusterMetricsRoutingLogic(mg)

	ref := logic.Select("msg", nil)
	assert.Nil(t, ref)
}
