/*
 * metrics_gossip.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/ddata"
)

const MetricsMapKey = "_cluster_metrics"

// MetricsGossip handles periodic collection and distribution of node pressure metrics.
type MetricsGossip struct {
	repl      *ddata.Replicator
	collector *actor.MetricsCollector
	nodeID    string
	interval  time.Duration
}

// NewMetricsGossip creates a new MetricsGossip instance.
func NewMetricsGossip(nodeID string, repl *ddata.Replicator, interval time.Duration) *MetricsGossip {
	return &MetricsGossip{
		repl:      repl,
		collector: actor.NewMetricsCollector(),
		nodeID:    nodeID,
		interval:  interval,
	}
}

// Collector returns the internal metrics collector.
func (g *MetricsGossip) Collector() *actor.MetricsCollector {
	return g.collector
}

// Start begins the metrics collection and gossip loop.
func (g *MetricsGossip) Start(ctx context.Context) {
	ticker := time.NewTicker(g.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pressure := g.collector.Collect()
				g.repl.PutInMap(MetricsMapKey, g.nodeID, pressure, ddata.WriteLocal)
			}
		}
	}()
}

// ClusterPressure returns a map of node IDs to their pressure status.
func (g *MetricsGossip) ClusterPressure() map[string]actor.NodePressure {
	m := g.repl.LWWMap(MetricsMapKey)
	entries := m.Entries()
	res := make(map[string]actor.NodePressure, len(entries))
	for k, v := range entries {
		if p, ok := v.(actor.NodePressure); ok {
			res[k] = p
		} else if m, ok := v.(map[string]any); ok {
			// Handle unmarshaled JSON object
			data, _ := json.Marshal(m)
			var np actor.NodePressure
			if err := json.Unmarshal(data, &np); err == nil {
				res[k] = np
			}
		}
	}
	return res
}
