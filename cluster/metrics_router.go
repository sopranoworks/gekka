/*
 * metrics_router.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"math/rand"

	"github.com/sopranoworks/gekka/actor"
)

// WeightFunc calculates a routing weight from node pressure metrics.
// Higher weight means more traffic. Must return a value >= 0.
type WeightFunc func(p actor.NodePressure) float64

// DefaultWeightFunc uses the inverse of the composite pressure score.
func DefaultWeightFunc(p actor.NodePressure) float64 {
	w := 1.0 - p.Score
	if w < 0.05 {
		w = 0.05
	}
	return w
}

// CPUWeightFunc weights nodes inversely by CPU usage only.
func CPUWeightFunc(p actor.NodePressure) float64 {
	w := 1.0 - p.CPUUsage
	if w < 0.05 {
		w = 0.05
	}
	return w
}

// HeapWeightFunc weights nodes inversely by heap memory (normalized to 1 GB).
func HeapWeightFunc(p actor.NodePressure) float64 {
	const maxHeap = 1 << 30 // 1 GB normalization threshold
	ratio := float64(p.HeapMemory) / float64(maxHeap)
	if ratio > 1.0 {
		ratio = 1.0
	}
	w := 1.0 - ratio
	if w < 0.05 {
		w = 0.05
	}
	return w
}

// ClusterMetricsRoutingLogic routes messages to the least-loaded cluster node
// based on MetricsGossip data.
type ClusterMetricsRoutingLogic struct {
	metrics  *MetricsGossip
	weightFn WeightFunc

	// fallback when no metrics are available
	fallback actor.RoundRobinRoutingLogic
}

// NewClusterMetricsRoutingLogic creates a routing logic that uses cluster
// metrics to select the least-loaded node.
func NewClusterMetricsRoutingLogic(metrics *MetricsGossip, weightFn ...WeightFunc) *ClusterMetricsRoutingLogic {
	fn := DefaultWeightFunc
	if len(weightFn) > 0 && weightFn[0] != nil {
		fn = weightFn[0]
	}
	return &ClusterMetricsRoutingLogic{
		metrics:  metrics,
		weightFn: fn,
	}
}

// Select chooses a routee weighted by inverse pressure.
func (l *ClusterMetricsRoutingLogic) Select(message any, routees []actor.Ref) actor.Ref {
	if len(routees) == 0 {
		return nil
	}
	if len(routees) == 1 {
		return routees[0]
	}

	pressure := l.metrics.ClusterPressure()
	if len(pressure) == 0 {
		return l.fallback.Select(message, routees)
	}

	weights := make([]float64, len(routees))
	totalWeight := 0.0

	for i, r := range routees {
		path, err := actor.ParseActorPath(r.Path())
		if err != nil {
			weights[i] = 0.5
		} else {
			nodeID := path.Address.Host + ":" + itoa(path.Address.Port)
			if p, ok := pressure[nodeID]; ok {
				weights[i] = l.weightFn(p)
			} else {
				weights[i] = 0.5 // unknown node gets default weight
			}
		}
		totalWeight += weights[i]
	}

	// Weighted random selection
	r := rand.Float64() * totalWeight
	current := 0.0
	for i, w := range weights {
		current += w
		if r <= current {
			return routees[i]
		}
	}
	return routees[len(routees)-1]
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 20)
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[pos:])
}
