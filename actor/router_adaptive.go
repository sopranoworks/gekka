/*
 * router_adaptive.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"math/rand"
	"sync"
)

// ClusterMetricsProvider provides cluster-wide pressure scores for adaptive routing.
type ClusterMetricsProvider interface {
	GetClusterPressure() map[string]float64
}

var (
	clusterMetricsProvider   ClusterMetricsProvider
	clusterMetricsProviderMu sync.RWMutex
)

// SetClusterMetricsProvider installs the provider used by the adaptive router.
func SetClusterMetricsProvider(p ClusterMetricsProvider) {
	clusterMetricsProviderMu.Lock()
	defer clusterMetricsProviderMu.Unlock()
	clusterMetricsProvider = p
}

// AdaptiveLoadBalancingRoutingLogic selects routees based on node pressure.
type AdaptiveLoadBalancingRoutingLogic struct {
	fallback RoundRobinRoutingLogic
}

// Select chooses a routee by weighting them inversely proportional to their node's pressure score.
func (l *AdaptiveLoadBalancingRoutingLogic) Select(message any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}

	clusterMetricsProviderMu.RLock()
	p := clusterMetricsProvider
	clusterMetricsProviderMu.RUnlock()

	if p == nil {
		return l.fallback.Select(message, routees)
	}

	pressureMap := p.GetClusterPressure()
	if len(pressureMap) == 0 {
		return l.fallback.Select(message, routees)
	}

	// Calculate weights: weight = 1.0 - pressure (min 0.05 to ensure some chance)
	weights := make([]float64, len(routees))
	totalWeight := 0.0

	for i, r := range routees {
		path, err := ParseActorPath(r.Path())
		if err != nil {
			weights[i] = 0.5
		} else {
			nodeID := path.Address.Host + ":" + string(rune(path.Address.Port)) // Simplified node key
			// Better node key extraction needed if we want to match MetricsGossip nodeID
			// In Cluster, nodeID was host:port.
			nodeID = path.Address.Host + ":" + itoa_val(path.Address.Port)
			
			pressure, ok := pressureMap[nodeID]
			if !ok {
				pressure = 0.5 // Unknown node, assume medium pressure
			}
			weight := 1.0 - pressure
			if weight < 0.05 {
				weight = 0.05
			}
			weights[i] = weight
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

func itoa_val(v int) string {
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
