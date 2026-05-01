/*
 * cluster_management.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"

	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/sopranoworks/gekka/cluster/sharding"
)

// ── management.ClusterStateProvider extensions ────────────────────────────────
//
// These methods satisfy the four Phase-13 additions to ClusterStateProvider
// that drive the /cluster/services, /cluster/config, and /cluster/sharding
// management endpoints.

// Services returns all ORSet-backed service registrations known to this node
// as a map of service name → address slice.
// Satisfies management.ClusterStateProvider; used by GET /cluster/services.
func (c *Cluster) Services() map[string][]string {
	return c.repl.AllSetsSnapshot()
}

// ConfigEntries returns the current cluster configuration from the
// "cluster-config" LWWMap as a map of config key → value.
// Satisfies management.ClusterStateProvider; used by GET /cluster/config.
func (c *Cluster) ConfigEntries() map[string]any {
	return c.repl.LWWMap("cluster-config").Entries()
}

// UpdateConfigEntry sets configKey to value in the "cluster-config" LWWMap
// and propagates it to peers during the next gossip round.
// Satisfies management.ClusterStateProvider; used by POST /cluster/config.
func (c *Cluster) UpdateConfigEntry(configKey string, value any) {
	c.repl.PutInMap("cluster-config", configKey, value, ddata.WriteLocal)
}

// ShardDistribution returns the shard→region allocation snapshot for the
// given entity type, sourced from the registered ShardCoordinator.
// Returns nil, false when no coordinator has been registered for typeName
// (i.e. StartSharding was not called on this node for that type, or this
// node is not currently the singleton coordinator).
// Satisfies management.ClusterStateProvider; used by GET /cluster/sharding/{typeName}.
func (c *Cluster) ShardDistribution(typeName string) (map[string]string, bool) {
	coord, ok := sharding.LookupCoordinator(typeName)
	if !ok {
		return nil, false
	}
	return coord.AllocationSnapshot(), true
}

// RebalanceShard sends a RebalanceShard control message to the coordinator for
// typeName, requesting that shardId be moved to targetRegion.
// Returns an error when no coordinator is registered for typeName.
// Satisfies management.ClusterStateProvider; used by POST /cluster/sharding/{typeName}/rebalance.
func (c *Cluster) RebalanceShard(typeName, shardId, targetRegion string) error {
	coord, ok := sharding.LookupCoordinator(typeName)
	if !ok {
		return fmt.Errorf("no coordinator registered for entity type %q", typeName)
	}
	coord.Self().Tell(sharding.RebalanceShard{
		ShardId:      shardId,
		TargetRegion: targetRegion,
	})
	return nil
}

// ShardingHealthCheckReady evaluates the configured sharding-type readiness
// probe driven by pekko.cluster.sharding.healthcheck.* and returns
// (true, "") when every named type has a registered coordinator on this
// node or when the configured Names list is empty (the Pekko default).
//
// On failure it returns (false, "sharding_not_ready: <reason>") which the
// management server's /health/ready handler surfaces as the readiness
// probe's reason field. The check delegates to
// sharding.ClusterShardingHealthCheck (cluster/sharding/healthcheck.go);
// the Names slice and Timeout duration come from
// c.cfg.Sharding.HealthCheck, which is parsed in hocon_config.go.
//
// Satisfies management.ClusterStateProvider; consumed by
// internal/management/server.go:readinessReason as the final readiness
// gate after the existing four (shutting_down, quarantined,
// unreachable_members, not_up). Empty Names short-circuits to ready=true
// without invoking the lookup loop.
func (c *Cluster) ShardingHealthCheckReady() (bool, string) {
	hc := c.cfg.Sharding.HealthCheck
	if len(hc.Names) == 0 {
		return true, ""
	}
	err := sharding.ClusterShardingHealthCheck(context.Background(), sharding.HealthCheckConfig{
		Names:   hc.Names,
		Timeout: hc.Timeout,
	})
	if err != nil {
		return false, "sharding_not_ready: " + err.Error()
	}
	return true, ""
}
