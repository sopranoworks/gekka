/*
 * config_registry.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// ConfigRegistry provides an eventually-consistent, cluster-wide key/value
// store backed by the Replicator's LWWMap CRDT.
//
// Writes follow Last-Write-Wins semantics: the entry with the highest
// timestamp survives a merge.  Because wall-clock skew between nodes can
// cause rare ordering anomalies, ConfigRegistry is intended for slowly-changing
// configuration (log levels, feature flags, thresholds) rather than
// transactional state.
//
// Gossip is handled entirely by the underlying Replicator.  Call
// Replicator.Start to enable periodic background sync, or trigger a one-shot
// sync with Replicator.GossipAll for tests.
//
// Usage:
//
//	r   := ddata.NewReplicator("node-1", router)
//	cfg := ddata.NewConfigRegistry(r, "cluster-config")
//	cfg.UpdateConfig("log-level", "DEBUG")
//	val, ok := cfg.GetConfig("log-level")
type ConfigRegistry struct {
	r      *Replicator
	mapKey string // LWWMap key used inside the Replicator
}

// NewConfigRegistry returns a ConfigRegistry whose settings are stored in the
// Replicator's LWWMap at mapKey.  All nodes in the cluster must use the same
// mapKey to exchange settings via gossip.
func NewConfigRegistry(r *Replicator, mapKey string) *ConfigRegistry {
	return &ConfigRegistry{r: r, mapKey: mapKey}
}

// MapKey returns the LWWMap key used inside the Replicator.
// Useful when constructing gossip messages in tests.
func (c *ConfigRegistry) MapKey() string { return c.mapKey }

// UpdateConfig sets configKey to value on the local node.
// The change will propagate to other nodes during the next gossip round.
func (c *ConfigRegistry) UpdateConfig(configKey string, value any) {
	c.r.PutInMap(c.mapKey, configKey, value, WriteLocal)
}

// GetConfig retrieves the current value for configKey.
// Returns (nil, false) if the key has never been set on this node.
// After a gossip merge the value reflects the most recently written entry
// across all nodes.
func (c *ConfigRegistry) GetConfig(configKey string) (any, bool) {
	return c.r.LWWMap(c.mapKey).Get(configKey)
}

// Keys returns all configuration keys known to this node.
func (c *ConfigRegistry) Keys() []string {
	entries := c.r.LWWMap(c.mapKey).Entries()
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	return keys
}
