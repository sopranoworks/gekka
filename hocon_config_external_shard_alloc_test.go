/*
 * hocon_config_external_shard_alloc_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"
	"time"
)

// TestHOCON_ShardingConfig_ExternalShardAllocationClientTimeout verifies that
// pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout is
// parsed into NodeConfig.Sharding.ExternalShardAllocation.ClientTimeout.
func TestHOCON_ShardingConfig_ExternalShardAllocationClientTimeout(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko.remote.artery.canonical.hostname = "127.0.0.1"
pekko.remote.artery.canonical.port = 2552
pekko.cluster.seed-nodes = []
pekko.cluster.sharding.external-shard-allocation-strategy.client-timeout = 7500ms
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := 7500 * time.Millisecond
	if got := cfg.Sharding.ExternalShardAllocation.ClientTimeout; got != want {
		t.Errorf("ExternalShardAllocation.ClientTimeout = %v, want %v", got, want)
	}
}

// TestHOCON_ShardingConfig_ExternalShardAllocationClientTimeout_AkkaAlias
// verifies that the akka.* prefix is also honored.
func TestHOCON_ShardingConfig_ExternalShardAllocationClientTimeout_AkkaAlias(t *testing.T) {
	cfg, err := parseHOCONString(`
akka.remote.artery.canonical.hostname = "127.0.0.1"
akka.remote.artery.canonical.port = 2552
akka.cluster.seed-nodes = []
akka.cluster.sharding.external-shard-allocation-strategy.client-timeout = 12s
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	want := 12 * time.Second
	if got := cfg.Sharding.ExternalShardAllocation.ClientTimeout; got != want {
		t.Errorf("ExternalShardAllocation.ClientTimeout = %v, want %v", got, want)
	}
}
