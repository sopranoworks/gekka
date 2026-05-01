/*
 * sharding_healthcheck_endpoint_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/cluster/sharding"
)

// Sub-plan 8d: end-to-end verification that the two sharding healthcheck
// paths parsed from HOCON
// (`pekko.cluster.sharding.healthcheck.{names, timeout}`) are consumed by
// the new production endpoint Cluster.ShardingHealthCheckReady, which
// the management server's /health/ready handler consults as a readiness
// gate.
//
// The test exercises the runtime path end-to-end: HOCON → cfg.Sharding.
// HealthCheck → ShardingHealthCheckReady → sharding.ClusterShardingHealthCheck
// → LookupCoordinator. Before any coordinator is registered for the
// configured Names list, ShardingHealthCheckReady must return ready=false
// with the underlying lookup error embedded in the reason. After
// RegisterCoordinator runs for that name, the same call must return
// ready=true.
func TestSharding_ShardingHealthCheckReady_ConsultsConfiguredNames(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster {
    seed-nodes = []
    sharding {
      healthcheck {
        names   = ["typeA"]
        timeout = 1s
      }
    }
  }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestSystem"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer node.Shutdown()

	// Sanity: parsed values reach cfg.Sharding.HealthCheck. This is a
	// pre-condition for the runtime check below — without it the test
	// would degenerate into the empty-Names short-circuit.
	if got, want := len(node.cfg.Sharding.HealthCheck.Names), 1; got != want {
		t.Fatalf("cfg.Sharding.HealthCheck.Names len = %d, want %d", got, want)
	}
	if got := node.cfg.Sharding.HealthCheck.Names[0]; got != "typeA" {
		t.Fatalf("cfg.Sharding.HealthCheck.Names[0] = %q, want %q", got, "typeA")
	}
	if got := node.cfg.Sharding.HealthCheck.Timeout; got <= 0 {
		t.Fatalf("cfg.Sharding.HealthCheck.Timeout = %v, want > 0", got)
	}

	// Coordinator for "typeA" is not registered yet → readiness probe
	// must surface the lookup error from sharding.ClusterShardingHealthCheck.
	ready, reason := node.ShardingHealthCheckReady()
	if ready {
		t.Fatalf("expected ready=false before coordinator registration, got ready=true (reason=%q)", reason)
	}
	if !strings.HasPrefix(reason, "sharding_not_ready: ") {
		t.Errorf("reason = %q, want prefix %q", reason, "sharding_not_ready: ")
	}
	if !strings.Contains(reason, "typeA") {
		t.Errorf("reason = %q, want it to surface the missing type name 'typeA'", reason)
	}

	// Register a coordinator for "typeA"; clean up via t.Cleanup since
	// the registry is process-global.
	sharding.RegisterCoordinator("typeA", &sharding.ShardCoordinator{})
	t.Cleanup(func() { sharding.UnregisterCoordinator("typeA") })

	ready, reason = node.ShardingHealthCheckReady()
	if !ready {
		t.Fatalf("expected ready=true after coordinator registration, got ready=false (reason=%q)", reason)
	}
	if reason != "" {
		t.Errorf("reason = %q, want empty string when ready", reason)
	}
}

// TestSharding_ShardingHealthCheckReady_EmptyNamesShortCircuits guards the
// empty-Names short-circuit: when no sharding healthcheck names are
// configured (the Pekko default), ShardingHealthCheckReady must return
// ready=true immediately without invoking the lookup path.
func TestSharding_ShardingHealthCheckReady_EmptyNamesShortCircuits(t *testing.T) {
	cfg, err := parseHOCONString(`
pekko {
  remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
  cluster { seed-nodes = [] }
}
`)
	if err != nil {
		t.Fatalf("parseHOCONString: %v", err)
	}
	cfg.SystemName = "TestSystem"

	node, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer node.Shutdown()

	if len(node.cfg.Sharding.HealthCheck.Names) != 0 {
		t.Fatalf("expected empty Names by default, got %v", node.cfg.Sharding.HealthCheck.Names)
	}

	ready, reason := node.ShardingHealthCheckReady()
	if !ready {
		t.Fatalf("expected ready=true with empty Names, got ready=false (reason=%q)", reason)
	}
	if reason != "" {
		t.Errorf("reason = %q, want empty string", reason)
	}
}
