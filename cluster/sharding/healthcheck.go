/*
 * healthcheck.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// HealthCheckConfig captures the inputs to ClusterShardingHealthCheck.
//
// Mirrors Pekko's pekko.cluster.sharding.healthcheck block:
//   - Names      → pekko.cluster.sharding.healthcheck.names
//   - Timeout    → pekko.cluster.sharding.healthcheck.timeout
type HealthCheckConfig struct {
	// Names lists the sharding type names whose ShardCoordinator must
	// have been registered locally for the health check to pass. Empty
	// means "no sharding instances are monitored" — the check passes.
	Names []string

	// Timeout caps how long the health check is allowed to run. Defaults
	// to 5 s when zero.
	Timeout time.Duration
}

// ErrHealthCheckTimeout is returned when ClusterShardingHealthCheck does not
// finish within the configured timeout.
var ErrHealthCheckTimeout = errors.New("sharding healthcheck: timed out")

// ClusterShardingHealthCheck verifies that every sharding type listed in
// cfg.Names has a registered coordinator. It returns nil when the check
// passes (or when the names list is empty), an error describing the missing
// type when at least one is unregistered, and ErrHealthCheckTimeout when
// the deadline expires before the lookup completes.
//
// Pekko semantic: once an entity type has been registered locally, the
// health check stays positive for that type for the lifetime of the
// process — it never goes back to false even if a coordinator
// re-elects to a different node.
func ClusterShardingHealthCheck(ctx context.Context, cfg HealthCheckConfig) error {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() {
		for _, name := range cfg.Names {
			if _, ok := LookupCoordinator(name); !ok {
				resultCh <- fmt.Errorf("sharding healthcheck: type %q has no registered coordinator", name)
				return
			}
		}
		resultCh <- nil
	}()

	select {
	case err := <-resultCh:
		return err
	case <-deadline.Done():
		return ErrHealthCheckTimeout
	}
}
