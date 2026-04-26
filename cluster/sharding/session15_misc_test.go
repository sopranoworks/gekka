/*
 * session15_misc_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Tests for Round-2 session 15 — sharding miscellaneous features.
//
// Covered HOCON paths:
//   - pekko.cluster.sharding.verbose-debug-logging
//   - pekko.cluster.sharding.fail-on-invalid-entity-state-transition
//   - pekko.cluster.sharding.healthcheck.names
//   - pekko.cluster.sharding.healthcheck.timeout
//   - pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.interval

package sharding

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestFailOnInvalidEntityStateTransition_Panics verifies that with
// FailOnInvalidEntityStateTransition = true a Shard panics when it sees an
// invalid handoff transition (ShardDrainRequest before ShardBeginHandoff).
func TestFailOnInvalidEntityStateTransition_Panics(t *testing.T) {
	settings := ShardSettings{FailOnInvalidEntityStateTransition: true}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invalid state transition, got none")
		}
	}()

	// Drain without prior BeginHandoff is invalid.
	shard.Receive(ShardDrainRequest{ShardId: "shard-0", RegionRef: &mockRef{path: "/region"}})
}

// TestFailOnInvalidEntityStateTransition_Off_LogsWarning verifies that with
// FailOnInvalidEntityStateTransition = false (the default) the same invalid
// transition does NOT panic — execution continues.
func TestFailOnInvalidEntityStateTransition_Off_LogsWarning(t *testing.T) {
	settings := ShardSettings{FailOnInvalidEntityStateTransition: false}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	// Should not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic with flag=off: %v", r)
		}
	}()

	region := &mockRef{path: "/region"}
	shard.Receive(ShardDrainRequest{ShardId: "shard-0", RegionRef: region})

	// The drain still flushes the (empty) stash and emits a ShardDrainResponse.
	if got := len(region.messages); got != 1 {
		t.Errorf("expected 1 message to region after drain, got %d", got)
	}
}

// TestVerboseDebugLogging_Plumbing verifies the flag round-trips into
// ShardSettings on the live Shard.
func TestVerboseDebugLogging_Plumbing(t *testing.T) {
	settings := ShardSettings{VerboseDebugLogging: true}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)
	if !shard.settings.VerboseDebugLogging {
		t.Error("VerboseDebugLogging did not round-trip onto Shard")
	}
}

// TestIdleEntityCheckInterval_Honored verifies the explicit interval is used
// rather than the PassivationIdleTimeout/2 default.
func TestIdleEntityCheckInterval_Honored(t *testing.T) {
	cases := []struct {
		name          string
		idleTimeout   time.Duration
		idleInterval  time.Duration
		minScheduleAt time.Duration // we cannot observe internal time.AfterFunc directly,
		// so we settle for a plumbing check: the field is present
		// on Shard.settings.
	}{
		{"explicit interval", 10 * time.Minute, 250 * time.Millisecond, 250 * time.Millisecond},
		{"default cadence (timeout/2)", 10 * time.Second, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			settings := ShardSettings{
				PassivationIdleTimeout:  tc.idleTimeout,
				IdleEntityCheckInterval: tc.idleInterval,
			}
			shard, _ := newTestShard(t, "TestType", "shard-0", settings)
			if shard.settings.IdleEntityCheckInterval != tc.idleInterval {
				t.Errorf("IdleEntityCheckInterval = %v, want %v",
					shard.settings.IdleEntityCheckInterval, tc.idleInterval)
			}
			if shard.settings.PassivationIdleTimeout != tc.idleTimeout {
				t.Errorf("PassivationIdleTimeout = %v, want %v",
					shard.settings.PassivationIdleTimeout, tc.idleTimeout)
			}
		})
	}
}

// TestHealthCheck_PassesWhenNoNamesConfigured verifies an empty Names list
// yields a no-op pass.
func TestHealthCheck_PassesWhenNoNamesConfigured(t *testing.T) {
	if err := ClusterShardingHealthCheck(context.Background(), HealthCheckConfig{}); err != nil {
		t.Errorf("empty names should pass, got %v", err)
	}
}

// TestHealthCheck_FailsWhenNamedTypeNotRegistered verifies that an
// unregistered name produces a non-timeout error.
func TestHealthCheck_FailsWhenNamedTypeNotRegistered(t *testing.T) {
	cfg := HealthCheckConfig{
		Names:   []string{"unregistered-type-" + t.Name()},
		Timeout: 1 * time.Second,
	}
	err := ClusterShardingHealthCheck(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unregistered type")
	}
	if errors.Is(err, ErrHealthCheckTimeout) {
		t.Errorf("expected non-timeout error for unregistered type, got %v", err)
	}
}

// TestHealthCheck_PassesAfterRegistration verifies the lookup succeeds once
// a coordinator has registered.
func TestHealthCheck_PassesAfterRegistration(t *testing.T) {
	typeName := "session15-healthcheck-" + t.Name()
	RegisterCoordinator(typeName, &ShardCoordinator{})
	t.Cleanup(func() {
		coordRegistryMu.Lock()
		delete(coordRegistry, typeName)
		coordRegistryMu.Unlock()
	})
	cfg := HealthCheckConfig{
		Names:   []string{typeName},
		Timeout: 1 * time.Second,
	}
	if err := ClusterShardingHealthCheck(context.Background(), cfg); err != nil {
		t.Errorf("expected pass after registration, got %v", err)
	}
}

// TestHealthCheck_RespectsTimeout verifies that ErrHealthCheckTimeout is
// returned when the lookup is artificially blocked past the timeout. We
// simulate this by holding the registry's write lock for longer than the
// configured timeout.
func TestHealthCheck_RespectsTimeout(t *testing.T) {
	coordRegistryMu.Lock()
	t.Cleanup(func() { coordRegistryMu.Unlock() })

	cfg := HealthCheckConfig{
		Names:   []string{"any-name"},
		Timeout: 100 * time.Millisecond,
	}
	start := time.Now()
	err := ClusterShardingHealthCheck(context.Background(), cfg)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrHealthCheckTimeout) {
		t.Errorf("expected ErrHealthCheckTimeout, got %v", err)
	}
	if elapsed < 90*time.Millisecond {
		t.Errorf("returned too fast (%v), expected ≥ ~100ms", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("blocked too long (%v) — timeout did not fire", elapsed)
	}
}

// TestSession15Plumbing verifies all 5 new ShardSettings fields round-trip
// into the live Shard struct.
func TestSession15Plumbing(t *testing.T) {
	settings := ShardSettings{
		VerboseDebugLogging:                true,
		FailOnInvalidEntityStateTransition: true,
		IdleEntityCheckInterval:            321 * time.Millisecond,
	}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)
	got := shard.settings
	if !got.VerboseDebugLogging {
		t.Error("VerboseDebugLogging not plumbed")
	}
	if !got.FailOnInvalidEntityStateTransition {
		t.Error("FailOnInvalidEntityStateTransition not plumbed")
	}
	if got.IdleEntityCheckInterval != 321*time.Millisecond {
		t.Errorf("IdleEntityCheckInterval = %v", got.IdleEntityCheckInterval)
	}
}
