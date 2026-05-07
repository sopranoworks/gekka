/*
 * shard_start_timeout_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Tests for Phase 7.1 — pekko.cluster.sharding.shard-start-timeout enforced
// during Shard.PreStart. When the lease cannot be acquired (or any other
// startup step blocks) before the timeout elapses, the shard emits a logged
// warning, marks itself failed, and refuses to deliver subsequent envelopes.

package sharding

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// alwaysFailingLease is a Lease implementation whose Acquire never returns
// true. It is used to keep Shard.PreStart blocked on lease retries so the
// shard-start-timeout deadline is the only thing that releases it.
type alwaysFailingLease struct {
	calls atomic.Int32
}

func (l *alwaysFailingLease) Acquire(_ context.Context, _ func(error)) (bool, error) {
	l.calls.Add(1)
	return false, nil
}

func (l *alwaysFailingLease) Release(_ context.Context) (bool, error) { return true, nil }

func (l *alwaysFailingLease) CheckLease() bool { return false }

func (l *alwaysFailingLease) Settings() icluster.LeaseSettings {
	return icluster.LeaseSettings{LeaseName: "always-fail", OwnerName: "node-1"}
}

// TestShard_StartTimeoutCapsLeaseLoop verifies that PreStart returns within
// ShardStartTimeout even when the configured lease never acquires, instead
// of looping forever as the legacy code did.
func TestShard_StartTimeoutCapsLeaseLoop(t *testing.T) {
	settings := ShardSettings{
		Lease:             &alwaysFailingLease{},
		LeaseRetryDelay:   5 * time.Millisecond,
		ShardStartTimeout: 80 * time.Millisecond,
	}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		shard.PreStart()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed > 500*time.Millisecond {
			t.Fatalf("PreStart took too long: %v (expected ≤500ms; timeout=%v)",
				elapsed, settings.ShardStartTimeout)
		}
		if elapsed < settings.ShardStartTimeout/2 {
			t.Fatalf("PreStart returned too quickly: %v (timeout=%v) — likely did not respect retries",
				elapsed, settings.ShardStartTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PreStart did not return within 2s — shard-start-timeout is not enforced")
	}

	if !shard.startupFailed {
		t.Fatal("expected shard.startupFailed = true after timeout")
	}
	if shard.leaseHeld {
		t.Fatal("expected shard.leaseHeld = false when lease never succeeded")
	}
}

// TestShard_StartTimeoutDefault verifies that an unset ShardStartTimeout
// falls back to the Pekko-equivalent 10s default. The test does not wait
// the full default; it just checks the resolved value.
func TestShard_StartTimeoutDefault(t *testing.T) {
	settings := ShardSettings{}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)
	if got, want := shard.resolveShardStartTimeout(), 10*time.Second; got != want {
		t.Errorf("resolveShardStartTimeout default = %v, want %v", got, want)
	}
}

// TestShard_StartTimeoutOverride verifies a non-zero ShardStartTimeout is
// returned verbatim by the resolver — no defaulting kicks in.
func TestShard_StartTimeoutOverride(t *testing.T) {
	settings := ShardSettings{ShardStartTimeout: 7 * time.Second}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)
	if got, want := shard.resolveShardStartTimeout(), 7*time.Second; got != want {
		t.Errorf("resolveShardStartTimeout override = %v, want %v", got, want)
	}
}

// TestShard_StartTimeoutNoLeaseDoesNotFail verifies that with no lease,
// PreStart still completes successfully and startupFailed stays false even
// if ShardStartTimeout is small.
func TestShard_StartTimeoutNoLeaseDoesNotFail(t *testing.T) {
	settings := ShardSettings{ShardStartTimeout: 50 * time.Millisecond}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)
	shard.PreStart()
	if shard.startupFailed {
		t.Fatal("expected startupFailed=false when no lease is configured")
	}
}

// TestShard_StartTimeoutDropsEnvelopesAfterFailure verifies that once the
// timeout has fired and startupFailed is set, incoming ShardingEnvelope
// messages are dropped (no entity is spawned, no panic).
func TestShard_StartTimeoutDropsEnvelopesAfterFailure(t *testing.T) {
	settings := ShardSettings{
		Lease:             &alwaysFailingLease{},
		LeaseRetryDelay:   5 * time.Millisecond,
		ShardStartTimeout: 30 * time.Millisecond,
	}
	shard, mctx := newTestShard(t, "TestType", "shard-0", settings)
	shard.PreStart()
	if !shard.startupFailed {
		t.Fatal("preflight: expected startupFailed=true after timeout")
	}

	sendEnvelope(shard, "e1", map[string]string{"k": "v"})
	if got := len(mctx.created); got != 0 {
		t.Errorf("startupFailed shard created %d entities; want 0 (created=%v)",
			got, mctx.created)
	}
}
