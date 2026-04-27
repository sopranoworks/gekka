/*
 * shard_lease_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"testing"
	"time"

	icluster "github.com/sopranoworks/gekka/internal/cluster"
)

// TestShard_LeaseAcquiredOnPreStart verifies that PreStart acquires the
// configured lease before the Shard becomes active (Round-2 session 20).
func TestShard_LeaseAcquiredOnPreStart(t *testing.T) {
	lease := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "shard-test",
		OwnerName: "node-1",
	}, false)

	settings := ShardSettings{Lease: lease}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	shard.PreStart()

	if !lease.CheckLease() {
		t.Fatal("expected lease to be held after PreStart")
	}
	if !shard.leaseHeld {
		t.Fatal("expected shard.leaseHeld == true after PreStart")
	}
}

// TestShard_LeaseReleasedOnPostStop verifies that PostStop releases the
// previously acquired lease so a peer Shard can take over (Round-2 session 20).
func TestShard_LeaseReleasedOnPostStop(t *testing.T) {
	lease := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "shard-test",
		OwnerName: "node-1",
	}, false)

	settings := ShardSettings{Lease: lease}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	shard.PreStart()
	if !lease.CheckLease() {
		t.Fatal("expected lease held after PreStart")
	}

	shard.PostStop()

	if lease.CheckLease() {
		t.Fatal("expected lease released after PostStop")
	}
	if shard.leaseHeld {
		t.Fatal("expected shard.leaseHeld == false after PostStop")
	}
}

// TestShard_LeaseRetriesOnFailure verifies that PreStart retries lease
// acquisition with the configured backoff when an Acquire returns false
// (Round-2 session 20).
func TestShard_LeaseRetriesOnFailure(t *testing.T) {
	inner := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "shard-test",
		OwnerName: "node-1",
	}, false)
	lease := &shardFailOnceLease{inner: inner}

	settings := ShardSettings{
		Lease:           lease,
		LeaseRetryDelay: 5 * time.Millisecond,
	}
	shard, _ := newTestShard(t, "TestType", "shard-0", settings)

	shard.PreStart()

	if !inner.CheckLease() {
		t.Fatal("expected lease held after retry")
	}
	if !shard.leaseHeld {
		t.Fatal("expected shard.leaseHeld == true after retry")
	}
}

// TestShard_NoLeaseSkipsAcquire verifies that PreStart and PostStop are no-ops
// for the lease lifecycle when no lease is configured (Round-2 session 20).
func TestShard_NoLeaseSkipsAcquire(t *testing.T) {
	shard, _ := newTestShard(t, "TestType", "shard-0", ShardSettings{})

	shard.PreStart()
	if shard.leaseHeld {
		t.Fatal("expected shard.leaseHeld == false with no lease")
	}

	shard.PostStop() // must not panic
}

// ── shardFailOnceLease ──────────────────────────────────────────────────────

// shardFailOnceLease wraps a TestLease and fails the first Acquire call so
// the Shard's retry loop can be exercised.
type shardFailOnceLease struct {
	inner  *icluster.TestLease
	called bool
}

func (l *shardFailOnceLease) Acquire(ctx context.Context, onLost func(error)) (bool, error) {
	if !l.called {
		l.called = true
		return false, nil
	}
	return l.inner.Acquire(ctx, onLost)
}

func (l *shardFailOnceLease) Release(ctx context.Context) (bool, error) {
	return l.inner.Release(ctx)
}

func (l *shardFailOnceLease) CheckLease() bool {
	return l.inner.CheckLease()
}

func (l *shardFailOnceLease) Settings() icluster.LeaseSettings {
	return l.inner.Settings()
}
