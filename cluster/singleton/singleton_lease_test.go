/*
 * singleton_lease_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package singleton

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	icluster "github.com/sopranoworks/gekka/internal/cluster"
	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// TestSingletonManager_LeaseAcquiredBeforeSpawn verifies that the manager
// acquires the lease before spawning the singleton.
func TestSingletonManager_LeaseAcquiredBeforeSpawn(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}

	lease := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "singleton-test",
		OwnerName: "node-1",
	}, false)

	mgr := newWiredManager(cm, ctx)
	mgr.WithLease(lease)

	mgr.PreStart()

	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn, got %d", ctx.spawnCount())
	}
	if !lease.CheckLease() {
		t.Fatal("expected lease to be held after spawn")
	}
}

// TestSingletonManager_LeaseReleasedOnStop verifies that PostStop releases
// the lease.
func TestSingletonManager_LeaseReleasedOnStop(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}

	lease := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "singleton-test",
		OwnerName: "node-1",
	}, false)

	mgr := newWiredManager(cm, ctx)
	mgr.WithLease(lease)

	mgr.PreStart()
	if !lease.CheckLease() {
		t.Fatal("expected lease to be held after PreStart")
	}

	mgr.PostStop()
	if lease.CheckLease() {
		t.Fatal("expected lease to be released after PostStop")
	}
}

// TestSingletonManager_LeaseReleasedOnLeadershipLoss verifies that the lease
// is released when leadership transfers to another node.
func TestSingletonManager_LeaseReleasedOnLeadershipLoss(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2553, 1)
	ctx := &singletonTestContext{}

	lease := icluster.NewTestLease(icluster.LeaseSettings{
		LeaseName: "singleton-test",
		OwnerName: "node-2",
	}, false)

	mgr := newWiredManager(cm, ctx)
	mgr.WithLease(lease)

	// Local is only Up node → oldest → spawn
	mgr.PreStart()
	if !lease.CheckLease() {
		t.Fatal("expected lease held")
	}

	// Add an older node → local loses leadership
	older := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String("TestSystem"),
			Hostname: proto.String("127.0.0.1"),
			Port:     proto.Uint32(2552),
		},
		Uid: proto.Uint32(2),
	}
	cm.Mu.Lock()
	cm.State.AllAddresses = append(cm.State.AllAddresses, older)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(0),
	})
	cm.Mu.Unlock()

	mgr.Receive(cluster.MemberUp{Member: cluster.MemberAddress{Host: "127.0.0.1", Port: 2552}})

	if lease.CheckLease() {
		t.Fatal("expected lease released after leadership loss")
	}
}

// TestSingletonManager_LeaseFailureRetries verifies that the manager retries
// lease acquisition with a short delay.
func TestSingletonManager_LeaseFailureRetries(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}

	lease := &failOnceTestLease{
		inner:     icluster.NewTestLease(icluster.LeaseSettings{LeaseName: "singleton-test"}, false),
		failFirst: true,
	}

	mgr := newWiredManager(cm, ctx)
	mgr.lease = lease
	mgr.leaseRetryDelay = 10 * time.Millisecond

	mgr.PreStart()

	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn after retry, got %d", ctx.spawnCount())
	}
}

// TestSingletonManager_NoLeaseNoChange verifies behavior is unchanged without a lease.
func TestSingletonManager_NoLeaseNoChange(t *testing.T) {
	cm := newSingletonTestCM("127.0.0.1", 2552, 0)
	ctx := &singletonTestContext{}
	mgr := newWiredManager(cm, ctx)

	mgr.PreStart()
	if ctx.spawnCount() != 1 {
		t.Fatalf("expected 1 spawn, got %d", ctx.spawnCount())
	}
}

// ── failOnceTestLease ────────────────────────────────────────────────────────

// failOnceTestLease wraps a TestLease and fails the first Acquire call.
type failOnceTestLease struct {
	inner     *icluster.TestLease
	failFirst bool
	called    bool
}

func (l *failOnceTestLease) Acquire(ctx context.Context, onLost func(error)) (bool, error) {
	if l.failFirst && !l.called {
		l.called = true
		return false, nil
	}
	return l.inner.Acquire(ctx, onLost)
}

func (l *failOnceTestLease) Release(ctx context.Context) (bool, error) {
	return l.inner.Release(ctx)
}

func (l *failOnceTestLease) CheckLease() bool {
	return l.inner.CheckLease()
}

func (l *failOnceTestLease) Settings() icluster.LeaseSettings {
	return l.inner.Settings()
}
