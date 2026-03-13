/*
 * cluster_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"context"
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

func TestVectorClockComparison(t *testing.T) {
	v1 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(1)},
		},
	}
	v2 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(2)},
		},
	}
	v3 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(2), Timestamp: proto.Int64(1)},
		},
	}
	v4 := &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(1)},
			{HashIndex: proto.Int32(2), Timestamp: proto.Int64(1)},
		},
	}

	if CompareVectorClock(v1, v1) != ClockSame {
		t.Errorf("expected same")
	}
	if CompareVectorClock(v1, v2) != ClockBefore {
		t.Errorf("expected v1 before v2")
	}
	if CompareVectorClock(v2, v1) != ClockAfter {
		t.Errorf("expected v2 after v1")
	}
	if CompareVectorClock(v1, v3) != ClockConcurrent {
		t.Errorf("expected concurrent")
	}
	if CompareVectorClock(v1, v4) != ClockBefore {
		t.Errorf("expected v1 before v4")
	}
}

func TestCheckConvergence(t *testing.T) {
	local := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint32(123),
		Uid2:    proto.Uint32(0),
	}
	// No-op router
	router := func(ctx context.Context, path string, msg any) error { return nil }
	cm := NewClusterManager(local, router)
	cm.State.Overview = &gproto_cluster.GossipOverview{
		Seen: []int32{0},
	}

	// Default state has only self. Convergence should be true.
	if !cm.CheckConvergence() {
		t.Errorf("expected convergence with single node")
	}

	// Add another member but not in seen set
	addr2 := &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2553)},
		Uid:     proto.Uint32(456),
		Uid2:    proto.Uint32(0),
	}
	cm.State.AllAddresses = append(cm.State.AllAddresses, addr2)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
	})

	if cm.CheckConvergence() {
		t.Errorf("expected non-convergence, remote node hasn't seen state")
	}

	// Add remote node to seen set
	cm.State.Overview.Seen = append(cm.State.Overview.Seen, 1)

	if !cm.CheckConvergence() {
		t.Errorf("expected convergence, all nodes have seen state")
	}
}
