/*
 * cluster_manager_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"gekka/gekka/cluster"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestVectorClockComparison(t *testing.T) {
	v1 := &cluster.VectorClock{
		Versions: []*cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(1)},
		},
	}
	v2 := &cluster.VectorClock{
		Versions: []*cluster.VectorClock_Version{
			{HashIndex: proto.Int32(1), Timestamp: proto.Int64(2)},
		},
	}
	v3 := &cluster.VectorClock{
		Versions: []*cluster.VectorClock_Version{
			{HashIndex: proto.Int32(2), Timestamp: proto.Int64(1)},
		},
	}
	v4 := &cluster.VectorClock{
		Versions: []*cluster.VectorClock_Version{
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
	local := &UniqueAddress{
		Address: &Address{System: proto.String("sys"), Hostname: proto.String("localhost"), Port: proto.Uint32(2552)},
		Uid:     proto.Uint64(123),
	}
	nm := NewNodeManager(local.Address, 0)
	router := NewRouter(nm)
	cm := NewClusterManager(local, router)

	// Default state has only self. Convergence should be true.
	if !cm.CheckConvergence() {
		t.Errorf("expected convergence with single node")
	}

	// Add another member but not in seen set
	addr2 := &cluster.UniqueAddress{
		Address: &cluster.Address{System: proto.String("sys"), Hostname: proto.String("remote"), Port: proto.Uint32(2553)},
		Uid:     proto.Uint32(456),
	}
	cm.state.AllAddresses = append(cm.state.AllAddresses, addr2)
	cm.state.Members = append(cm.state.Members, &cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       cluster.MemberStatus_Up.Enum(),
	})

	if cm.CheckConvergence() {
		t.Errorf("expected non-convergence, remote node hasn't seen state")
	}

	// Add remote node to seen set
	cm.state.Overview.Seen = append(cm.state.Overview.Seen, 1)

	if !cm.CheckConvergence() {
		t.Errorf("expected convergence, all nodes have seen state")
	}
}
