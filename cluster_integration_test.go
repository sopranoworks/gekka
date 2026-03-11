/*
 * cluster_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/cluster"
	"google.golang.org/protobuf/proto"
)

func TestCluster_JoinHandshake(t *testing.T) {
	// 1. Setup Seed Node (Server)
	seedAddr := &Address{
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2554),
		System:   proto.String("clusterSystem"),
		Protocol: proto.String("pekko"),
	}
	seedUA := &UniqueAddress{Address: seedAddr, Uid: proto.Uint64(1)}
	seedNM := NewNodeManager(seedAddr, 1)
	seedRouter := NewRouter(seedNM)
	seedCM := cluster.NewClusterManager(toClusterUniqueAddress(seedUA), func(ctx context.Context, path string, msg any) error {
		return seedRouter.Send(ctx, path, msg)
	})
	seedNM.SetClusterManager(seedCM)

	ln, err := net.Listen("tcp", "127.0.0.1:2554")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = seedNM.ProcessConnection(ctx, conn, INBOUND, nil, 0) }()
		}
	}()

	// 2. Setup Joining Node (Client)
	joinAddr := &Address{
		Hostname: proto.String("127.0.0.1"),
		Port:     proto.Uint32(2555),
		System:   proto.String("clusterSystem"),
		Protocol: proto.String("pekko"),
	}
	joinUA := &UniqueAddress{Address: joinAddr, Uid: proto.Uint64(2)}
	joinNM := NewNodeManager(joinAddr, 2)
	joinRouter := NewRouter(joinNM)
	joinCM := cluster.NewClusterManager(toClusterUniqueAddress(joinUA), func(ctx context.Context, path string, msg any) error {
		return joinRouter.Send(ctx, path, msg)
	})
	joinNM.SetClusterManager(joinCM)

	// 3. Perform Join
	if err := joinCM.JoinCluster(ctx, "127.0.0.1", 2554); err != nil {
		t.Fatalf("JoinCluster failed: %v", err)
	}

	// 4. Verify Welcome (Wait for state update)
	deadline := time.Now().Add(5 * time.Second)
	success := false
	for time.Now().Before(deadline) {
		joinCM.Mu.RLock()
		if len(joinCM.State.AllAddresses) > 0 {
			success = true
			joinCM.Mu.RUnlock()
			break
		}
		joinCM.Mu.RUnlock()
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatal("timed out waiting for Welcome response")
	}
}

func TestCluster_GossipConvergence(t *testing.T) {
	// Node 1 (Seed)
	addr1 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2560), System: proto.String("sys"), Protocol: proto.String("pekko")}
	ua1 := &UniqueAddress{Address: addr1, Uid: proto.Uint64(111)}
	nm1 := NewNodeManager(addr1, 111)
	router1 := NewRouter(nm1)
	cm1 := cluster.NewClusterManager(toClusterUniqueAddress(ua1), func(ctx context.Context, path string, msg any) error {
		return router1.Send(ctx, path, msg)
	})
	nm1.SetClusterManager(cm1)

	ln1, _ := net.Listen("tcp", "127.0.0.1:2560")
	defer ln1.Close()
	go func() {
		for {
			conn, err := ln1.Accept()
			if err != nil {
				return
			}
			go func() { _ = nm1.ProcessConnection(context.Background(), conn, INBOUND, nil, 0) }()
		}
	}()

	// Node 2 (Joining)
	addr2 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2561), System: proto.String("sys"), Protocol: proto.String("pekko")}
	ua2 := &UniqueAddress{Address: addr2, Uid: proto.Uint64(222)}
	nm2 := NewNodeManager(addr2, 222)
	router2 := NewRouter(nm2)
	cm2 := cluster.NewClusterManager(toClusterUniqueAddress(ua2), func(ctx context.Context, path string, msg any) error {
		return router2.Send(ctx, path, msg)
	})
	nm2.SetClusterManager(cm2)

	ln2, _ := net.Listen("tcp", "127.0.0.1:2561")
	defer ln2.Close()
	go func() {
		for {
			conn, err := ln2.Accept()
			if err != nil {
				return
			}
			go func() { _ = nm2.ProcessConnection(context.Background(), conn, INBOUND, nil, 0) }()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Perform Handshake
	if err := cm2.JoinCluster(ctx, "127.0.0.1", 2560); err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	// Manually add Node 2 to Node 1's Gossip state to simulate convergence progress
	cm1.Mu.Lock()
	cm1.State.AllAddresses = append(cm1.State.AllAddresses, toClusterUniqueAddress(ua2))
	cm1.State.Members = append(cm1.State.Members, &cluster.Member{
		AddressIndex: proto.Int32(int32(len(cm1.State.AllAddresses) - 1)),
		Status:       cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(0),
	})
	cm1.Mu.Unlock()

	// Verify Node 2 eventually sees Node 1's update
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cm2.Mu.RLock()
		if len(cm2.State.Members) >= 2 {
			cm2.Mu.RUnlock()
			return // Success
		}
		cm2.Mu.RUnlock()
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatal("Gossip state failed to converge")
}

func TestCluster_LeaderElection(t *testing.T) {
	setup := func(port uint32, uid uint64) (*cluster.ClusterManager, *NodeManager, net.Listener) {
		addr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(port), System: proto.String("leaderSys"), Protocol: proto.String("pekko")}
		ua := &UniqueAddress{Address: addr, Uid: proto.Uint64(uid)}
		nm := NewNodeManager(addr, uid)
		router := NewRouter(nm)
		cm := cluster.NewClusterManager(toClusterUniqueAddress(ua), func(ctx context.Context, path string, msg any) error {
			return router.Send(ctx, path, msg)
		})
		nm.SetClusterManager(cm)
		ln, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { _ = nm.ProcessConnection(context.Background(), conn, INBOUND, nil, 0) }()
			}
		}()
		return cm, nm, ln
	}

	cm1, _, ln1 := setup(2570, 100)
	defer ln1.Close()
	cm2, _, ln2 := setup(2571, 200)
	defer ln2.Close()
	cm3, _, ln3 := setup(2572, 50)
	defer ln3.Close()

	// Initial leader should be itself
	l1 := cm1.DetermineLeader()
	if l1.GetAddress().GetPort() != 2570 {
		t.Errorf("expected node 1 as leader initially, got port %d", l1.GetAddress().GetPort())
	}

	// Connect nodes together by manual state update
	allUA := []*cluster.UniqueAddress{
		toClusterUniqueAddress(&UniqueAddress{Address: &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2570), System: proto.String("leaderSys"), Protocol: proto.String("pekko")}, Uid: proto.Uint64(100)}),
		toClusterUniqueAddress(&UniqueAddress{Address: &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2571), System: proto.String("leaderSys"), Protocol: proto.String("pekko")}, Uid: proto.Uint64(200)}),
		toClusterUniqueAddress(&UniqueAddress{Address: &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2572), System: proto.String("leaderSys"), Protocol: proto.String("pekko")}, Uid: proto.Uint64(50)}),
	}
	members := []*cluster.Member{
		{AddressIndex: proto.Int32(0), Status: cluster.MemberStatus_Up.Enum()},
		{AddressIndex: proto.Int32(1), Status: cluster.MemberStatus_Up.Enum()},
		{AddressIndex: proto.Int32(2), Status: cluster.MemberStatus_Up.Enum()},
	}

	for _, cm := range []*cluster.ClusterManager{cm1, cm2, cm3} {
		cm.Mu.Lock()
		cm.State.AllAddresses = allUA
		cm.State.Members = members
		cm.Mu.Unlock()
	}

	expectPort := uint32(2570)
	if cm1.DetermineLeader().GetAddress().GetPort() != expectPort ||
		cm2.DetermineLeader().GetAddress().GetPort() != expectPort ||
		cm3.DetermineLeader().GetAddress().GetPort() != expectPort {
		t.Fatal("nodes disagreed on leader or selected wrong one")
	}
}
