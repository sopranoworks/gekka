/*
 * cluster_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"gekka/gekka/cluster"
	"log"
	"net"
	"testing"
	"time"

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
	seedCM := NewClusterManager(seedUA, seedRouter)
	seedNM.SetClusterManager(seedCM)

	ln, _ := net.Listen("tcp", "127.0.0.1:2554")
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go seedNM.ProcessConnection(ctx, conn, INBOUND, nil, 0)
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
	joinCM := NewClusterManager(joinUA, joinRouter)
	joinNM.SetClusterManager(joinCM)

	// 3. Perform Join
	if err := joinCM.JoinCluster(ctx, "127.0.0.1", 2554); err != nil {
		t.Fatalf("JoinCluster failed: %v", err)
	}

	// 4. Verify Welcome (Wait for state update)
	deadline := time.Now().Add(3 * time.Second)
	success := false
	for time.Now().Before(deadline) {
		joinCM.mu.RLock()
		if len(joinCM.state.AllAddresses) > 0 {
			success = true
			joinCM.mu.RUnlock()
			break
		}
		joinCM.mu.RUnlock()
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatal("timed out waiting for Welcome response")
	}
}

func TestCluster_GossipConvergence(t *testing.T) {
	// This test sets up two nodes and verifies that a "fake" member update propagates.

	// Node 1 (Seed)
	addr1 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2560), System: proto.String("sys"), Protocol: proto.String("pekko")}
	ua1 := &UniqueAddress{Address: addr1, Uid: proto.Uint64(111)}
	nm1 := NewNodeManager(addr1, 0)
	router1 := NewRouter(nm1)
	cm1 := NewClusterManager(ua1, router1)
	nm1.SetClusterManager(cm1)

	ln1, _ := net.Listen("tcp", "127.0.0.1:2560")
	defer ln1.Close()
	go func() {
		for {
			conn, err := ln1.Accept()
			if err != nil {
				return
			}
			go nm1.ProcessConnection(context.Background(), conn, INBOUND, nil, 0)
		}
	}()

	// Node 2 (Joining)
	addr2 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2561), System: proto.String("sys"), Protocol: proto.String("pekko")}
	ua2 := &UniqueAddress{Address: addr2, Uid: proto.Uint64(222)}
	nm2 := NewNodeManager(addr2, 0)
	router2 := NewRouter(nm2)
	cm2 := NewClusterManager(ua2, router2)
	nm2.SetClusterManager(cm2)

	ln2, _ := net.Listen("tcp", "127.0.0.1:2561")
	defer ln2.Close()
	go func() {
		for {
			conn, err := ln2.Accept()
			if err != nil {
				return
			}
			go nm2.ProcessConnection(context.Background(), conn, INBOUND, nil, 0)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Perform Handshake
	if err := cm2.JoinCluster(ctx, "127.0.0.1", 2560); err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	// Manually add Node 2 to Node 1's Gossip state to simulate convergence progress
	cm1.mu.Lock()
	cm1.state.AllAddresses = append(cm1.state.AllAddresses, toClusterUniqueAddress(ua2))
	cm1.state.Members = append(cm1.state.Members, &cluster.Member{
		AddressIndex: proto.Int32(int32(len(cm1.state.AllAddresses) - 1)),
		Status:       cluster.MemberStatus_Joining.Enum(),
		UpNumber:     proto.Int32(0),
	})
	cm1.mu.Unlock()

	// Start gossip ticks (faster for test)
	go cm1.gossipTick()
	go cm2.gossipTick()

	// Verify Node 2 eventually sees Node 1's update
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cm2.mu.RLock()
		if len(cm2.state.Members) >= 2 {
			cm2.mu.RUnlock()
			return // Success
		}
		cm2.mu.RUnlock()
		time.Sleep(200 * time.Millisecond)
		go cm1.gossipTick() // Force more ticks
		go cm2.gossipTick()
	}

	t.Fatal("Gossip state failed to converge")
}

func TestCluster_LeaderElection(t *testing.T) {
	// Node setup function helper
	setup := func(port uint32, uid uint64) (*ClusterManager, *NodeManager, net.Listener) {
		addr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(port), System: proto.String("leaderSys"), Protocol: proto.String("pekko")}
		ua := &UniqueAddress{Address: addr, Uid: proto.Uint64(uid)}
		nm := NewNodeManager(addr, uid)
		cm := NewClusterManager(ua, NewRouter(nm))
		nm.SetClusterManager(cm)
		ln, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go nm.ProcessConnection(context.Background(), conn, INBOUND, nil, 0)
			}
		}()
		return cm, nm, ln
	}

	cm1, _, ln1 := setup(2570, 100)
	defer ln1.Close()
	cm2, _, ln2 := setup(2571, 200)
	defer ln2.Close()
	cm3, _, ln3 := setup(2572, 50) // Lowest UID on this port
	defer ln3.Close()

	// Initial leader of each should be itself (if only node in state)
	l1 := cm1.DetermineLeader()
	if l1.GetAddress().GetPort() != 2570 {
		t.Errorf("expected node 1 as leader initially, got port %d", l1.GetAddress().GetPort())
	}

	// Connect nodes together by manual state update (to skip joining wait)
	allUA := []*cluster.UniqueAddress{
		toClusterUniqueAddress(cm1.localAddress),
		toClusterUniqueAddress(cm2.localAddress),
		toClusterUniqueAddress(cm3.localAddress),
	}
	members := []*cluster.Member{
		{AddressIndex: proto.Int32(0), Status: cluster.MemberStatus_Up.Enum()},
		{AddressIndex: proto.Int32(1), Status: cluster.MemberStatus_Up.Enum()},
		{AddressIndex: proto.Int32(2), Status: cluster.MemberStatus_Up.Enum()},
	}

	for _, cm := range []*ClusterManager{cm1, cm2, cm3} {
		cm.mu.Lock()
		cm.state.AllAddresses = allUA
		cm.state.Members = members
		cm.mu.Unlock()
	}

	// Deterministic Sorting:
	// Ports: 2570, 2571, 2572.
	// 2570 is the leader (lowest port, alphabetical host is same "127.0.0.1")
	log.Printf("Leader calculated by 1: %v", cm1.DetermineLeader().GetAddress().GetPort())
	log.Printf("Leader calculated by 2: %v", cm2.DetermineLeader().GetAddress().GetPort())
	log.Printf("Leader calculated by 3: %v", cm3.DetermineLeader().GetAddress().GetPort())

	expectPort := uint32(2570)
	if cm1.DetermineLeader().GetAddress().GetPort() != expectPort ||
		cm2.DetermineLeader().GetAddress().GetPort() != expectPort ||
		cm3.DetermineLeader().GetAddress().GetPort() != expectPort {
		t.Fatal("nodes disagreed on leader or selected wrong one")
	}
}

func TestCluster_ReachabilityFailure(t *testing.T) {
	// Node setup
	addr1 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2580), System: proto.String("sys"), Protocol: proto.String("pekko")}
	cm1 := NewClusterManager(&UniqueAddress{Address: addr1, Uid: proto.Uint64(1)}, NewRouter(NewNodeManager(addr1, 0)))

	addr2 := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2581), System: proto.String("sys"), Protocol: proto.String("pekko")}
	ua2 := &UniqueAddress{Address: addr2, Uid: proto.Uint64(2)}

	// Manually link
	cm1.mu.Lock()
	cm1.state.AllAddresses = append(cm1.state.AllAddresses, toClusterUniqueAddress(ua2))
	// Train the failure detector for node 2
	key := fmt.Sprintf("%s:%d-%d", addr2.GetHostname(), addr2.GetPort(), ua2.GetUid())
	for i := 0; i < 10; i++ {
		cm1.fd.Heartbeat(key)
	}

	cm1.state.Members = append(cm1.state.Members, &cluster.Member{AddressIndex: proto.Int32(1), Status: cluster.MemberStatus_Up.Enum()})
	cm1.mu.Unlock()

	// Simulate NO heartbeats for node 2 from node 1
	// Wait for phi to increase
	deadline := time.Now().Add(5 * time.Second)
	foundUnreachable := false
	for time.Now().Before(deadline) {
		cm1.CheckReachability() // This will check all addresses

		cm1.mu.RLock()
		if cm1.state.Overview != nil && len(cm1.state.Overview.ObserverReachability) > 0 {
			or := cm1.state.Overview.ObserverReachability[0]
			for _, sr := range or.SubjectReachability {
				if sr.GetAddressIndex() == 1 && sr.GetStatus() == cluster.ReachabilityStatus_Unreachable {
					foundUnreachable = true
					break
				}
			}
		}
		cm1.mu.RUnlock()
		if foundUnreachable {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if !foundUnreachable {
		t.Fatal("Node 1 failed to detect Node 2 as unreachable")
	}
	log.Printf("Verified: Node 1 marked Node 2 as UNREACHABLE")
}
