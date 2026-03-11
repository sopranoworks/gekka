/*
 * cluster_router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

func newUniqueAddress(addr *Address, uid uint64) *UniqueAddress {
	return &UniqueAddress{
		Address: addr,
		Uid:     proto.Uint32(uint32(uid & 0xFFFFFFFF)),
		Uid2:    proto.Uint32(uint32(uid >> 32)),
	}
}

func TestClusterRouter_RoundRobinSelection(t *testing.T) {
	// 1. Setup Cluster State with 3 UP nodes
	localAddr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2590), System: proto.String("sys"), Protocol: proto.String("pekko")}
	localUA := &UniqueAddress{Address: localAddr, Uid: proto.Uint32(1), Uid2: proto.Uint32(0)}

	cm := NewClusterManager(localUA, nil)
	
	// Prepare state
	cm.Mu.Lock()
	cm.State.Members[0].Status = MemberStatus_Up.Enum()
	
	cm.State.AllAddresses = append(cm.State.AllAddresses,
		newUniqueAddress(&Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2591), System: proto.String("sys"), Protocol: proto.String("pekko")}, 2),
		newUniqueAddress(&Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2592), System: proto.String("sys"), Protocol: proto.String("pekko")}, 3))

	cm.State.Members = append(cm.State.Members,
		&Member{AddressIndex: proto.Int32(1), Status: MemberStatus_Up.Enum()},
		&Member{AddressIndex: proto.Int32(2), Status: MemberStatus_Up.Enum()})
	cm.Mu.Unlock()

	router := NewClusterRouter(cm, nil)
	settings := &ClusterRouterPoolSettings{
		TotalInstances:    proto.Uint32(10),
		AllowLocalRoutees: proto.Bool(true),
	}

	// 2. Mock Heartbeats to make them available
	for i := 0; i < 3; i++ {
		ua := cm.State.AllAddresses[i]
		key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
		for j := 0; j < 5; j++ {
			cm.Fd.Heartbeat(key)
		}
	}

	// 3. Round Robin across 3 nodes
	ports := make(map[uint32]int)
	for i := 0; i < 6; i++ {
		ua, err := router.SelectRoutee(settings)
		if err != nil {
			t.Fatalf("failed to select routee: %v", err)
		}
		ports[ua.GetAddress().GetPort()]++
	}

	if len(ports) != 3 {
		t.Errorf("expected 3 nodes in selection, got %v", len(ports))
	}
	for p, count := range ports {
		if count != 2 {
			t.Errorf("port %d should have been selected twice, got %d", p, count)
		}
	}
}

func TestClusterRouter_HealthFiltering(t *testing.T) {
	localAddr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2600), System: proto.String("sys"), Protocol: proto.String("pekko")}
	localUA := &UniqueAddress{Address: localAddr, Uid: proto.Uint32(1), Uid2: proto.Uint32(0)}
	cm := NewClusterManager(localUA, nil)
	
	cm.Mu.Lock()
	cm.State.Members[0].Status = MemberStatus_Up.Enum()
	
	// Add a remote node but don't give it heartbeats
	cm.State.AllAddresses = append(cm.State.AllAddresses,
		newUniqueAddress(&Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2601), System: proto.String("sys"), Protocol: proto.String("pekko")}, 2))
	cm.State.Members = append(cm.State.Members,
		&Member{AddressIndex: proto.Int32(1), Status: MemberStatus_Up.Enum()})
	cm.Mu.Unlock()

	router := NewClusterRouter(cm, nil)
	settings := &ClusterRouterPoolSettings{
		AllowLocalRoutees: proto.Bool(false), // Only remote
	}

	_, err := router.SelectRoutee(settings)
	if err == nil {
		t.Fatal("expected error since no remote nodes are healthy")
	}

	// Now make it healthy
	ua := cm.State.AllAddresses[1]
	key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
	for i := 0; i < 10; i++ {
		cm.Fd.Heartbeat(key)
	}

	uaSelected, err := router.SelectRoutee(settings)
	if err != nil {
		t.Fatalf("failed to select now-healthy node: %v", err)
	}
	if uaSelected.GetAddress().GetPort() != 2601 {
		t.Errorf("expected port 2601, got %d", uaSelected.GetAddress().GetPort())
	}
}

func TestClusterRouter_LocalAffinity(t *testing.T) {
	localAddr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2610), System: proto.String("sys"), Protocol: proto.String("pekko")}
	localUA := &UniqueAddress{Address: localAddr, Uid: proto.Uint32(1), Uid2: proto.Uint32(0)}
	cm := NewClusterManager(localUA, nil)
	
	cm.Mu.Lock()
	cm.State.Members[0].Status = MemberStatus_Up.Enum()
	cm.Mu.Unlock()

	// Make local healthy
	key := fmt.Sprintf("%s:%d-%d", localAddr.GetHostname(), localAddr.GetPort(), uint32(localUA.GetUid()&0xFFFFFFFF))
	for i := 0; i < 5; i++ {
		cm.Fd.Heartbeat(key)
	}

	router := NewClusterRouter(cm, nil)

	// Exclude local
	settingsNoLocal := &ClusterRouterPoolSettings{
		AllowLocalRoutees: proto.Bool(false),
	}
	_, err := router.SelectRoutee(settingsNoLocal)
	if err == nil {
		t.Fatal("expected error as local node is the only one but excluded")
	}

	// Include local
	settingsWithLocal := &ClusterRouterPoolSettings{
		AllowLocalRoutees: proto.Bool(true),
	}
	ua, err := router.SelectRoutee(settingsWithLocal)
	if err != nil {
		t.Fatalf("expected to select local node: %v", err)
	}
	if ua.GetAddress().GetPort() != 2610 {
		t.Errorf("expected local port 2610, got %d", ua.GetAddress().GetPort())
	}
}

func TestClusterRouter_RoleFiltering(t *testing.T) {
	localAddr := &Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2620), System: proto.String("sys"), Protocol: proto.String("pekko")}
	localUA := &UniqueAddress{Address: localAddr, Uid: proto.Uint32(1), Uid2: proto.Uint32(0)}
	cm := NewClusterManager(localUA, nil)
	
	cm.Mu.Lock()
	cm.State.AllRoles = []string{"compute", "storage"}
	cm.State.Members[0].Status = MemberStatus_Up.Enum()
	
	// Add node 1 (compute)
	cm.State.AllAddresses = append(cm.State.AllAddresses,
		newUniqueAddress(&Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2621), System: proto.String("sys"), Protocol: proto.String("pekko")}, 2))
	cm.State.Members = append(cm.State.Members,
		&Member{AddressIndex: proto.Int32(1), Status: MemberStatus_Up.Enum(), RolesIndexes: []int32{0}})

	// Add node 2 (storage)
	cm.State.AllAddresses = append(cm.State.AllAddresses,
		newUniqueAddress(&Address{Hostname: proto.String("127.0.0.1"), Port: proto.Uint32(2622), System: proto.String("sys"), Protocol: proto.String("pekko")}, 3))
	cm.State.Members = append(cm.State.Members,
		&Member{AddressIndex: proto.Int32(2), Status: MemberStatus_Up.Enum(), RolesIndexes: []int32{1}})
	cm.Mu.Unlock()

	// Make them healthy
	for i := 1; i <= 2; i++ {
		ua := cm.State.AllAddresses[i]
		key := fmt.Sprintf("%s:%d-%d", ua.GetAddress().GetHostname(), ua.GetAddress().GetPort(), ua.GetUid())
		for j := 0; j < 5; j++ {
			cm.Fd.Heartbeat(key)
		}
	}

	router := NewClusterRouter(cm, nil)

	// Test filtering for "storage"
	settingsStorage := &ClusterRouterPoolSettings{
		UseRoles:          []string{"storage"},
		AllowLocalRoutees: proto.Bool(false),
	}
	ua, err := router.SelectRoutee(settingsStorage)
	if err != nil {
		t.Fatalf("failed to select storage node: %v", err)
	}
	if ua.GetAddress().GetPort() != 2622 {
		t.Errorf("expected port 2622, got %d", ua.GetAddress().GetPort())
	}

	// Test filtering for "compute"
	settingsCompute := &ClusterRouterPoolSettings{
		UseRoles:          []string{"compute"},
		AllowLocalRoutees: proto.Bool(false),
	}
	ua, err = router.SelectRoutee(settingsCompute)
	if err != nil {
		t.Fatalf("failed to select compute node: %v", err)
	}
	if ua.GetAddress().GetPort() != 2621 {
		t.Errorf("expected port 2621, got %d", ua.GetAddress().GetPort())
	}
}
