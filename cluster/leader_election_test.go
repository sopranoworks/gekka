/*
 * leader_election_test.go
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

// addMemberWithStatus adds a member to cm.State with the given status and
// upNumber. Must be called while holding cm.Mu (write). Test helper.
func addMemberWithStatus(cm *ClusterManager, ua *gproto_cluster.UniqueAddress, status gproto_cluster.MemberStatus, upNumber int32) int32 {
	addrIdx := int32(len(cm.State.AllAddresses))
	cm.State.AllAddresses = append(cm.State.AllAddresses, ua)
	cm.State.AllHashes = append(cm.State.AllHashes, "0")
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(addrIdx),
		Status:       status.Enum(),
		UpNumber:     proto.Int32(upNumber),
	})
	return addrIdx
}

// TestDetermineLeader_TerminalStatusesNeverLead pins that Down, Exiting and
// Removed members can never be elected leader, even when they sort first by
// address — only a Joining/WeaklyUp fallback candidate remains eligible.
func TestDetermineLeader_TerminalStatusesNeverLead(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("10.0.0.9", 2552, 1) // local Joining, sorts LAST
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	addMemberWithStatus(cm, makeUAWithDC("10.0.0.1", 2552, 2), gproto_cluster.MemberStatus_Down, 1)
	addMemberWithStatus(cm, makeUAWithDC("10.0.0.2", 2552, 3), gproto_cluster.MemberStatus_Exiting, 2)
	addMemberWithStatus(cm, makeUAWithDC("10.0.0.3", 2552, 4), gproto_cluster.MemberStatus_Removed, 3)
	cm.Mu.Unlock()

	got := cm.DetermineLeader()
	if got == nil {
		t.Fatal("DetermineLeader returned nil — the Joining local node must win the bootstrap fallback")
	}
	if got.GetAddress().GetHostname() != "10.0.0.9" {
		t.Fatalf("DetermineLeader returned %s:%d — a Down/Exiting/Removed member must never lead",
			got.GetAddress().GetHostname(), got.GetAddress().GetPort())
	}
}

// TestPerformLeaderActions_MixedClusterAddressLowestUpPromotesJoiningPeers is
// the direct regression for the 2026-07-13 clean-boot showcase convergence
// deadlock (findings/2026-07-12-open-clean-boot-convergence-issue.md).
//
// Topology mirrors the showcase run: five JVM members at ports 2551-2555
// carrying upNumbers 1-5, the local gekka node at port 2541 already promoted
// to Up (by the JVM leader, upNumber 6), and two gekka peers at 2542/2543
// still Joining. Per Pekko's MembershipState.leaderOf, the leader is the
// FIRST member in address ordering whose status is Up/Leaving — the local
// node — NOT the member with the smallest upNumber (that ordering is
// ageOrdering, used for singleton-oldest only). Every JVM peer computes
// leadership this way and therefore stops promoting joiners the moment
// 2541 is Up; if gekka defers to the upNumber-lowest member instead, no
// node in the cluster performs Joining → Up transitions and the join stalls
// forever — exactly the observed Gate-1 deadlock.
func TestPerformLeaderActions_MixedClusterAddressLowestUpPromotesJoiningPeers(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.WelcomeReceived.Store(true) // node is a welcomed cluster member

	cm.Mu.Lock()
	// Local node: address-lowest, already Up (promoted by the JVM leader).
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(6)
	// JVM members s1..s5, upNumbers 1..5 — all Up, all with HIGHER ports.
	upIdxs := []int32{0} // local is address index 0
	for i, port := range []uint32{2551, 2552, 2553, 2554, 2555} {
		idx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", port, uint32(51+i)), gproto_cluster.MemberStatus_Up, int32(1+i))
		upIdxs = append(upIdxs, idx)
	}
	// gekka peers g2/g3 — still Joining, upNumber unassigned.
	g2Idx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_Joining, 0)
	g3Idx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2543, 43), gproto_cluster.MemberStatus_Joining, 0)
	// Convergence holds: every Up member has seen the current state.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: upIdxs}
	cm.Mu.Unlock()

	leader := cm.DetermineLeader()
	if leader == nil {
		t.Fatal("DetermineLeader returned nil with six Up members")
	}
	if leader.GetAddress().GetPort() != 2541 {
		t.Fatalf("DetermineLeader returned port %d — Pekko leaderOf selects the address-lowest Up member (2541), not the upNumber-lowest (2551)",
			leader.GetAddress().GetPort())
	}

	cm.performLeaderActions()

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, tc := range []struct {
		label string
		idx   int32
	}{{"g2", g2Idx}, {"g3", g3Idx}} {
		var st gproto_cluster.MemberStatus
		for _, m := range cm.State.Members {
			if m.GetAddressIndex() == tc.idx {
				st = m.GetStatus()
			}
		}
		if st != gproto_cluster.MemberStatus_Up {
			t.Errorf("%s status = %v, want Up — the address-lowest Up member must perform leader actions (Joining-stuck convergence deadlock)", tc.label, st)
		}
	}
}

// TestPerformLeaderActions_NoLeaderActionsWhileRemoteJoinPending pins the
// pre-Welcome gate: a node that has asked to join a REMOTE seed is not yet a
// cluster member and must not perform any leader action — in particular it
// must not promote itself Joining → Up (upNumber=1) off its own single-node
// view while the InitJoin/Welcome exchange is still in flight. That bogus
// self-promotion is what made showcase nodes print READY (IsLocalNodeUp)
// while the real gossiped membership still had them Joining, and it seeds
// the upNumber-1 conflict that SBR later resolves by self-downing.
func TestPerformLeaderActions_NoLeaderActionsWhileRemoteJoinPending(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2542, 42)
	cm := NewClusterManager(local, router)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Join a remote seed; the (no-op router) Welcome never arrives.
	if err := cm.JoinCluster(ctx, "127.0.0.1", 2551); err != nil {
		t.Fatalf("JoinCluster: %v", err)
	}

	// However many leader ticks fire before Welcome, the node must stay Joining.
	for i := 0; i < 3; i++ {
		cm.performLeaderActions()
	}
	if cm.IsLocalNodeUp() {
		t.Fatal("node promoted itself to Up before the seed's Welcome arrived — pre-Welcome self-promotion regression")
	}

	// Once welcomed, leader actions resume (single-member view promotes via
	// the bootstrap path; in production the merged Welcome state governs).
	cm.WelcomeReceived.Store(true)
	cm.performLeaderActions()
	if !cm.IsLocalNodeUp() {
		t.Fatal("node failed to promote after Welcome — the pre-Welcome gate must not permanently disable leader actions")
	}
}

// TestJoinCluster_SelfJoinKeepsBootstrapOpen pins that joining ONESELF (the
// Pekko "is JOINING itself and forming new cluster" bootstrap) does not arm
// the remote-join pending gate: a self-joining seed node has no peer to be
// welcomed by and must still self-promote on its first leader tick.
func TestJoinCluster_SelfJoinKeepsBootstrapOpen(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2551, 51)
	cm := NewClusterManager(local, router)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cm.JoinCluster(ctx, "127.0.0.1", 2551); err != nil {
		t.Fatalf("JoinCluster(self): %v", err)
	}

	cm.performLeaderActions()
	if !cm.IsLocalNodeUp() {
		t.Fatal("self-joining seed node failed to bootstrap-promote itself to Up")
	}
}
