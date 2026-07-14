/*
 * upnumber_assignment_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * Pins Pekko's upNumber assignment semantics for leader promotions
 * (ClusterDaemon.scala leaderActionsOnConvergence): within one leader
 * pass the FIRST promoted member gets `1 + youngestMember.upNumber`
 * (youngest = the member with the highest ASSIGNED upNumber; the
 * Int.MaxValue placeholder carried by not-yet-Up members maps to 0,
 * MembershipState.scala:207-211) and every further member promoted in
 * the same pass gets `upNumber += 1` — distinct, monotonically
 * increasing values that never collide with a live member's number.
 * Age ties that still arise (concurrent leaders during a partition)
 * are broken by Member.addressOrdering = host then port
 * (Member.scala:83-92, 132-138), and members without an assigned
 * upNumber are the YOUNGEST possible, never the oldest (Pekko keeps
 * them at Int.MaxValue until copyUp; the singleton's
 * OldestChangedBuffer excludes them entirely,
 * ClusterSingletonManager.scala:339-342).
 */

package cluster

import (
	"context"
	"math"
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// upNumberOf returns the upNumber of the member at addrIdx. Test helper.
func upNumberOf(t *testing.T, cm *ClusterManager, addrIdx int32) int32 {
	t.Helper()
	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	for _, m := range cm.State.Members {
		if m.GetAddressIndex() == addrIdx {
			return m.GetUpNumber()
		}
	}
	t.Fatalf("member with addrIdx %d not found", addrIdx)
	return 0
}

// TestLeaderPromotion_SamePassAssignsDistinctMonotonicUpNumbers pins that
// two members promoted Joining → Up in the SAME leader pass receive
// DISTINCT, consecutive upNumbers seeded from the youngest assigned number
// — not one shared len(members) value. A shared upNumber pushes the age
// ordering between the two members onto tie-breaks for every later
// OldestNode/keep-oldest decision.
func TestLeaderPromotion_SamePassAssignsDistinctMonotonicUpNumbers(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.WelcomeReceived.Store(true)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	j1 := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_Joining, 0)
	j2 := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2543, 43), gproto_cluster.MemberStatus_Joining, 0)
	// Convergence: the only Up member (local) has seen the state.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	cm.Mu.Unlock()

	cm.performLeaderActions()

	u1, u2 := upNumberOf(t, cm, j1), upNumberOf(t, cm, j2)
	if u1 == u2 {
		t.Fatalf("both members promoted in the same leader pass share upNumber %d — Pekko assigns 1+youngest to the first and += 1 to each further member (ClusterDaemon.scala:1399-1407)", u1)
	}
	got := map[int32]bool{u1: true, u2: true}
	if !got[2] || !got[3] {
		t.Fatalf("promoted upNumbers = {%d, %d}, want {2, 3} — youngest assigned upNumber is 1 (the local leader), so the pass assigns 2 then 3", u1, u2)
	}
}

// TestLeaderPromotion_SeedsFromYoungestUpNumberNotMemberCount pins the
// monotonicity requirement: the seed is the highest upNumber currently
// assigned, NOT the member count. After old members leave, len(members)
// undercuts surviving upNumbers, making a brand-new member "older than" a
// genuinely older one in every upNumber-dependent decision cluster-wide
// (the assignment is gossiped verbatim and adopted by JVM peers).
func TestLeaderPromotion_SeedsFromYoungestUpNumberNotMemberCount(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.WelcomeReceived.Store(true)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	// Survivor of earlier churn: upNumbers 2-4 belonged to since-removed
	// members, this peer was promoted fifth.
	pIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2551, 51), gproto_cluster.MemberStatus_Up, 5)
	jIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_Joining, 0)
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0, pIdx}}
	cm.Mu.Unlock()

	cm.performLeaderActions()

	if got := upNumberOf(t, cm, jIdx); got != 6 {
		t.Fatalf("promoted upNumber = %d, want 6 — Pekko seeds 1 + youngestMember.upNumber (= 5 here); a len(members)-based value (3) undercuts the surviving member's 5 and inverts the age ordering", got)
	}
}

// TestLeaderPromotion_WeaklyUpSharesThePassCounter pins that WeaklyUp → Up
// promotions draw from the SAME per-pass counter as Joining → Up — Pekko's
// isJoiningToUp covers both statuses in one collect with one counter.
func TestLeaderPromotion_WeaklyUpSharesThePassCounter(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.WelcomeReceived.Store(true)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	wIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_WeaklyUp, 0)
	jIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2543, 43), gproto_cluster.MemberStatus_Joining, 0)
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	cm.Mu.Unlock()

	cm.performLeaderActions()

	uw, uj := upNumberOf(t, cm, wIdx), upNumberOf(t, cm, jIdx)
	if uw == uj {
		t.Fatalf("WeaklyUp→Up and Joining→Up promotions in the same pass share upNumber %d — Pekko uses one counter across both transitions", uw)
	}
	got := map[int32]bool{uw: true, uj: true}
	if !got[2] || !got[3] {
		t.Fatalf("promoted upNumbers = {%d, %d}, want {2, 3}", uw, uj)
	}
}

// TestLeaderPromotion_MaxValuePlaceholderIsNotTheYoungestSeed pins that a
// member still carrying Pekko's Int.MaxValue not-yet-Up placeholder (as
// received on the wire for Joining/WeaklyUp members) does not poison the
// youngest-seed computation: Pekko maps Int.MaxValue to 0 in
// youngestMember (MembershipState.scala:210).
func TestLeaderPromotion_MaxValuePlaceholderIsNotTheYoungestSeed(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	cm.WelcomeReceived.Store(true)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	// Pekko-originated Joining member carrying the Int.MaxValue placeholder.
	fIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2551, 51), gproto_cluster.MemberStatus_Joining, math.MaxInt32)
	jIdx := addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_Joining, 0)
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	cm.Mu.Unlock()

	cm.performLeaderActions()

	uf, uj := upNumberOf(t, cm, fIdx), upNumberOf(t, cm, jIdx)
	if uf == uj {
		t.Fatalf("same-pass promotions share upNumber %d", uf)
	}
	got := map[int32]bool{uf: true, uj: true}
	if !got[2] || !got[3] {
		t.Fatalf("promoted upNumbers = {%d, %d}, want {2, 3} — the Int.MaxValue placeholder must be treated as unassigned (0), not as the youngest seed (which would overflow)", uf, uj)
	}
}

// TestPromoteDCMembers_AssignsDistinctMonotonicUpNumbers pins the same
// per-pass semantics for the DC-leader promotion path.
func TestPromoteDCMembers_AssignsDistinctMonotonicUpNumbers(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	roleIdxs := cm.upsertRolesLocked([]string{"dc-tokyo"})
	var idxs []int32
	for i, port := range []uint32{2542, 2543} {
		ua := makeUAWithDC("127.0.0.1", port, uint32(42+i))
		addrIdx := int32(len(cm.State.AllAddresses))
		cm.State.AllAddresses = append(cm.State.AllAddresses, ua)
		cm.State.AllHashes = append(cm.State.AllHashes, "0")
		cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
			AddressIndex: proto.Int32(addrIdx),
			Status:       gproto_cluster.MemberStatus_Joining.Enum(),
			UpNumber:     proto.Int32(0),
			RolesIndexes: roleIdxs,
		})
		idxs = append(idxs, addrIdx)
	}
	cm.Mu.Unlock()

	cm.promoteDCMembers("tokyo")

	u1, u2 := upNumberOf(t, cm, idxs[0]), upNumberOf(t, cm, idxs[1])
	if u1 == u2 {
		t.Fatalf("both DC members promoted in the same pass share upNumber %d — want distinct monotonic values", u1)
	}
	got := map[int32]bool{u1: true, u2: true}
	if !got[2] || !got[3] {
		t.Fatalf("promoted upNumbers = {%d, %d}, want {2, 3}", u1, u2)
	}
}

// TestSBRKeepOldest_UpNumberTieBreaksByHostThenPort pins that the SBR
// oldest-member selection breaks upNumber ties by host THEN PORT, matching
// Pekko's Member.addressOrdering (Member.scala:132-138). A host-only
// tie-break leaves same-host members (co-located deploys) tied, letting
// slice order decide — a gekka node and a JVM node could then pick
// DIFFERENT "oldest" members on the two sides of a keep-oldest partition
// and issue inconsistent survive/down verdicts.
func TestSBRKeepOldest_UpNumberTieBreaksByHostThenPort(t *testing.T) {
	a := Member{Address: MemberAddress{Host: "127.0.0.1", Port: 2552}, Reachable: true, UpNumber: 7}
	b := Member{Address: MemberAddress{Host: "127.0.0.1", Port: 2551}, Reachable: true, UpNumber: 7}
	// Deliberately order the slice so a host-only tie-break returns the
	// WRONG member (first in slice).
	got := oldestMember([]Member{a, b})
	if got.Address.Port != 2551 {
		t.Fatalf("oldestMember with tied upNumbers returned port %d, want 2551 — Pekko's isOlderThan tie-breaks by addressOrdering = host then port (Member.scala:83-92, 132-138)", got.Address.Port)
	}
}

// TestOldestNode_UnassignedUpNumberIsYoungestNotOldest pins that a member
// without an assigned upNumber (gekka keeps not-yet-Up members at 0) is the
// YOUNGEST candidate, never the oldest: Pekko carries Int.MaxValue until
// copyUp, and the singleton's OldestChangedBuffer excludes Joining/WeaklyUp
// entirely. With 0 sorting first, a transient WeaklyUp member would steal
// singleton-oldest during every non-converged window.
func TestOldestNode_UnassignedUpNumberIsYoungestNotOldest(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(2)
	addMemberWithStatus(cm, makeUAWithDC("127.0.0.1", 2542, 42), gproto_cluster.MemberStatus_WeaklyUp, 0)
	cm.Mu.Unlock()

	ua := cm.OldestNode("")
	if ua == nil {
		t.Fatal("OldestNode returned nil")
	}
	if ua.GetAddress().GetPort() != 2541 {
		t.Fatalf("OldestNode returned port %d, want 2541 — a WeaklyUp member with unassigned upNumber (0) must sort YOUNGEST (Pekko: Int.MaxValue until copyUp), not oldest", ua.GetAddress().GetPort())
	}
}
