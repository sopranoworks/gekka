/*
 * gossip_prune_seen_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

// These tests pin gekka's gossip removal-propagation and convergence
// semantics to Pekko's protocol (the source of truth), covering the two
// residual divergences documented in the 2026-07-13 member-flicker findings:
//
// Divergence A — vector-clock pruning on member removal:
//   - Gossip.remove (Gossip.scala:253) prunes the removed member's clock
//     node and records a tombstone; the leader does this for every
//     Exiting/Down → Removed transition (ClusterDaemon removeAll).
//   - Gossip.merge (Gossip.scala:184-186) prunes clock entries for every
//     tombstoned node after unioning the two clocks.
//   - receiveGossip's merge case (ClusterDaemon.scala:1141-1154) prunes the
//     clock node of any member whose status is Down/Exiting on one side and
//     absent from the other — replaying the leader's prune when the
//     tombstone is not available.
//   - Member.pickHighestPriority (Member.scala:197) drops a member present
//     in only ONE of the merged gossips when its UniqueAddress is
//     tombstoned — removal must win over a stale concurrent branch that
//     still carries the member.
//
// Divergence B — Overview.Seen is version-scoped, never carried across
// versions:
//   - updateLatestGossip (ClusterDaemon.scala:1626-1641) pairs EVERY local
//     change (`:+ vclockNode`) with onlySeen(self): nobody else can have
//     seen the new version.
//   - Gossip.merge (Gossip.scala:196-197) resets Seen to empty ("Nobody can
//     have seen this new gossip yet"); receiveGossip then re-marks only
//     self.
//   - Only the Same-version case unions the two Seen sets (mergeSeen,
//     ClusterDaemon.scala:1127) — both sets acknowledge the identical
//     version, so the union is sound there and ONLY there.
//
// gekka previously did none of the pruning and unioned Seen everywhere,
// which (a) grew the clock without bound under membership churn and
// reinfected pruned components into JVM peers, and (b) let acknowledgements
// of OLD versions satisfy the convergence check for NEW versions — leader
// actions gated on convergence could fire with zero peers having seen the
// state they were gated on.

import (
	"context"
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

// setClock replaces g's vector clock with the given node-name→timestamp map,
// using the same wire-shape builder as production code.
func setClock(g *gproto_cluster.Gossip, m map[string]int64) {
	g.AllHashes, g.Version = clockFromResolvedMap(m)
}

// resolvedClock returns g's clock as a node-name→timestamp map.
func resolvedClock(cm *ClusterManager, g *gproto_cluster.Gossip) map[string]int64 {
	return cm.vectorClockToMap(g.Version, g.AllHashes)
}

// TestLeaderRemoval_PrunesClockNodeAndRecordsTombstone pins Pekko's
// Gossip.remove semantics for the leader's Exiting → Removed transition:
// the removed member's vector-clock component is pruned (Gossip.scala:253)
// and a tombstone is recorded (Gossip.scala:255) so the removal propagates
// on the wire and later merges cannot reintroduce the component.
func TestLeaderRemoval_PrunesClockNodeAndRecordsTombstone(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	x := makeUAWithDC("127.0.0.1", 2551, 51)
	xName := pekkoVclockNodeHash(x)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	cm.State.Members[0].UpNumber = proto.Int32(1)
	xIdx := addMemberWithStatus(cm, x, gproto_cluster.MemberStatus_Exiting, 2)
	setClock(cm.State, map[string]int64{cm.localHashString(): 3, xName: 7})
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	cm.Mu.Unlock()

	// Local node is the address-lowest Up member → leader. The pass must
	// transition X Exiting → Removed, prune X's clock node and tombstone it.
	cm.performLeaderActions()

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	clock := resolvedClock(cm, cm.State)
	if _, ok := clock[xName]; ok {
		t.Errorf("removed member's clock component survived the leader's removal — Pekko's Gossip.remove prunes VectorClock.Node(vclockName(node)); clock = %v", clock)
	}
	if _, ok := clock[cm.localHashString()]; !ok {
		t.Errorf("local clock component missing after removal pass; clock = %v", clock)
	}
	foundTombstone := false
	for _, ts := range cm.State.GetTombstones() {
		if ts.GetAddressIndex() == xIdx {
			foundTombstone = true
			if ts.GetTimestamp() <= 0 {
				t.Errorf("tombstone for removed member has no removal timestamp")
			}
		}
	}
	if !foundTombstone {
		t.Errorf("no wire tombstone recorded for the removed member — Pekko's Gossip.remove adds (node -> removalTimestamp) so peers apply merge-time pruning and resurrection protection")
	}
}

// TestMergeGossipStates_PrunesTombstonedClockNodes pins Pekko's Gossip.merge
// step 2 (Gossip.scala:183-186): after unioning the two vector clocks, every
// clock entry belonging to a tombstoned node is pruned.
func TestMergeGossipStates_PrunesTombstonedClockNodes(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	lName := cm.localHashString()

	x := makeUAWithDC("127.0.0.1", 2551, 51)
	xName := pekkoVclockNodeHash(x)

	mkMember := func(idx int32, st gproto_cluster.MemberStatus) *gproto_cluster.Member {
		return &gproto_cluster.Member{AddressIndex: proto.Int32(idx), Status: st.Enum(), UpNumber: proto.Int32(1)}
	}

	// Local branch: X was fully removed a while ago but its clock component
	// still lingers (the pre-fix steady state).
	s1 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, x},
		Members:      []*gproto_cluster.Member{mkMember(0, gproto_cluster.MemberStatus_Up)},
		Overview:     &gproto_cluster.GossipOverview{},
	}
	setClock(s1, map[string]int64{lName: 3, xName: 7})

	// Remote branch: carries the removal tombstone for X (what a JVM peer
	// gossips after its leader removed X).
	s2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, x},
		Members:      []*gproto_cluster.Member{mkMember(0, gproto_cluster.MemberStatus_Up)},
		Overview:     &gproto_cluster.GossipOverview{},
		Tombstones: []*gproto_cluster.Tombstone{
			{AddressIndex: proto.Int32(1), Timestamp: proto.Int64(1234567890000)},
		},
	}
	setClock(s2, map[string]int64{lName: 2})

	cm.Mu.Lock()
	merged := cm.mergeGossipStates(s1, s2)
	cm.Mu.Unlock()

	clock := resolvedClock(cm, merged)
	if _, ok := clock[xName]; ok {
		t.Errorf("tombstoned node's clock component survived the merge — Pekko's Gossip.merge prunes entries for every tombstoned node; clock = %v", clock)
	}
	if got := clock[lName]; got != 3 {
		t.Errorf("local component = %d, want pairwise max 3", got)
	}
	if len(merged.GetTombstones()) == 0 {
		t.Errorf("merged gossip lost the incoming tombstone — Pekko's Gossip.merge unions both sides' tombstones (step 1)")
	}
}

// TestMergeGossipStates_PrunesRemovedOnOneSideClockNodes pins the
// receiveGossip merge-case pre-prune (ClusterDaemon.scala:1141-1154): a
// member whose status is Down/Exiting in one gossip and absent from the
// other was removed by the other side's leader — its clock node must be
// pruned even when no tombstone is available (tombstones expire after
// prune-gossip-tombstones-after).
func TestMergeGossipStates_PrunesRemovedOnOneSideClockNodes(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	lName := cm.localHashString()

	x := makeUAWithDC("127.0.0.1", 2551, 51)
	xName := pekkoVclockNodeHash(x)

	mk := func(idx int32, st gproto_cluster.MemberStatus) *gproto_cluster.Member {
		return &gproto_cluster.Member{AddressIndex: proto.Int32(idx), Status: st.Enum(), UpNumber: proto.Int32(1)}
	}

	// Local branch still carries X as Down, with X's clock component.
	s1 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, x},
		Members: []*gproto_cluster.Member{
			mk(0, gproto_cluster.MemberStatus_Up),
			mk(1, gproto_cluster.MemberStatus_Down),
		},
		Overview: &gproto_cluster.GossipOverview{},
	}
	setClock(s1, map[string]int64{lName: 3, xName: 7})

	// Remote branch: X is gone entirely (removed + tombstone already
	// expired), concurrent with the local branch.
	s2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local},
		Members:      []*gproto_cluster.Member{mk(0, gproto_cluster.MemberStatus_Up)},
		Overview:     &gproto_cluster.GossipOverview{},
	}
	setClock(s2, map[string]int64{lName: 4})

	cm.Mu.Lock()
	merged := cm.mergeGossipStates(s1, s2)
	cm.Mu.Unlock()

	clock := resolvedClock(cm, merged)
	if _, ok := clock[xName]; ok {
		t.Errorf("Down-on-one-side/absent-on-other member's clock component survived the merge — ClusterDaemon.scala:1141-1154 replays the leader's prune; clock = %v", clock)
	}
	if got := clock[lName]; got != 4 {
		t.Errorf("local component = %d, want pairwise max 4", got)
	}
}

// TestMergeGossipStates_DropsTombstonedSingleSideMember pins
// Member.pickHighestPriority (Member.scala:192-201): a member present in
// only ONE of the two gossips whose exact UniqueAddress is tombstoned is
// dropped from the merged member set. Without this, a stale concurrent
// branch that still carries the member as Up resurrects it cluster-wide:
// JVM peers adopt gekka's merged gossip wholesale on the After path, and
// nothing ever removes the zombie again.
func TestMergeGossipStates_DropsTombstonedSingleSideMember(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	lName := cm.localHashString()

	x := makeUAWithDC("127.0.0.1", 2551, 51)

	mk := func(idx int32, st gproto_cluster.MemberStatus) *gproto_cluster.Member {
		return &gproto_cluster.Member{AddressIndex: proto.Int32(idx), Status: st.Enum(), UpNumber: proto.Int32(1)}
	}

	// Stale local branch: X still Up.
	s1 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, x},
		Members: []*gproto_cluster.Member{
			mk(0, gproto_cluster.MemberStatus_Up),
			mk(1, gproto_cluster.MemberStatus_Up),
		},
		Overview: &gproto_cluster.GossipOverview{},
	}
	setClock(s1, map[string]int64{lName: 3})

	// Remote branch: X removed, tombstone present, member gone.
	s2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, x},
		Members:      []*gproto_cluster.Member{mk(0, gproto_cluster.MemberStatus_Up)},
		Overview:     &gproto_cluster.GossipOverview{},
		Tombstones: []*gproto_cluster.Tombstone{
			{AddressIndex: proto.Int32(1), Timestamp: proto.Int64(1234567890000)},
		},
	}
	setClock(s2, map[string]int64{lName: 2})

	cm.Mu.Lock()
	merged := cm.mergeGossipStates(s1, s2)
	cm.Mu.Unlock()

	for _, m := range merged.Members {
		idx := int(m.GetAddressIndex())
		if idx < len(merged.AllAddresses) {
			ua := merged.AllAddresses[idx]
			if ua.GetAddress().GetPort() == 2551 && ua.GetUid() == x.GetUid() {
				t.Errorf("tombstoned single-side member survived the merge as %v — Member.pickHighestPriority drops it (removal must win over a stale concurrent branch)", m.GetStatus())
			}
		}
	}
}

// TestIncrementVersion_ResetsSeenToOnlySelf pins Pekko's updateLatestGossip
// (ClusterDaemon.scala:1626-1641): every local change stamps the clock
// (`:+ vclockNode`) AND resets Seen to only self (onlySeen) — nobody else
// can have seen the new version. Without the reset, acknowledgements of the
// PREVIOUS version keep satisfying CheckConvergence for the NEW version,
// so convergence-gated leader actions fire with zero peers having actually
// seen the state they are gated on.
func TestIncrementVersion_ResetsSeenToOnlySelf(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	b := makeUAWithDC("127.0.0.1", 2551, 51)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	bIdx := addMemberWithStatus(cm, b, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, map[string]int64{cm.localHashString(): 1})
	// Both members acknowledged the CURRENT version.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0, bIdx}}

	cm.incrementVersionWithLockHeld()

	seen := append([]int32(nil), cm.State.Overview.GetSeen()...)
	cm.Mu.Unlock()

	if len(seen) != 1 || seen[0] != 0 {
		t.Errorf("Seen after a local change = %v, want exactly [0] (only self) — Pekko's updateLatestGossip calls onlySeen(self) with every `:+ vclockNode` stamp", seen)
	}
}

// TestConcurrentGossipMerge_ResetsSeenToSelf pins Pekko's Gossip.merge Seen
// handling (Gossip.scala:196-197 "Nobody can have seen this new gossip
// yet") plus receiveGossip's re-mark of self (ClusterDaemon.scala:1165):
// after merging two CONCURRENT gossips the Seen set contains exactly the
// local node. Unioning both sides' Seen counts members as having
// acknowledged a merged gossip none of them ever received.
func TestConcurrentGossipMerge_ResetsSeenToSelf(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	lName := cm.localHashString()

	r := makeUAWithDC("127.0.0.1", 2551, 51)
	rName := pekkoVclockNodeHash(r)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	rIdx := addMemberWithStatus(cm, r, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, map[string]int64{lName: 2})
	// Locally, both members acknowledged the local branch.
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0, rIdx}}
	cm.Mu.Unlock()

	// Remote concurrent branch (addresses deliberately in a different
	// order to exercise index remapping): R bumped its own component and
	// marked itself seen.
	remote := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{r, local},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(2)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{Seen: []int32{0}},
	}
	setClock(remote, map[string]int64{rName: 2})

	if err := cm.processIncomingGossip(remote, r); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	seen := cm.State.Overview.GetSeen()
	if len(seen) != 1 || seen[0] != 0 {
		t.Errorf("Seen after a concurrent merge = %v, want exactly [0] (only self) — Pekko's Gossip.merge resets Seen and receiveGossip re-marks only self", seen)
	}
}

// TestUpdateReachability_ReachableNeverCreatesRecords pins Pekko's
// Reachability semantics: records are created ONLY when a node is observed
// UNREACHABLE (Reachability.unreachable); the reachable path merely
// transitions an existing Unreachable record back. A fully healthy cluster
// has an EMPTY reachability table. gekka's CheckReachability calls
// updateReachability(Reachable) for every healthy member on every tick, so
// creating a fresh record there bumps the gossip version once per newly
// seen member — and each bump resets Seen (onlySeen(self)), restarting
// convergence. During an 8-node join window those spurious bumps starve
// convergence long enough to push joiners into the WeaklyUp fallback and
// destabilize the JVM SBR (observed live: divfix-v1 DownAll collapse).
func TestUpdateReachability_ReachableNeverCreatesRecords(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	b := makeUAWithDC("127.0.0.1", 2551, 51)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	bIdx := addMemberWithStatus(cm, b, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, map[string]int64{cm.localHashString(): 1})
	clockBefore := resolvedClock(cm, cm.State)[cm.localHashString()]

	cm.updateReachability(bIdx, gproto_cluster.ReachabilityStatus_Reachable)

	records := len(cm.State.Overview.GetObserverReachability())
	clockAfter := resolvedClock(cm, cm.State)[cm.localHashString()]
	cm.Mu.Unlock()

	if records != 0 {
		t.Errorf("updateReachability(Reachable) created %d observer record(s) — Pekko's reachability table stays EMPTY while all nodes are reachable; records are created only on unreachability", records)
	}
	if clockAfter != clockBefore {
		t.Errorf("updateReachability(Reachable) bumped the vector clock (%d -> %d) with no actual reachability change — spurious bumps reset Seen and starve convergence", clockBefore, clockAfter)
	}

	// The genuine transition path must keep working: Unreachable creates a
	// record (+bump), and Reachable then flips it back (+bump).
	cm.Mu.Lock()
	cm.updateReachability(bIdx, gproto_cluster.ReachabilityStatus_Unreachable)
	cm.updateReachability(bIdx, gproto_cluster.ReachabilityStatus_Reachable)
	var status gproto_cluster.ReachabilityStatus
	found := false
	for _, r := range cm.State.Overview.GetObserverReachability() {
		for _, s := range r.GetSubjectReachability() {
			if s.GetAddressIndex() == bIdx {
				status = s.GetStatus()
				found = true
			}
		}
	}
	cm.Mu.Unlock()
	if !found || status != gproto_cluster.ReachabilityStatus_Reachable {
		t.Errorf("Unreachable -> Reachable transition broken (found=%v status=%v) — the guard must only prevent CREATION of Reachable records, not the recovery flip", found, status)
	}
}

// TestProcessIncomingGossip_TalksBackWhenSenderHasNotSeenOurState pins
// Pekko's receiveGossip talkback (ClusterDaemon.scala:1214-1218): after
// ingesting gossip, the receiver replies with its own (seen-marked) state
// to the sender whenever the sender's view differed — always on merge and
// on older-remote, and on newer/same-remote exactly when the incoming
// gossip does not carry the receiver's seen-mark. Without talkback, seen
// acknowledgements only propagate on the periodic gossip tick, and
// re-convergence after every leader action takes many seconds instead of
// one round trip — long enough to starve Joining -> Up promotion into the
// WeaklyUp fallback under version-scoped Seen semantics.
func TestProcessIncomingGossip_TalksBackWhenSenderHasNotSeenOurState(t *testing.T) {
	type routed struct {
		path string
		msg  any
	}
	var sent []routed
	router := func(_ context.Context, path string, msg any) error {
		sent = append(sent, routed{path, msg})
		return nil
	}
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)

	r := makeUAWithDC("127.0.0.1", 2551, 51)
	rName := pekkoVclockNodeHash(r)

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	addMemberWithStatus(cm, r, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, map[string]int64{cm.localHashString(): 1})
	cm.Mu.Unlock()

	// Remote sends a strictly newer gossip whose Seen does NOT include us.
	remote := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{r, local},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(2)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{Seen: []int32{0}},
	}
	setClock(remote, map[string]int64{cm.localHashString(): 1, rName: 2})

	if err := cm.processIncomingGossip(remote, r); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	var talkbacks int
	for _, s := range sent {
		if _, ok := s.msg.(*gproto_cluster.GossipEnvelope); ok {
			talkbacks++
		}
	}
	if talkbacks == 0 {
		t.Fatalf("no GossipEnvelope talkback sent to the sender after ingesting newer gossip that lacked our seen-mark — Pekko's receiveGossip replies via gossipTo(from) so acknowledgements propagate in one round trip (talkback rule, ClusterDaemon.scala:1126-1134,1214-1218)")
	}

	// Same-version gossip that ALREADY carries our seen-mark must NOT
	// trigger a reply (Pekko: talkback = !remoteGossip.seenByNode(self)).
	sent = nil
	remote2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{r, local},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(2)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{Seen: []int32{0, 1}},
	}
	cm.Mu.RLock()
	sameClock := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	cm.Mu.RUnlock()
	setClock(remote2, sameClock)

	if err := cm.processIncomingGossip(remote2, r); err != nil {
		t.Fatalf("processIncomingGossip(same): %v", err)
	}
	for _, s := range sent {
		if _, ok := s.msg.(*gproto_cluster.GossipEnvelope); ok {
			t.Errorf("talkback sent for same-version gossip that already carried our seen-mark — Pekko suppresses it (talkback = !seenByNode(self)); this would loop gossip forever between converged peers")
		}
	}
}

// TestSameVersionGossip_UnionsSeen pins the ONE case where unioning Seen is
// Pekko's own behavior (mergeSeen, ClusterDaemon.scala:1124-1127): when the
// two gossips share the SAME vector-clock version, both Seen sets
// acknowledge the identical state and the union is sound. This must keep
// working after the merge/bump reset fixes — it is how acknowledgements
// flow back to the leader for convergence.
func TestSameVersionGossip_UnionsSeen(t *testing.T) {
	router := func(_ context.Context, _ string, _ any) error { return nil }
	local := makeUAWithDC("127.0.0.1", 2541, 41)
	cm := NewClusterManager(local, router)
	lName := cm.localHashString()

	r := makeUAWithDC("127.0.0.1", 2551, 51)
	rName := pekkoVclockNodeHash(r)
	clock := map[string]int64{lName: 1, rName: 1}

	cm.Mu.Lock()
	cm.State.Members[0].Status = gproto_cluster.MemberStatus_Up.Enum()
	rIdx := addMemberWithStatus(cm, r, gproto_cluster.MemberStatus_Up, 2)
	setClock(cm.State, clock)
	cm.State.Overview = &gproto_cluster.GossipOverview{Seen: []int32{0}}
	cm.Mu.Unlock()

	remote := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{r, local},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(2)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{Seen: []int32{0}},
	}
	setClock(remote, clock)

	if err := cm.processIncomingGossip(remote, r); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	defer cm.Mu.RUnlock()
	got := map[int32]bool{}
	for _, s := range cm.State.Overview.GetSeen() {
		got[s] = true
	}
	if !got[0] || !got[rIdx] {
		t.Errorf("Seen after same-version gossip = %v, want both self (0) and the remote acknowledger (%d) — Pekko's Same case unions via mergeSeen", cm.State.Overview.GetSeen(), rIdx)
	}
}
