/*
 * vector_clock_identity_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cluster

// These tests pin gekka's vector-clock identity and merge semantics to
// Pekko's protocol (the source of truth):
//
//   - Pekko's wire field allHashes is gossip.version.versions.keys — the
//     vector clock's NODE-NAME table (MD5 hex of vclockName), with NO
//     positional relationship to allAddresses (ClusterMessageSerializer.
//     gossipToProto).
//   - A node's clock-node name is MD5(s"${address}-${longUid}") computed
//     from its OWN identity (Gossip.vclockName / VectorClock.Node) — never
//     adopted from another gossip's hash table by position.
//   - Gossip.merge unions concurrent branches with a string-keyed
//     pairwise-max clock; receiveGossip's merge case does NOT bump the
//     local component (`:+ vclockNode` happens only in updateLatestGossip
//     on genuine local state changes).
//
// The 2026-07-13 join-time member flicker (findings/2026-07-13-open-member-
// flicker-on-join.md) was caused by breaking the second rule: gekka adopted
// AllHashes[index-of-own-address] — usually ANOTHER member's clock node —
// as its own identity and then bumped that member's component on every
// local change, fabricating causal history that let a stale member set
// spuriously dominate a genuinely newer concurrent branch.

import (
	"context"
	"testing"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"google.golang.org/protobuf/proto"
)

func vcTestUA(system, host string, port uint32, longUid uint64) *gproto_cluster.UniqueAddress {
	return &gproto_cluster.UniqueAddress{
		Address: &gproto_cluster.Address{
			Protocol: proto.String("pekko"),
			System:   proto.String(system),
			Hostname: proto.String(host),
			Port:     proto.Uint32(port),
		},
		Uid:  proto.Uint32(uint32(longUid & 0xffffffff)),
		Uid2: proto.Uint32(uint32(longUid >> 32)),
	}
}

// TestLocalClockNodeName_MatchesPekkoVclockName pins gekka's self clock-node
// name to Pekko's exact scheme: MD5(s"${address}-${longUid}") where address
// renders as protocol://system@host:port and longUid is the SIGNED 64-bit
// decimal. The expected literal is cross-validated against a live Pekko
// 1.1.2 run: in showcase-diag-v3-20260713, Pekko node s1
// (pekko://ShowcaseCluster@127.0.0.1:2551, UID -3105663664294864687) put
// exactly this hash in its gossip's allHashes table.
func TestLocalClockNodeName_MatchesPekkoVclockName(t *testing.T) {
	// unsigned representation of -3105663664294864687
	const s1Uid = uint64(15341080409414686929)
	local := vcTestUA("ShowcaseCluster", "127.0.0.1", 2551, s1Uid)
	cm := NewClusterManager(local, func(context.Context, string, any) error { return nil })

	const want = "cf2dd0263fd83142376efa1d3980039b"
	if got := cm.localHashString(); got != want {
		t.Fatalf("local clock-node name = %q, want Pekko vclockName MD5 %q", got, want)
	}
}

// TestNoAdoptionOfPositionalHash verifies that processing an incoming gossip
// NEVER changes the local node's clock-node identity. Pekko's allHashes is
// ordered by the clock's TreeMap (sorted MD5 strings), so the entry at the
// local address's POSITION belongs to an arbitrary — usually different —
// member; adopting it means bumping that member's clock component from now
// on (the flicker's root cause).
func TestNoAdoptionOfPositionalHash(t *testing.T) {
	local := vcTestUA("TestSystem", "127.0.0.1", 7355, 42)
	cm := NewClusterManager(local, func(context.Context, string, any) error { return nil })
	selfNode := cm.localHashString()

	seed := vcTestUA("TestSystem", "127.0.0.1", 2551, 77)
	const foreignHash = "9fb06f12964ac10faa227d0ef14b3ca5" // some other node's clock node

	// Pekko-style incoming gossip: our address sits at index 1, and the
	// hash table's index-1 entry is a DIFFERENT node's clock node.
	incoming := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{seed, local},
		AllHashes:    []string{"cf2dd0263fd83142376efa1d3980039b", foreignHash},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Joining.Enum(), UpNumber: proto.Int32(0)},
		},
		Overview: &gproto_cluster.GossipOverview{},
		Version: &gproto_cluster.VectorClock{
			Versions: []*gproto_cluster.VectorClock_Version{
				{HashIndex: proto.Int32(0), Timestamp: proto.Int64(3)},
				{HashIndex: proto.Int32(1), Timestamp: proto.Int64(2)},
			},
		},
	}

	// Incoming strictly dominates our empty clock → wholesale adoption path.
	if err := cm.processIncomingGossip(incoming, seed); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}
	if got := cm.localHashString(); got != selfNode {
		t.Fatalf("local clock-node identity changed after gossip: %q → %q (positional adoption)", selfNode, got)
	}
	if cm.localHashString() == foreignHash {
		t.Fatalf("local node adopted another member's clock node %q", foreignHash)
	}

	// A concurrent gossip must not trigger adoption either.
	concurrent := proto.Clone(incoming).(*gproto_cluster.Gossip)
	concurrent.Version = &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(0), Timestamp: proto.Int64(1)},
		},
	}
	cm.Mu.Lock()
	cm.State.Version = &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(0), Timestamp: proto.Int64(9)},
		},
	}
	cm.State.AllHashes = []string{selfNode}
	cm.Mu.Unlock()
	if err := cm.processIncomingGossip(concurrent, seed); err != nil {
		t.Fatalf("processIncomingGossip (concurrent): %v", err)
	}
	if got := cm.localHashString(); got != selfNode {
		t.Fatalf("local clock-node identity changed after concurrent merge: %q → %q", selfNode, got)
	}
}

// TestConcurrentGossipMerge_NoSelfBump pins Pekko's receiveGossip merge
// semantics: merging concurrent branches produces the string-keyed
// pairwise-max clock and NOTHING ELSE — no self-component bump. Bumping on
// merge makes the merged state spuriously dominate every peer's concurrent
// branch instead of remaining mergeable.
func TestConcurrentGossipMerge_NoSelfBump(t *testing.T) {
	local := vcTestUA("TestSystem", "127.0.0.1", 7355, 42)
	peer := vcTestUA("TestSystem", "127.0.0.1", 2551, 77)
	cm := NewClusterManager(local, func(context.Context, string, any) error { return nil })
	selfNode := cm.localHashString()
	const peerNode = "cf2dd0263fd83142376efa1d3980039b"

	cm.Mu.Lock()
	cm.State.AllAddresses = append(cm.State.AllAddresses, peer)
	cm.State.Members = append(cm.State.Members, &gproto_cluster.Member{
		AddressIndex: proto.Int32(1),
		Status:       gproto_cluster.MemberStatus_Up.Enum(),
		UpNumber:     proto.Int32(1),
	})
	cm.State.AllHashes = []string{selfNode}
	cm.State.Version = &gproto_cluster.VectorClock{
		Versions: []*gproto_cluster.VectorClock_Version{
			{HashIndex: proto.Int32(0), Timestamp: proto.Int64(1)},
		},
	}
	cm.Mu.Unlock()

	incoming := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{peer, local},
		AllHashes:    []string{peerNode},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Joining.Enum(), UpNumber: proto.Int32(0)},
		},
		Overview: &gproto_cluster.GossipOverview{},
		Version: &gproto_cluster.VectorClock{
			Versions: []*gproto_cluster.VectorClock_Version{
				{HashIndex: proto.Int32(0), Timestamp: proto.Int64(1)},
			},
		},
	}

	// {selfNode:1} vs {peerNode:1} → concurrent → merge.
	if err := cm.processIncomingGossip(incoming, peer); err != nil {
		t.Fatalf("processIncomingGossip: %v", err)
	}

	cm.Mu.RLock()
	got := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	cm.Mu.RUnlock()
	want := map[string]int64{selfNode: 1, peerNode: 1}
	if len(got) != len(want) || got[selfNode] != want[selfNode] || got[peerNode] != want[peerNode] {
		t.Fatalf("merged clock = %v, want exact pairwise max %v (no self bump on merge)", got, want)
	}
}

// TestMergeGossipStates_StringKeyedUnion pins the merge's clock
// reconstruction: every component of both branches must survive, keyed by
// its hash STRING. The pre-fix positional rebuild dropped components whose
// hash string was absent from the merged (address-parallel) table —
// destroying causal history and mis-ordering subsequent comparisons.
func TestMergeGossipStates_StringKeyedUnion(t *testing.T) {
	local := vcTestUA("TestSystem", "127.0.0.1", 7355, 42)
	cm := NewClusterManager(local, func(context.Context, string, any) error { return nil })

	peer := vcTestUA("TestSystem", "127.0.0.1", 2551, 77)
	const hA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const hX = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1"
	const hY = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy2"

	s1 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local},
		AllHashes:    []string{hA},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
		},
		Overview: &gproto_cluster.GossipOverview{},
		Version: &gproto_cluster.VectorClock{
			Versions: []*gproto_cluster.VectorClock_Version{
				{HashIndex: proto.Int32(0), Timestamp: proto.Int64(2)},
			},
		},
	}
	// Pekko-style branch: clock nodes hX, hY — NEITHER positionally aligned
	// with its two addresses.
	s2 := &gproto_cluster.Gossip{
		AllAddresses: []*gproto_cluster.UniqueAddress{local, peer},
		AllHashes:    []string{hX, hY},
		Members: []*gproto_cluster.Member{
			{AddressIndex: proto.Int32(0), Status: gproto_cluster.MemberStatus_Up.Enum(), UpNumber: proto.Int32(1)},
			{AddressIndex: proto.Int32(1), Status: gproto_cluster.MemberStatus_Joining.Enum(), UpNumber: proto.Int32(0)},
		},
		Overview: &gproto_cluster.GossipOverview{},
		Version: &gproto_cluster.VectorClock{
			Versions: []*gproto_cluster.VectorClock_Version{
				{HashIndex: proto.Int32(0), Timestamp: proto.Int64(4)},
				{HashIndex: proto.Int32(1), Timestamp: proto.Int64(7)},
			},
		},
	}

	merged := cm.mergeGossipStates(s1, s2)

	got := cm.vectorClockToMap(merged.Version, merged.AllHashes)
	want := map[string]int64{hA: 2, hX: 4, hY: 7}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("merged clock = %v, want %v (component %s:%d dropped or misattached)", got, want, k[:8], v)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("merged clock = %v has %d components, want %d", got, len(got), len(want))
	}

	// A correct union strictly dominates both parents.
	m1 := cm.vectorClockToMap(s1.Version, s1.AllHashes)
	m2 := cm.vectorClockToMap(s2.Version, s2.AllHashes)
	if ord := cm.compareResolvedClocks(got, m1); ord != ClockAfter {
		t.Fatalf("merged vs s1 ordering = %v, want After", ord)
	}
	if ord := cm.compareResolvedClocks(got, m2); ord != ClockAfter {
		t.Fatalf("merged vs s2 ordering = %v, want After", ord)
	}

	// Member union must contain both members.
	if len(merged.Members) != 2 {
		t.Fatalf("merged members = %d, want 2", len(merged.Members))
	}
}

// TestIncrementVersion_InsertsSelfNode pins Pekko's `VectorClock :+ node`
// semantics: bumping inserts the node's component when absent. The pre-fix
// rebuild silently DROPPED the bump when the local hash was missing from
// AllHashes, leaving state changes causally invisible.
func TestIncrementVersion_InsertsSelfNode(t *testing.T) {
	local := vcTestUA("TestSystem", "127.0.0.1", 7355, 42)
	cm := NewClusterManager(local, func(context.Context, string, any) error { return nil })

	cm.Mu.Lock()
	cm.incrementVersionWithLockHeld()
	got := cm.vectorClockToMap(cm.State.Version, cm.State.AllHashes)
	cm.Mu.Unlock()

	selfNode := cm.localHashString()
	if got[selfNode] != 1 || len(got) != 1 {
		t.Fatalf("clock after first increment = %v, want {%q:1}", got, selfNode)
	}
}
