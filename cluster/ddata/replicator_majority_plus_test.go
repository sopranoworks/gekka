/*
 * replicator_majority_plus_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"math"
	"testing"
)

// Phase 8 — sharding coordinator-state read/write-majority-plus.
//
// Pekko's coordinator state is written/read with a "majority + N" quorum
// where N is taken from
//   pekko.cluster.sharding.coordinator-state.write-majority-plus
//   pekko.cluster.sharding.coordinator-state.read-majority-plus
// The "all" sentinel maps to math.MaxInt and clamps the quorum to the full
// cluster size. The tests below pin the helper + gossip-fanout behaviour for
// the new WriteMajorityPlus / ReadMajorityPlus consistency variants.

func TestWriteMajorityPlus_ZeroEqualsWriteMajority(t *testing.T) {
	if WriteMajorityPlus(0) != WriteMajority {
		t.Errorf("WriteMajorityPlus(0) must equal WriteMajority (no extra nodes)")
	}
	if ReadMajorityPlus(0) != WriteMajority {
		t.Errorf("ReadMajorityPlus(0) must equal WriteMajority (read-side mirrors)")
	}
}

func TestEffectiveMajorityQuorumPlus_AddsAdditional(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 0

	cases := []struct {
		numPeers int
		plus     int
		want     int
	}{
		// 7-node cluster, plus=3 → ceil(7/2) + 3 = 4 + 3 = 7 (all)
		{6, 3, 7},
		// 5-node cluster, plus=1 → 3 + 1 = 4
		{4, 1, 4},
		// 11-node cluster, plus=2 → 6 + 2 = 8
		{10, 2, 8},
		// plus=0 collapses to natural majority
		{6, 0, 4},
	}
	for _, tc := range cases {
		got := r.EffectiveMajorityQuorumPlus(tc.numPeers, tc.plus)
		if got != tc.want {
			t.Errorf("EffectiveMajorityQuorumPlus(numPeers=%d, plus=%d) = %d, want %d",
				tc.numPeers, tc.plus, got, tc.want)
		}
	}
}

func TestEffectiveMajorityQuorumPlus_AllSentinelClampsToTotal(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 0

	// math.MaxInt represents the "all" sentinel.
	cases := []struct {
		numPeers int
		want     int
	}{
		{0, 1},  // total=1
		{6, 7},  // total=7
		{10, 11},
	}
	for _, tc := range cases {
		got := r.EffectiveMajorityQuorumPlus(tc.numPeers, math.MaxInt)
		if got != tc.want {
			t.Errorf("EffectiveMajorityQuorumPlus(numPeers=%d, plus=MaxInt) = %d, want %d (cluster size)",
				tc.numPeers, got, tc.want)
		}
	}
}

func TestEffectiveMajorityQuorumPlus_RespectsMinCap(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 5

	// total=7, plus=0, natural majority=4 — MajorityMinCap=5 raises it to 5.
	if got := r.EffectiveMajorityQuorumPlus(6, 0); got != 5 {
		t.Errorf("EffectiveMajorityQuorumPlus(6, 0) MinCap=5 = %d, want 5", got)
	}
	// total=7, plus=1 → 4+1=5 — equals MinCap.
	if got := r.EffectiveMajorityQuorumPlus(6, 1); got != 5 {
		t.Errorf("EffectiveMajorityQuorumPlus(6, 1) MinCap=5 = %d, want 5", got)
	}
	// total=7, plus=3 → 4+3=7 — exceeds MinCap, capped at total.
	if got := r.EffectiveMajorityQuorumPlus(6, 3); got != 7 {
		t.Errorf("EffectiveMajorityQuorumPlus(6, 3) MinCap=5 = %d, want 7", got)
	}
}

func TestSelectGossipTargets_WriteMajorityPlus_Fanout(t *testing.T) {
	peers := make([]peerInfo, 6)
	for i := range peers {
		peers[i] = peerInfo{path: ""}
	}
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true
	r.MajorityMinCap = 0

	// 7-node cluster (6 peers + self), plus=3 → quorum=7, fanout=6 (all peers).
	got := r.selectGossipTargets(peers, WriteMajorityPlus(3))
	if len(got) != 6 {
		t.Errorf("WriteMajorityPlus(3) fanout on 7-node = %d, want 6", len(got))
	}

	// plus=1 → quorum=5, fanout=4.
	got = r.selectGossipTargets(peers, WriteMajorityPlus(1))
	if len(got) != 4 {
		t.Errorf("WriteMajorityPlus(1) fanout on 7-node = %d, want 4", len(got))
	}

	// plus=0 must collapse to plain WriteMajority (fanout=3 on 7-node).
	got = r.selectGossipTargets(peers, WriteMajorityPlus(0))
	if len(got) != 3 {
		t.Errorf("WriteMajorityPlus(0) fanout on 7-node = %d, want 3", len(got))
	}
}

func TestSelectGossipTargets_WriteMajorityPlus_AllSentinel(t *testing.T) {
	peers := make([]peerInfo, 6)
	for i := range peers {
		peers[i] = peerInfo{path: ""}
	}
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true

	// "all" — quorum clamps to total=7, fanout=6 (all peers).
	got := r.selectGossipTargets(peers, WriteMajorityPlus(math.MaxInt))
	if len(got) != 6 {
		t.Errorf(`WriteMajorityPlus("all") fanout on 7-node = %d, want 6`, len(got))
	}
}

func TestSelectGossipTargets_ReadMajorityPlus_MatchesWrite(t *testing.T) {
	peers := make([]peerInfo, 6)
	for i := range peers {
		peers[i] = peerInfo{path: ""}
	}
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true

	for _, plus := range []int{0, 1, 3, math.MaxInt} {
		w := r.selectGossipTargets(peers, WriteMajorityPlus(plus))
		rd := r.selectGossipTargets(peers, ReadMajorityPlus(plus))
		if len(w) != len(rd) {
			t.Errorf("ReadMajorityPlus(%d) fanout = %d, want %d (matches write)",
				plus, len(rd), len(w))
		}
	}
}

// IncrementCounter / AddToSet / RemoveFromSet / PutInMap must gossip on
// majority-plus the same way they do on plain WriteMajority — i.e. the
// "did we trigger a fan-out?" predicate keys off "is this a non-local
// consistency?", not on the legacy iota equality with WriteMajority.
//
// We exercise this by snapshotting the gossip-round counter through
// HandleIncoming (which fires the gossipDelta path) is overkill for a unit
// test; instead we drive selectGossipTargets directly with a non-empty peer
// list and observe the truncated fanout. The truncation only differs from
// WriteAll's full-fanout when the consistency is in the "majority" family,
// so a non-zero plus that is < natural-majority is the discriminating case.
func TestSelectGossipTargets_WriteMajorityPlusGossipTriggers(t *testing.T) {
	peers := make([]peerInfo, 6)
	for i := range peers {
		peers[i] = peerInfo{path: ""}
	}
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true

	all := r.selectGossipTargets(peers, WriteAll)
	plus := r.selectGossipTargets(peers, WriteMajorityPlus(1))
	if len(plus) >= len(all) {
		t.Errorf("WriteMajorityPlus(1) fanout=%d should be < WriteAll fanout=%d",
			len(plus), len(all))
	}
}
