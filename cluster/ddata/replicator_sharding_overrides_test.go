/*
 * replicator_sharding_overrides_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"testing"
)

// Sub-plan 8b: tests for the runtime consumers of the three sharding-side
// distributed-data overrides — `majority-min-cap`, `max-delta-elements`,
// and `prefer-oldest`. These exercise the EffectiveMajorityQuorum helper
// and the selectGossipTargets ordering / truncation policy that gives each
// override a non-test-only consumer in the gossip path.

func TestEffectiveMajorityQuorum_DefaultMinCap(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 0

	cases := []struct {
		numPeers int
		want     int // expected quorum (including self)
	}{
		{0, 1},  // total=1 → majority=1
		{1, 2},  // total=2 → majority=2
		{2, 2},  // total=3 → majority=2
		{3, 3},  // total=4 → majority=3
		{4, 3},  // total=5 → majority=3
		{6, 4},  // total=7 → majority=4
		{10, 6}, // total=11 → majority=6
	}
	for _, tc := range cases {
		if got := r.EffectiveMajorityQuorum(tc.numPeers); got != tc.want {
			t.Errorf("EffectiveMajorityQuorum(%d) = %d, want %d", tc.numPeers, got, tc.want)
		}
	}
}

func TestEffectiveMajorityQuorum_FloorClampedToTotal(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 100

	// total=4 (3 peers + self) — MajorityMinCap=100 should clamp to 4.
	if got := r.EffectiveMajorityQuorum(3); got != 4 {
		t.Errorf("EffectiveMajorityQuorum(3) with MajorityMinCap=100 = %d, want 4 (clamped)", got)
	}

	// total=1 (no peers) — clamp to 1.
	if got := r.EffectiveMajorityQuorum(0); got != 1 {
		t.Errorf("EffectiveMajorityQuorum(0) with MajorityMinCap=100 = %d, want 1", got)
	}
}

func TestEffectiveMajorityQuorum_FloorRaisesQuorum(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.MajorityMinCap = 5

	// total=10 → natural majority=6. MajorityMinCap=5 < 6 → returns 6.
	if got := r.EffectiveMajorityQuorum(9); got != 6 {
		t.Errorf("EffectiveMajorityQuorum(9) with MajorityMinCap=5 = %d, want 6 (natural majority wins)", got)
	}

	// total=6 → natural majority=4. MajorityMinCap=5 > 4 → returns 5.
	if got := r.EffectiveMajorityQuorum(5); got != 5 {
		t.Errorf("EffectiveMajorityQuorum(5) with MajorityMinCap=5 = %d, want 5 (floor wins)", got)
	}

	// total=4 → natural majority=3. MajorityMinCap=5 > 4 → clamps to 4.
	if got := r.EffectiveMajorityQuorum(3); got != 4 {
		t.Errorf("EffectiveMajorityQuorum(3) with MajorityMinCap=5 = %d, want 4 (clamped to total)", got)
	}
}

func TestSelectGossipTargets_PreferOldestOrdering(t *testing.T) {
	peers := []peerInfo{
		{path: "pekko://Sys@host:1/user/r"}, // oldest (added first)
		{path: "pekko://Sys@host:2/user/r"},
		{path: "pekko://Sys@host:3/user/r"}, // youngest (added last)
	}

	// PreferOldest=true → insertion order preserved.
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true
	got := r.selectGossipTargets(peers, WriteAll)
	if len(got) != 3 {
		t.Fatalf("got %d targets, want 3", len(got))
	}
	for i, p := range peers {
		if got[i].path != p.path {
			t.Errorf("PreferOldest=true target[%d] = %q, want %q", i, got[i].path, p.path)
		}
	}

	// PreferOldest=false → reversed (youngest-first).
	r.PreferOldest = false
	got = r.selectGossipTargets(peers, WriteAll)
	if len(got) != 3 {
		t.Fatalf("got %d targets, want 3", len(got))
	}
	for i, p := range peers {
		want := peers[len(peers)-1-i].path
		_ = p
		if got[i].path != want {
			t.Errorf("PreferOldest=false target[%d] = %q, want %q", i, got[i].path, want)
		}
	}
}

func TestSelectGossipTargets_WriteMajorityTruncation(t *testing.T) {
	peers := make([]peerInfo, 6)
	for i := range peers {
		peers[i] = peerInfo{path: ""}
	}
	r := NewReplicator("node-1", nil)
	r.PreferOldest = true

	// MajorityMinCap=0 → natural majority on total=7 is 4; fanout=3.
	r.MajorityMinCap = 0
	got := r.selectGossipTargets(peers, WriteMajority)
	if len(got) != 3 {
		t.Errorf("WriteMajority fanout (MajorityMinCap=0) = %d, want 3", len(got))
	}

	// MajorityMinCap=5 raises quorum from natural=4 → 5; fanout=4.
	r.MajorityMinCap = 5
	got = r.selectGossipTargets(peers, WriteMajority)
	if len(got) != 4 {
		t.Errorf("WriteMajority fanout (MajorityMinCap=5, total=7) = %d, want 4", len(got))
	}

	// MajorityMinCap=6 → quorum=6; fanout=5.
	r.MajorityMinCap = 6
	got = r.selectGossipTargets(peers, WriteMajority)
	if len(got) != 5 {
		t.Errorf("WriteMajority fanout (MajorityMinCap=6) = %d, want 5", len(got))
	}

	// MajorityMinCap=100 → clamped to total=7; fanout=6 (all peers).
	r.MajorityMinCap = 100
	got = r.selectGossipTargets(peers, WriteMajority)
	if len(got) != 6 {
		t.Errorf("WriteMajority fanout (MajorityMinCap=100, clamped) = %d, want 6 (all peers)", len(got))
	}

	// WriteAll always returns all peers regardless of MajorityMinCap.
	r.MajorityMinCap = 6
	got = r.selectGossipTargets(peers, WriteAll)
	if len(got) != 6 {
		t.Errorf("WriteAll fanout = %d, want 6 (all peers)", len(got))
	}

	// WriteLocal also returns all (caller decides whether to gossip).
	got = r.selectGossipTargets(peers, WriteLocal)
	if len(got) != 6 {
		t.Errorf("WriteLocal fanout = %d, want 6 (all peers)", len(got))
	}
}
