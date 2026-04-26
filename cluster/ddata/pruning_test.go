/*
 * pruning_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"testing"
	"time"
)

func TestGCounter_Prune(t *testing.T) {
	gc := NewGCounter()
	gc.Increment("node1", 5)
	gc.Increment("node2", 3)
	gc.Increment("node3", 7)

	if !gc.NeedsPruning("node2") {
		t.Fatal("node2 should need pruning before Prune")
	}

	gc.Prune("node2", "node1")

	if gc.NeedsPruning("node2") {
		t.Error("node2 should not need pruning after Prune")
	}
	// node1 absorbed node2's value: 5 + 3 = 8. Total: 8 + 7 = 15.
	if v := gc.Value(); v != 15 {
		t.Errorf("Value = %d, want 15", v)
	}
	if v := gc.NodeValue("node1"); v != 8 {
		t.Errorf("node1 = %d, want 8", v)
	}
	if v := gc.NodeValue("node2"); v != 0 {
		t.Errorf("node2 = %d, want 0 after prune", v)
	}
}

func TestGCounter_Prune_IdempotentWithNoOp(t *testing.T) {
	gc := NewGCounter()
	gc.Increment("node1", 5)

	// removed == collapseInto is a no-op
	gc.Prune("node1", "node1")
	if v := gc.Value(); v != 5 {
		t.Errorf("Value = %d, want 5 after self-prune", v)
	}

	// pruning an unknown node is a no-op
	gc.Prune("unknown", "node1")
	if v := gc.Value(); v != 5 {
		t.Errorf("Value = %d, want 5 after unknown-prune", v)
	}
}

func TestORSet_Prune(t *testing.T) {
	os := NewORSet()
	os.Add("node1", "a")
	os.Add("node2", "b")
	os.Add("node2", "c")

	if !os.NeedsPruning("node2") {
		t.Fatal("node2 should need pruning before Prune")
	}

	os.Prune("node2", "node1")

	if os.NeedsPruning("node2") {
		t.Error("node2 should not need pruning after Prune")
	}

	// All three elements should still be present.
	for _, want := range []string{"a", "b", "c"} {
		if !os.Contains(want) {
			t.Errorf("element %q missing after prune", want)
		}
	}
}

func TestPNCounter_Prune(t *testing.T) {
	pn := NewPNCounter()
	pn.Increment("node1", 10)
	pn.Increment("node2", 5)
	pn.Decrement("node2", 2)

	pn.Prune("node2", "node1")

	if pn.NeedsPruning("node2") {
		t.Error("node2 still needs pruning after Prune")
	}
	// Observable value preserved: 10 + 5 - 2 = 13
	if v := pn.Value(); v != 13 {
		t.Errorf("Value = %d, want 13", v)
	}
}

func TestPruningManager_LifecycleOnOldestNode(t *testing.T) {
	pm := NewPruningManager(
		50*time.Millisecond,
		150*time.Millisecond,
		func() bool { return true },         // this node is the oldest
		func() string { return "survivor" }, // collapse into "survivor"
	)

	gc := NewGCounter()
	gc.Increment("removed-node", 10)
	gc.Increment("survivor", 5)

	pm.NodeRemoved("removed-node")

	// First tick initiates pruning.
	prunables := map[string]Prunable{"counter": gc}
	pm.Tick(prunables)

	if gc.NeedsPruning("removed-node") {
		t.Error("counter should have been pruned on first Tick")
	}
	if v := gc.Value(); v != 15 {
		t.Errorf("Value = %d, want 15", v)
	}

	// Before dissemination expires, the node stays tracked.
	if tracked := pm.TrackedRemovedNodes(); len(tracked) != 1 {
		t.Errorf("tracked = %v, want one entry still in dissemination", tracked)
	}

	// After the dissemination window elapses, the tombstone is collected.
	time.Sleep(170 * time.Millisecond)
	pm.Tick(prunables)
	if tracked := pm.TrackedRemovedNodes(); len(tracked) != 0 {
		t.Errorf("tracked = %v, want empty after dissemination window", tracked)
	}
}

func TestPruningManager_NonOldestIsNoOp(t *testing.T) {
	pm := NewPruningManager(
		10*time.Millisecond,
		50*time.Millisecond,
		func() bool { return false }, // NOT the oldest
		func() string { return "survivor" },
	)

	gc := NewGCounter()
	gc.Increment("removed-node", 10)

	pm.NodeRemoved("removed-node")
	pm.Tick(map[string]Prunable{"counter": gc})

	// Non-oldest nodes must not rewrite state — they wait for gossip.
	if !gc.NeedsPruning("removed-node") {
		t.Error("non-oldest node must not initiate pruning")
	}
}

func TestPruningManager_NodeRemovedIsIdempotent(t *testing.T) {
	pm := NewPruningManager(0, 0, nil, nil)
	pm.NodeRemoved("a")
	pm.NodeRemoved("a")
	pm.NodeRemoved("")
	if tracked := pm.TrackedRemovedNodes(); len(tracked) != 1 {
		t.Errorf("tracked = %v, want exactly [a]", tracked)
	}
}

// TestPruningManager_PruningMarkerTimeToLive verifies that when
// pruning-marker-time-to-live is wired, a tombstone is retained in the
// PruningComplete phase until the TTL elapses (measured from InitiatedAt).
// This is the round-2 session 16 acceptance behavior for
// `pekko.cluster.distributed-data.pruning-marker-time-to-live`.
func TestPruningManager_PruningMarkerTimeToLive(t *testing.T) {
	pm := NewPruningManager(
		20*time.Millisecond, // pruningInterval (informational)
		30*time.Millisecond, // maxPruningDissemination — short
		func() bool { return true },
		func() string { return "survivor" },
	)
	pm.SetPruningMarkerTimeToLive(120 * time.Millisecond)

	gc := NewGCounter()
	gc.Increment("removed-node", 4)
	gc.Increment("survivor", 1)
	pm.NodeRemoved("removed-node")

	// First Tick initiates pruning (rewrites onto survivor).
	prunables := map[string]Prunable{"counter": gc}
	pm.Tick(prunables)
	if gc.NeedsPruning("removed-node") {
		t.Fatal("counter should have been pruned on first Tick")
	}

	// After dissemination expires, the marker MUST stay tracked because the
	// per-marker TTL has not yet elapsed.
	time.Sleep(45 * time.Millisecond)
	pm.Tick(prunables)
	if got := pm.TrackedRemovedNodes(); len(got) != 1 {
		t.Errorf("after dissemination only: tracked = %v, want still tracked under TTL", got)
	}

	// After the TTL window measured from InitiatedAt, the marker is forgotten.
	time.Sleep(120 * time.Millisecond)
	pm.Tick(prunables)
	if got := pm.TrackedRemovedNodes(); len(got) != 0 {
		t.Errorf("after TTL: tracked = %v, want empty", got)
	}
}
