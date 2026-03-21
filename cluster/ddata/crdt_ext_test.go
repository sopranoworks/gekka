/*
 * crdt_ext_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

// ── PNCounter ────────────────────────────────────────────────────────────────

import (
	"encoding/json"
	"testing"
	"time"
)

// gossipPNCounterDirect pushes src's PNCounter at key to dst via HandleIncoming.
func gossipPNCounterDirect(t *testing.T, src, dst *Replicator, key string) {
	t.Helper()
	c := src.PNCounter(key)
	payload, err := json.Marshal(PNCounterPayload{c.Snapshot()})
	if err != nil {
		t.Fatalf("gossipPNCounter marshal: %v", err)
	}
	raw, err := json.Marshal(ReplicatorMsg{Type: "pncounter-gossip", Key: key, Payload: payload})
	if err != nil {
		t.Fatalf("gossipPNCounter envelope: %v", err)
	}
	if err := dst.HandleIncoming(raw); err != nil {
		t.Fatalf("gossipPNCounter HandleIncoming: %v", err)
	}
}

// gossipORFlagDirect pushes src's ORFlag at key to dst via HandleIncoming.
func gossipORFlagDirect(t *testing.T, src, dst *Replicator, key string) {
	t.Helper()
	f := src.ORFlag(key)
	payload, err := json.Marshal(f.Snapshot())
	if err != nil {
		t.Fatalf("gossipORFlag marshal: %v", err)
	}
	raw, err := json.Marshal(ReplicatorMsg{Type: "orflag-gossip", Key: key, Payload: payload})
	if err != nil {
		t.Fatalf("gossipORFlag envelope: %v", err)
	}
	if err := dst.HandleIncoming(raw); err != nil {
		t.Fatalf("gossipORFlag HandleIncoming: %v", err)
	}
}

// gossipLWWRegisterDirect pushes src's LWWRegister at key to dst via HandleIncoming.
func gossipLWWRegisterDirect(t *testing.T, src, dst *Replicator, key string) {
	t.Helper()
	reg := src.LWWRegister(key)
	payload, err := json.Marshal(LWWRegisterPayload{reg.Snapshot()})
	if err != nil {
		t.Fatalf("gossipLWWRegister marshal: %v", err)
	}
	raw, err := json.Marshal(ReplicatorMsg{Type: "lwwregister-gossip", Key: key, Payload: payload})
	if err != nil {
		t.Fatalf("gossipLWWRegister envelope: %v", err)
	}
	if err := dst.HandleIncoming(raw); err != nil {
		t.Fatalf("gossipLWWRegister HandleIncoming: %v", err)
	}
}

// ── PNCounter tests ──────────────────────────────────────────────────────────

// TestPNCounter_BasicIncrDecr verifies that a single node's increments and
// decrements produce the correct signed net value.
func TestPNCounter_BasicIncrDecr(t *testing.T) {
	c := NewPNCounter()
	c.Increment("A", 10)
	c.Increment("A", 5)
	c.Decrement("A", 3)
	if got := c.Value(); got != 12 {
		t.Errorf("expected 12, got %d", got)
	}
}

// TestPNCounter_CanGoNegative ensures the value is negative when decrements
// exceed increments.
func TestPNCounter_CanGoNegative(t *testing.T) {
	c := NewPNCounter()
	c.Increment("A", 2)
	c.Decrement("A", 5)
	if got := c.Value(); got != -3 {
		t.Errorf("expected -3, got %d", got)
	}
}

// TestPNCounter_InterleavedMultiNode verifies eventual consistency when two
// nodes interleave increments and decrements and then gossip to each other.
//
// Scenario:
//
//	Node A: +10, -2         → local net = +8
//	Node B: +5,  -7         → local net = -2
//	After bidirectional gossip: both see net = 8 + (-2) = 6
func TestPNCounter_InterleavedMultiNode(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	rA.PNCounter("score").Increment("node-A", 10)
	rA.PNCounter("score").Decrement("node-A", 2)

	rB.PNCounter("score").Increment("node-B", 5)
	rB.PNCounter("score").Decrement("node-B", 7)

	// Pre-gossip isolation: each node only sees its own writes.
	if got := rA.PNCounter("score").Value(); got != 8 {
		t.Errorf("pre-gossip A: expected 8, got %d", got)
	}
	if got := rB.PNCounter("score").Value(); got != -2 {
		t.Errorf("pre-gossip B: expected -2, got %d", got)
	}

	// Bidirectional gossip.
	gossipPNCounterDirect(t, rA, rB, "score")
	gossipPNCounterDirect(t, rB, rA, "score")

	// Both nodes converge to net = 10 - 2 + 5 - 7 = 6.
	for label, r := range map[string]*Replicator{"A": rA, "B": rB} {
		if got := r.PNCounter("score").Value(); got != 6 {
			t.Errorf("post-gossip %s: expected 6, got %d", label, got)
		}
	}
}

// TestPNCounter_MergeIdempotent verifies that applying the same snapshot twice
// produces the same result (idempotency).
func TestPNCounter_MergeIdempotent(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	rA.PNCounter("x").Increment("node-A", 7)
	rA.PNCounter("x").Decrement("node-A", 3)

	gossipPNCounterDirect(t, rA, rB, "x")
	gossipPNCounterDirect(t, rA, rB, "x") // duplicate — must be idempotent

	if got := rB.PNCounter("x").Value(); got != 4 {
		t.Errorf("expected 4 after idempotent merge, got %d", got)
	}
}

// ── LWWRegister tests ────────────────────────────────────────────────────────

// TestLWWRegister_BasicSetGet verifies a simple write and read.
func TestLWWRegister_BasicSetGet(t *testing.T) {
	reg := NewLWWRegister()
	reg.Set("node-A", "hello")
	v, ok := reg.Get()
	if !ok {
		t.Fatal("expected register to be set")
	}
	if v != "hello" {
		t.Errorf("expected 'hello', got %v", v)
	}
}

// TestLWWRegister_LaterTimestampWins verifies that a write with a higher
// timestamp replaces an earlier write after gossip.
func TestLWWRegister_LaterTimestampWins(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	rA.LWWRegister("leader").Set("node-A", "alpha")
	time.Sleep(time.Millisecond) // ensure B's timestamp is strictly higher
	rB.LWWRegister("leader").Set("node-B", "beta")

	// Bidirectional gossip.
	gossipLWWRegisterDirect(t, rA, rB, "leader")
	gossipLWWRegisterDirect(t, rB, rA, "leader")

	// "beta" was written last — both nodes must converge to it.
	for label, r := range map[string]*Replicator{"A": rA, "B": rB} {
		v, ok := r.LWWRegister("leader").Get()
		if !ok {
			t.Fatalf("Node %s: register not set", label)
		}
		if v != "beta" {
			t.Errorf("Node %s: expected 'beta', got %v", label, v)
		}
	}
}

// TestLWWRegister_NodeIDTieBreak verifies that when two writes share the same
// timestamp, the node with the lexicographically higher ID wins.
func TestLWWRegister_NodeIDTieBreak(t *testing.T) {
	// Inject writes at the same timestamp manually via MergeSnapshot.
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	ts := time.Now().UnixNano()
	rA.LWWRegister("tie").MergeSnapshot(LWWRegisterSnapshot{Value: "from-A", Timestamp: ts, NodeID: "node-A"})
	rB.LWWRegister("tie").MergeSnapshot(LWWRegisterSnapshot{Value: "from-B", Timestamp: ts, NodeID: "node-B"})

	gossipLWWRegisterDirect(t, rA, rB, "tie")
	gossipLWWRegisterDirect(t, rB, rA, "tie")

	// "node-B" > "node-A" lexically → "from-B" must win on both nodes.
	for label, r := range map[string]*Replicator{"A": rA, "B": rB} {
		v, ok := r.LWWRegister("tie").Get()
		if !ok {
			t.Fatalf("Node %s: register not set", label)
		}
		if v != "from-B" {
			t.Errorf("Node %s: expected 'from-B' (tie-break), got %v", label, v)
		}
	}
}

// TestLWWRegister_EarlyGossipIgnored verifies that an older snapshot received
// after a newer local write does not overwrite the local value.
func TestLWWRegister_EarlyGossipIgnored(t *testing.T) {
	reg := NewLWWRegister()
	reg.Set("node-A", "new")
	snap := reg.Snapshot()

	// Manually craft an older snapshot and merge it.
	old := LWWRegisterSnapshot{Value: "old", Timestamp: snap.Timestamp - 1, NodeID: "node-Z"}
	reg.MergeSnapshot(old)

	v, _ := reg.Get()
	if v != "new" {
		t.Errorf("expected 'new' to survive, got %v", v)
	}
}

// ── ORFlag tests ─────────────────────────────────────────────────────────────

// TestORFlag_DefaultFalse verifies that a newly created flag is false.
func TestORFlag_DefaultFalse(t *testing.T) {
	f := NewORFlag()
	if f.Value() {
		t.Error("expected new ORFlag to be false")
	}
}

// TestORFlag_SwitchOnOff verifies basic toggle behaviour on a single node.
func TestORFlag_SwitchOnOff(t *testing.T) {
	f := NewORFlag()
	f.SwitchOn("node-A")
	if !f.Value() {
		t.Error("expected flag to be true after SwitchOn")
	}
	f.SwitchOff()
	if f.Value() {
		t.Error("expected flag to be false after SwitchOff")
	}
}

// TestORFlag_ConvergenceAfterMultipleToggles simulates a real-world scenario
// where two nodes toggle the flag and then gossip to each other.
//
// Scenario:
//
//  1. Node A switches on.
//  2. Node B switches off while Node A's on-dot is already propagated to B.
//     A→B gossip occurs first; then B switches off (removes A's dot from B).
//  3. B→A gossip: A now sees B's remove and the flag is false on both.
//  4. A switches on again.
//  5. Final gossip A→B: both should see the flag as true.
func TestORFlag_ConvergenceAfterMultipleToggles(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	// Step 1: A switches on.
	rA.ORFlag("maintenance").SwitchOn("node-A")

	// Step 2: A→B gossip; B sees the flag as true.
	gossipORFlagDirect(t, rA, rB, "maintenance")
	if !rB.ORFlag("maintenance").Value() {
		t.Error("step 2: Node B should see flag=true after A→B gossip")
	}

	// Step 3: B switches off (removes A's observed dot).
	rB.ORFlag("maintenance").SwitchOff()
	gossipORFlagDirect(t, rB, rA, "maintenance")

	// Both nodes should now agree the flag is false.
	if rA.ORFlag("maintenance").Value() {
		t.Error("step 3: Node A should see flag=false after B switches off")
	}
	if rB.ORFlag("maintenance").Value() {
		t.Error("step 3: Node B should see flag=false")
	}

	// Step 4: A switches on again.
	rA.ORFlag("maintenance").SwitchOn("node-A")
	gossipORFlagDirect(t, rA, rB, "maintenance")

	// Step 5: Both nodes should converge to true.
	if !rA.ORFlag("maintenance").Value() {
		t.Error("step 5: Node A should see flag=true")
	}
	if !rB.ORFlag("maintenance").Value() {
		t.Error("step 5: Node B should see flag=true after final gossip")
	}
}

// TestORFlag_AddWinsOnConcurrentToggle verifies the add-wins property:
// a concurrent SwitchOn from one node beats a concurrent SwitchOff from
// another node after gossip converges.
func TestORFlag_AddWinsOnConcurrentToggle(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	// Both nodes start with A's flag on, already known to B.
	rA.ORFlag("feat").SwitchOn("node-A")
	gossipORFlagDirect(t, rA, rB, "feat")

	// Concurrent: A switches on again (new dot), B switches off (removes observed dots).
	// No gossip between these two operations — they happen concurrently.
	rA.ORFlag("feat").SwitchOn("node-A") // new dot — not yet known to B
	rB.ORFlag("feat").SwitchOff()        // removes only the dot B knows about

	// Now gossip in both directions.
	gossipORFlagDirect(t, rA, rB, "feat")
	gossipORFlagDirect(t, rB, rA, "feat")

	// A's new (concurrent) dot was not in B's observed set when B called
	// SwitchOff, so it survives the merge — add-wins.
	if !rA.ORFlag("feat").Value() {
		t.Error("Node A: concurrent add should win")
	}
	if !rB.ORFlag("feat").Value() {
		t.Error("Node B: concurrent add should win after gossip")
	}
}

// ── Replicator integration tests ─────────────────────────────────────────────

// TestReplicator_HandleIncoming_NewTypes verifies that HandleIncoming correctly
// dispatches pncounter-gossip, orflag-gossip, and lwwregister-gossip messages.
func TestReplicator_HandleIncoming_NewTypes(t *testing.T) {
	rA := NewReplicator("node-A", nil)
	rB := NewReplicator("node-B", nil)

	// Set up data on A.
	rA.PNCounter("hits").Increment("node-A", 42)
	rA.PNCounter("hits").Decrement("node-A", 2)
	rA.ORFlag("dark-mode").SwitchOn("node-A")
	rA.LWWRegister("version").Set("node-A", "v1.2.3")

	// Gossip everything from A to B.
	gossipPNCounterDirect(t, rA, rB, "hits")
	gossipORFlagDirect(t, rA, rB, "dark-mode")
	gossipLWWRegisterDirect(t, rA, rB, "version")

	// Verify B received all values.
	if got := rB.PNCounter("hits").Value(); got != 40 {
		t.Errorf("PNCounter: expected 40, got %d", got)
	}
	if !rB.ORFlag("dark-mode").Value() {
		t.Error("ORFlag: expected true")
	}
	v, ok := rB.LWWRegister("version").Get()
	if !ok || v != "v1.2.3" {
		t.Errorf("LWWRegister: expected v1.2.3, got %v (ok=%v)", v, ok)
	}
}
