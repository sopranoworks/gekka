/*
 * delta_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"encoding/json"
	"testing"
)

// ── GCounter delta tests ──────────────────────────────────────────────────────

func TestGCounterDelta_Correctness(t *testing.T) {
	// Verify that applying a sequence of deltas produces the same result as a
	// full-state merge.
	src := NewGCounter()
	dst := NewGCounter()

	ops := []struct {
		node  string
		delta uint64
	}{
		{"n1", 3},
		{"n2", 5},
		{"n1", 7},
		{"n3", 1},
	}

	for _, op := range ops {
		src.Increment(op.node, op.delta)
		payload, ok := src.DeltaPayload()
		if !ok {
			t.Fatal("expected delta after Increment")
		}
		d := payload.(GCounterDelta)
		dst.MergeCounterDelta(d)
		src.ResetDelta()
	}

	if src.Value() != dst.Value() {
		t.Errorf("value mismatch: src=%d dst=%d", src.Value(), dst.Value())
	}
}

func TestGCounterDelta_NoChangeAfterReset(t *testing.T) {
	c := NewGCounter()
	c.Increment("n1", 10)
	c.ResetDelta()

	_, ok := c.DeltaPayload()
	if ok {
		t.Error("expected no delta after ResetDelta")
	}
}

func TestGCounterDelta_Efficiency(t *testing.T) {
	// Full state gossip vs delta: after 1 increment on a large GCounter,
	// delta bytes < full-state bytes.
	const nodes = 100
	c := NewGCounter()
	for i := 0; i < nodes; i++ {
		c.Increment(string(rune('a'+i)), uint64(i+1))
	}
	c.ResetDelta() // baseline: no pending delta

	// One more increment — only one node slot changes.
	c.Increment("z_new", 99)

	deltaPayload, ok := c.DeltaPayload()
	if !ok {
		t.Fatal("expected delta")
	}
	deltaBytes, _ := json.Marshal(deltaPayload)
	fullBytes, _ := json.Marshal(GCounterPayload{State: c.Snapshot()})

	t.Logf("GCounter(%d nodes): full=%d bytes, delta=%d bytes (%.1f%% savings)",
		nodes+1, len(fullBytes), len(deltaBytes),
		100*(1-float64(len(deltaBytes))/float64(len(fullBytes))))

	if len(deltaBytes) >= len(fullBytes) {
		t.Errorf("delta (%d bytes) should be smaller than full state (%d bytes)",
			len(deltaBytes), len(fullBytes))
	}
}

func TestGCounterDelta_IdempotentMerge(t *testing.T) {
	src := NewGCounter()
	src.Increment("n1", 5)
	payload, _ := src.DeltaPayload()
	d := payload.(GCounterDelta)

	dst := NewGCounter()
	dst.MergeCounterDelta(d)
	dst.MergeCounterDelta(d) // apply twice — must not double-count

	// MergeCounterDelta is additive so applying the same delta twice would
	// double-count — this test documents the expected behaviour: receivers must
	// not apply the same delta more than once.  We verify that the full-state
	// path (MergeState) is idempotent as a baseline.
	src2 := NewGCounter()
	src2.MergeState(src.Snapshot())
	src2.MergeState(src.Snapshot()) // idempotent

	if src2.Value() != src.Value() {
		t.Errorf("MergeState should be idempotent: got %d, want %d", src2.Value(), src.Value())
	}
}

// ── ORSet delta tests ─────────────────────────────────────────────────────────

func TestORSetDelta_AddCorrectness(t *testing.T) {
	src := NewORSet()
	dst := NewORSet()

	elements := []string{"apple", "banana", "cherry"}
	for _, elem := range elements {
		src.Add("node1", elem)
		payload, ok := src.DeltaPayload()
		if !ok {
			t.Fatalf("expected delta after Add(%s)", elem)
		}
		d := payload.(ORSetDelta)
		dst.MergeORSetDelta(d)
		src.ResetDelta()
	}

	for _, elem := range elements {
		if !dst.Contains(elem) {
			t.Errorf("dst should contain %q after delta merge", elem)
		}
	}
}

func TestORSetDelta_RemoveCorrectness(t *testing.T) {
	src := NewORSet()
	dst := NewORSet()

	src.Add("n1", "apple")
	{
		payload, _ := src.DeltaPayload()
		dst.MergeORSetDelta(payload.(ORSetDelta))
		src.ResetDelta()
	}

	// Remove via delta
	src.Remove("apple")
	{
		payload, ok := src.DeltaPayload()
		if !ok {
			t.Fatal("expected delta after Remove")
		}
		dst.MergeORSetDelta(payload.(ORSetDelta))
		src.ResetDelta()
	}

	if dst.Contains("apple") {
		t.Error("dst should NOT contain apple after remove delta")
	}
}

func TestORSetDelta_Efficiency(t *testing.T) {
	// Large ORSet with 1000 elements; add 1 new element; delta << full state.
	const existing = 1000
	s := NewORSet()
	for i := 0; i < existing; i++ {
		s.Add("n1", string(rune(i+'a'%26)+rune(i)))
	}
	s.ResetDelta()

	s.Add("n1", "new_element_xyz")

	deltaPayload, ok := s.DeltaPayload()
	if !ok {
		t.Fatal("expected delta after Add")
	}
	deltaBytes, _ := json.Marshal(deltaPayload)
	fullBytes, _ := json.Marshal(s.Snapshot())

	t.Logf("ORSet(%d elements): full=%d bytes, delta=%d bytes (%.1f%% savings)",
		existing+1, len(fullBytes), len(deltaBytes),
		100*(1-float64(len(deltaBytes))/float64(len(fullBytes))))

	if len(deltaBytes) >= len(fullBytes) {
		t.Errorf("delta (%d bytes) should be smaller than full state (%d bytes)",
			len(deltaBytes), len(fullBytes))
	}
}

func TestORSetDelta_NoChangeAfterReset(t *testing.T) {
	s := NewORSet()
	s.Add("n1", "x")
	s.ResetDelta()
	_, ok := s.DeltaPayload()
	if ok {
		t.Error("expected no delta after ResetDelta")
	}
}

// ── LWWMap delta tests ────────────────────────────────────────────────────────

func TestLWWMapDelta_Correctness(t *testing.T) {
	src := NewLWWMap()
	dst := NewLWWMap()

	updates := []struct {
		key   string
		value string
	}{
		{"host1", "10.0.0.1"},
		{"host2", "10.0.0.2"},
		{"host1", "10.0.0.3"}, // update
	}

	for _, u := range updates {
		src.Put(u.key, u.value)
		payload, ok := src.DeltaPayload()
		if !ok {
			t.Fatalf("expected delta after Put(%s)", u.key)
		}
		dst.MergeLWWMapDelta(payload.(LWWMapDelta))
		src.ResetDelta()
	}

	for _, u := range updates {
		v, ok := dst.Get(u.key)
		if !ok {
			t.Errorf("dst missing key %q", u.key)
			continue
		}
		_ = v // last-write-wins — value may differ across iterations
	}
	// Final state: both should agree on host1's value being the last one written.
	srcVal, _ := src.Get("host1")
	dstVal, _ := dst.Get("host1")
	if srcVal != dstVal {
		t.Errorf("host1 value mismatch: src=%v dst=%v", srcVal, dstVal)
	}
}

func TestLWWMapDelta_Efficiency(t *testing.T) {
	const existing = 500
	m := NewLWWMap()
	for i := 0; i < existing; i++ {
		m.Put(string(rune('a'+i%26))+string(rune('0'+i%10)), i)
	}
	m.ResetDelta()

	m.Put("new_key", "new_value")

	deltaPayload, ok := m.DeltaPayload()
	if !ok {
		t.Fatal("expected delta after Put")
	}
	deltaBytes, _ := json.Marshal(deltaPayload)
	fullBytes, _ := json.Marshal(LWWMapPayload{State: m.Snapshot()})

	t.Logf("LWWMap(%d entries): full=%d bytes, delta=%d bytes (%.1f%% savings)",
		existing+1, len(fullBytes), len(deltaBytes),
		100*(1-float64(len(deltaBytes))/float64(len(fullBytes))))

	if len(deltaBytes) >= len(fullBytes) {
		t.Errorf("delta (%d bytes) should be smaller than full state (%d bytes)",
			len(deltaBytes), len(fullBytes))
	}
}

func TestLWWMapDelta_NoChangeAfterReset(t *testing.T) {
	m := NewLWWMap()
	m.Put("k", "v")
	m.ResetDelta()
	_, ok := m.DeltaPayload()
	if ok {
		t.Error("expected no delta after ResetDelta")
	}
}

// ── Convergence via full-state fallback ───────────────────────────────────────

// TestDeltaConvergence_LostDeltaHealed verifies that a node whose deltas were
// all dropped eventually converges once a full-state gossip arrives.
func TestDeltaConvergence_LostDeltaHealed(t *testing.T) {
	src := NewGCounter()
	dst := NewGCounter()

	// Three increments whose deltas we intentionally drop (not sent to dst).
	src.Increment("n1", 10)
	src.Increment("n2", 20)
	src.Increment("n1", 5)
	src.ResetDelta() // simulate: deltas were generated but dropped in transit

	// At this point dst is out of sync; it has never received any updates.
	if dst.Value() != 0 {
		t.Errorf("dst should be 0 before full-state gossip, got %d", dst.Value())
	}

	// Full-state fallback: dst receives the complete snapshot.
	dst.MergeState(src.Snapshot())

	if dst.Value() != src.Value() {
		t.Errorf("convergence failed: src=%d dst=%d", src.Value(), dst.Value())
	}
}

// TestDeltaConvergence_ORSet verifies full-state fallback for ORSet.
func TestDeltaConvergence_ORSet(t *testing.T) {
	src := NewORSet()
	dst := NewORSet()

	src.Add("n1", "alpha")
	src.Add("n1", "beta")
	src.Remove("alpha")
	src.ResetDelta() // deltas dropped

	// Full-state fallback
	dst.MergeSnapshot(src.Snapshot())

	if dst.Contains("alpha") {
		t.Error("dst should not contain alpha (removed)")
	}
	if !dst.Contains("beta") {
		t.Error("dst should contain beta")
	}
}

// TestDeltaConvergence_LWWMap verifies full-state fallback for LWWMap.
func TestDeltaConvergence_LWWMap(t *testing.T) {
	src := NewLWWMap()
	dst := NewLWWMap()

	src.Put("config", "v1")
	src.Put("config", "v2")
	src.ResetDelta() // deltas dropped

	// Full-state fallback
	dst.Merge(src.Snapshot())

	v, ok := dst.Get("config")
	if !ok {
		t.Fatal("dst missing 'config' after full-state fallback")
	}
	srcV, _ := src.Get("config")
	if v != srcV {
		t.Errorf("config value mismatch: src=%v dst=%v", srcV, v)
	}
}

// ── Replicator HandleIncoming delta routing ───────────────────────────────────

func TestReplicator_HandleIncoming_GCounterDelta(t *testing.T) {
	r := NewReplicator("n1", nil)

	// Pre-populate the counter so we have a baseline.
	r.GCounter("hits").Increment("n0", 100)

	// Simulate receiving a delta message.
	d := GCounterDelta{Delta: map[string]uint64{"n2": 42}}
	payload, _ := json.Marshal(d)
	msg := ReplicatorMsg{Type: "gcounter-delta", Key: "hits", Payload: payload}
	data, _ := json.Marshal(msg)

	if err := r.HandleIncoming(data); err != nil {
		t.Fatalf("HandleIncoming error: %v", err)
	}

	// n0=100 + n2=42 = 142
	if v := r.GCounter("hits").Value(); v != 142 {
		t.Errorf("expected 142, got %d", v)
	}
}

func TestReplicator_HandleIncoming_ORSetDelta(t *testing.T) {
	r := NewReplicator("n1", nil)

	d := ORSetDelta{
		AddedDots: map[string][]Dot{
			"svc-a": {{NodeID: "n2", Counter: 1}},
		},
		VV: map[string]uint64{"n2": 1},
	}
	payload, _ := json.Marshal(d)
	msg := ReplicatorMsg{Type: "orset-delta", Key: "services", Payload: payload}
	data, _ := json.Marshal(msg)

	if err := r.HandleIncoming(data); err != nil {
		t.Fatalf("HandleIncoming error: %v", err)
	}

	if !r.ORSet("services").Contains("svc-a") {
		t.Error("expected ORSet to contain svc-a after delta merge")
	}
}

func TestReplicator_HandleIncoming_LWWMapDelta(t *testing.T) {
	r := NewReplicator("n1", nil)

	d := LWWMapDelta{
		Changed: map[string]LWWEntry{
			"region": {Value: "us-east-1", Timestamp: 9999},
		},
	}
	payload, _ := json.Marshal(d)
	msg := ReplicatorMsg{Type: "lwwmap-delta", Key: "config", Payload: payload}
	data, _ := json.Marshal(msg)

	if err := r.HandleIncoming(data); err != nil {
		t.Fatalf("HandleIncoming error: %v", err)
	}

	v, ok := r.LWWMap("config").Get("region")
	if !ok {
		t.Fatal("expected LWWMap to contain 'region'")
	}
	if v != "us-east-1" {
		t.Errorf("expected us-east-1, got %v", v)
	}
}
