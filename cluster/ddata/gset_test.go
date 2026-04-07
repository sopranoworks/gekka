/*
 * gset_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"testing"
)

func TestGSet_AddAndContains(t *testing.T) {
	g := NewGSet()
	g.Add("a")
	g.Add("b")
	g.Add("a") // duplicate

	if !g.Contains("a") {
		t.Error("expected to contain 'a'")
	}
	if !g.Contains("b") {
		t.Error("expected to contain 'b'")
	}
	if g.Contains("c") {
		t.Error("should not contain 'c'")
	}
	if g.Size() != 2 {
		t.Errorf("expected size 2, got %d", g.Size())
	}
}

func TestGSet_Elements(t *testing.T) {
	g := NewGSet()
	g.Add("c")
	g.Add("a")
	g.Add("b")

	elems := g.Elements()
	if len(elems) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elems))
	}
	// Elements returns sorted order.
	if elems[0] != "a" || elems[1] != "b" || elems[2] != "c" {
		t.Errorf("expected [a b c], got %v", elems)
	}
}

func TestGSet_Merge(t *testing.T) {
	g1 := NewGSet()
	g1.Add("a")
	g1.Add("b")

	g2 := NewGSet()
	g2.Add("b")
	g2.Add("c")

	merged := g1.Merge(g2)
	if merged.Size() != 3 {
		t.Fatalf("expected 3 elements after merge, got %d", merged.Size())
	}
	for _, e := range []string{"a", "b", "c"} {
		if !merged.Contains(e) {
			t.Errorf("merged set should contain %q", e)
		}
	}

	// Commutativity: Merge(g2, g1) == Merge(g1, g2)
	merged2 := g2.Merge(g1)
	if merged2.Size() != merged.Size() {
		t.Error("merge should be commutative")
	}
}

func TestGSet_MergeEmpty(t *testing.T) {
	g := NewGSet()
	g.Add("x")

	empty := NewGSet()
	result := g.Merge(empty)
	if result.Size() != 1 || !result.Contains("x") {
		t.Error("merge with empty should preserve elements")
	}

	result2 := empty.Merge(g)
	if result2.Size() != 1 || !result2.Contains("x") {
		t.Error("empty merge with non-empty should take elements")
	}
}

func TestGSet_DeltaPropagation(t *testing.T) {
	g := NewGSet()

	// No delta initially.
	if _, ok := g.DeltaPayload(); ok {
		t.Error("expected no delta for empty set")
	}

	g.Add("a")
	g.Add("b")

	payload, ok := g.DeltaPayload()
	if !ok {
		t.Fatal("expected delta after adds")
	}
	delta := payload.(*GSetDelta)
	if len(delta.Added) != 2 {
		t.Fatalf("expected 2 added elements, got %d", len(delta.Added))
	}

	// Reset and verify clean.
	g.ResetDelta()
	if _, ok := g.DeltaPayload(); ok {
		t.Error("expected no delta after reset")
	}

	// Add more.
	g.Add("c")
	payload, ok = g.DeltaPayload()
	if !ok {
		t.Fatal("expected delta after adding 'c'")
	}
	delta = payload.(*GSetDelta)
	if len(delta.Added) != 1 || delta.Added[0] != "c" {
		t.Errorf("expected [c], got %v", delta.Added)
	}
}

func TestGSet_MergeDelta(t *testing.T) {
	// Simulate two replicas.
	replica1 := NewGSet()
	replica2 := NewGSet()

	replica1.Add("a")
	replica1.Add("b")

	// Get delta from replica1 and apply to replica2.
	payload, _ := replica1.DeltaPayload()
	delta := payload.(*GSetDelta)

	replica2.MergeDelta(delta)

	if replica2.Size() != 2 {
		t.Fatalf("expected 2 elements in replica2, got %d", replica2.Size())
	}
	if !replica2.Contains("a") || !replica2.Contains("b") {
		t.Error("replica2 should contain a and b after delta merge")
	}

	// Applying the same delta again should be idempotent.
	replica2.MergeDelta(delta)
	if replica2.Size() != 2 {
		t.Error("delta merge should be idempotent")
	}
}

func TestGSet_DuplicateAddNoDelta(t *testing.T) {
	g := NewGSet()
	g.Add("x")
	g.ResetDelta()

	// Adding an existing element should NOT produce a new delta.
	g.Add("x")
	if _, ok := g.DeltaPayload(); ok {
		t.Error("duplicate add should not produce delta")
	}
}
