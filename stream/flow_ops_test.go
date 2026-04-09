/*
 * flow_ops_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"fmt"
	"testing"
)

// ─── Intersperse ─────────────────────────────────────────────────────────

func TestIntersperse(t *testing.T) {
	src := Intersperse(FromSlice([]int{1, 2, 3}), 0)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 0, 2, 0, 3}
	if !intSliceEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestIntersperse_SingleElement(t *testing.T) {
	src := Intersperse(FromSlice([]int{42}), 0)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{42}
	if !intSliceEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestIntersperse_Empty(t *testing.T) {
	src := Intersperse(Empty[int](), 0)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want [], got %v", got)
	}
}

func TestIntersperseAll(t *testing.T) {
	src := IntersperseAll(FromSlice([]string{"a", "b", "c"}), "[", ",", "]")
	got, err := RunWith(src, Collect[string](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"[", "a", ",", "b", ",", "c", "]"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// ─── RecoverWithRetries ──────────────────────────────────────────────────

func TestRecoverWithRetries_Recovers(t *testing.T) {
	// Source that immediately fails
	failing := Failed[int](errors.New("fail"))

	recovered := false
	src := RecoverWithRetries(failing, 3, func(err error) Source[int, NotUsed] {
		recovered = true
		return FromSlice([]int{99, 100})
	})
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("expected recovery to be triggered")
	}
	if !intSliceEqual(got, []int{99, 100}) {
		t.Fatalf("want [99 100], got %v", got)
	}
}

func TestRecoverWithRetries_ExhaustsRetries(t *testing.T) {
	boom := errors.New("boom")
	failing := Failed[int](boom)

	src := RecoverWithRetries(failing, 2, func(err error) Source[int, NotUsed] {
		return Failed[int](err) // recovery also fails
	})
	_, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

// ─── MapError ────────────────────────────────────────────────────────────

func TestMapError_TransformsError(t *testing.T) {
	inner := errors.New("inner")
	src := MapError(Failed[int](inner), func(err error) error {
		return fmt.Errorf("wrapped: %w", err)
	})
	_, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err == nil || !errors.Is(err, inner) {
		t.Fatalf("want wrapped inner error, got %v", err)
	}
	if err.Error() != "wrapped: inner" {
		t.Fatalf("want 'wrapped: inner', got %q", err.Error())
	}
}

func TestMapError_PassesElements(t *testing.T) {
	src := MapError(FromSlice([]int{1, 2, 3}), func(err error) error {
		return fmt.Errorf("should not be called: %w", err)
	})
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	if !intSliceEqual(got, []int{1, 2, 3}) {
		t.Fatalf("want [1 2 3], got %v", got)
	}
}

// ─── StatefulMap ─────────────────────────────────────────────────────────

func TestStatefulMap_RunningIndex(t *testing.T) {
	src := StatefulMap(FromSlice([]string{"a", "b", "c"}), func() func(string) string {
		idx := 0
		return func(s string) string {
			result := fmt.Sprintf("%d:%s", idx, s)
			idx++
			return result
		}
	})
	got, err := RunWith(src, Collect[string](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0:a", "1:b", "2:c"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// ─── BatchWeighted ───────────────────────────────────────────────────────

func TestBatchWeighted(t *testing.T) {
	// Elements with varying costs: 1→cost1, 2→cost2, etc.
	// maxWeight=5: batch1=[1,2](cost=3), batch2=[3](cost=3 alone), batch3=[4](cost=4), batch4=[5](cost=5)
	src := BatchWeighted(
		FromSlice([]int{1, 2, 3, 4, 5}),
		5,
		func(n int) int64 { return int64(n) },
		func(n int) []int { return []int{n} },
		func(acc []int, n int) []int { return append(acc, n) },
	)
	got, err := RunWith(src, Collect[[]int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	// batch1: 1(cost1) + 2(cost2) = cost3, then 3(cost3) would exceed 5→6 so flush
	// batch2: 3(cost3) + 4(cost4) would exceed 5→7 so flush
	// batch3: 4(cost4) + 5(cost5) would exceed 5→9 so flush
	// batch4: 5(cost5) alone
	if len(got) < 2 {
		t.Fatalf("expected multiple batches, got %d: %v", len(got), got)
	}
	// Flatten and verify all elements present
	var flat []int
	for _, batch := range got {
		flat = append(flat, batch...)
	}
	if !intSliceEqual(flat, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("flattened want [1 2 3 4 5], got %v", flat)
	}
}

func TestBatchWeighted_SingleHeavy(t *testing.T) {
	// Each element exceeds maxWeight by itself
	src := BatchWeighted(
		FromSlice([]int{10, 20, 30}),
		5,
		func(n int) int64 { return int64(n) },
		func(n int) int { return n },
		func(acc, n int) int { return acc + n },
	)
	got, err := RunWith(src, Collect[int](), SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	// Each element starts a batch, its weight >= maxWeight, so emitted individually
	if !intSliceEqual(got, []int{10, 20, 30}) {
		t.Fatalf("want [10 20 30], got %v", got)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────

func intSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
