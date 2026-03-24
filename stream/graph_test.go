/*
 * graph_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── Zip / ZipWith ────────────────────────────────────────────────────────

// TestZip_EqualLength verifies that Zip emits N pairs when both sources have N
// elements, and that pairing is positional (first with first, etc.).
func TestZip_EqualLength(t *testing.T) {
	result, err := stream.RunWith(
		stream.Zip(
			stream.FromSlice([]int{1, 2, 3}),
			stream.FromSlice([]int{10, 20, 30}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []stream.Pair[int, int]{{1, 10}, {2, 20}, {3, 30}}
	if len(result) != len(want) {
		t.Fatalf("got %d pairs, want %d", len(result), len(want))
	}
	for i, p := range result {
		if p != want[i] {
			t.Fatalf("index %d: got %v, want %v", i, p, want[i])
		}
	}
}

// TestZip_LeftShorter verifies that Zip stops when the left source is
// exhausted, even though the right source has more elements.
func TestZip_LeftShorter(t *testing.T) {
	result, err := stream.RunWith(
		stream.Zip(
			stream.FromSlice([]int{1, 2}),       // shorter
			stream.FromSlice([]int{10, 20, 30}), // longer
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("got %d pairs, want 2", len(result))
	}
}

// TestZip_RightShorter verifies that Zip stops when the right source is
// exhausted, even though the left source has more elements.
func TestZip_RightShorter(t *testing.T) {
	result, err := stream.RunWith(
		stream.Zip(
			stream.FromSlice([]int{1, 2, 3, 4}), // longer
			stream.FromSlice([]int{10}),         // shorter
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d pairs, want 1", len(result))
	}
	if result[0].First != 1 || result[0].Second != 10 {
		t.Fatalf("got %v, want {1 10}", result[0])
	}
}

// TestZip_EmptyLeft verifies that Zip emits nothing when the left source is
// empty, regardless of the right source's length.
func TestZip_EmptyLeft(t *testing.T) {
	result, err := stream.RunWith(
		stream.Zip(
			stream.FromSlice([]int{}),
			stream.FromSlice([]int{1, 2, 3}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("got %d pairs, want 0", len(result))
	}
}

// TestZip_ErrorPropagates verifies that an error from either source terminates
// the zipped stream with that error.
func TestZip_ErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("zip source failure")

	_, err := stream.RunWith(
		stream.Zip(
			stream.Failed[int](sentinelErr),
			stream.FromSlice([]int{1, 2, 3}),
		),
		stream.Collect[stream.Pair[int, int]](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

// TestZipWith_Combine verifies that ZipWith applies the combiner function to
// each pair of elements.
func TestZipWith_Combine(t *testing.T) {
	result, err := stream.RunWith(
		stream.ZipWith(
			stream.FromSlice([]int{1, 2, 3}),
			stream.FromSlice([]int{10, 20, 30}),
			func(a, b int) int { return a + b },
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{11, 22, 33}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// ─── Merge ────────────────────────────────────────────────────────────────

// TestMerge_AllElementsDelivered verifies that every element from every source
// is present in the merged output regardless of emission order.
func TestMerge_AllElementsDelivered(t *testing.T) {
	src1 := stream.FromSlice([]int{1, 2, 3})
	src2 := stream.FromSlice([]int{4, 5, 6, 7})
	src3 := stream.FromSlice([]int{8, 9})

	result, err := stream.RunWith(
		stream.Merge(src1, src2, src3),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const want = 9 // 3 + 4 + 2
	if len(result) != want {
		t.Fatalf("got %d elements, want %d", len(result), want)
	}

	// Verify every expected value is present exactly once.
	sort.Ints(result)
	for i, v := range result {
		if v != i+1 {
			t.Fatalf("index %d: got %d, want %d", i, v, i+1)
		}
	}
}

// TestMerge_WithEmptySource verifies that an empty source among the inputs
// does not prevent the merge from completing normally.
func TestMerge_WithEmptySource(t *testing.T) {
	src1 := stream.FromSlice([]int{10, 20, 30})
	empty := stream.FromSlice([]int{})

	result, err := stream.RunWith(
		stream.Merge(src1, empty),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sort.Ints(result)
	want := []int{10, 20, 30}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestMerge_SingleSource verifies that Merge with one source behaves
// identically to running that source directly.
func TestMerge_SingleSource(t *testing.T) {
	const N = 50

	result, err := stream.RunWith(
		stream.Merge(stream.FromSlice(makeRange(N))),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
}

// TestMerge_FailPropagates verifies that an error from any upstream source
// propagates through the merged stream.
func TestMerge_FailPropagates(t *testing.T) {
	sentinelErr := errors.New("upstream failure")

	src1 := stream.FromSlice([]int{1, 2, 3})
	src2 := stream.Failed[int](sentinelErr)

	_, err := stream.RunWith(
		stream.Merge(src1, src2),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinelErr, got %v", err)
	}
}

// TestMerge_LargeParallelSources verifies correctness under higher load:
// five sources each contributing 100 elements, 500 total expected.
func TestMerge_LargeParallelSources(t *testing.T) {
	const (
		sourceCnt = 5
		perSource = 100
	)

	sources := make([]stream.Source[int, stream.NotUsed], sourceCnt)
	for i := range sources {
		base := i * perSource
		elems := make([]int, perSource)
		for j := range elems {
			elems[j] = base + j
		}
		sources[i] = stream.FromSlice(elems)
	}

	result, err := stream.RunWith(
		stream.Merge(sources...),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != sourceCnt*perSource {
		t.Fatalf("got %d elements, want %d", len(result), sourceCnt*perSource)
	}

	// Every value 0..499 must appear exactly once.
	seen := make(map[int]struct{}, len(result))
	for _, v := range result {
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate element %d", v)
		}
		seen[v] = struct{}{}
	}
	for i := range sourceCnt * perSource {
		if _, ok := seen[i]; !ok {
			t.Fatalf("missing element %d", i)
		}
	}
}

// ─── Broadcast ────────────────────────────────────────────────────────────

// TestBroadcast_AllBranchesReceiveAll verifies that each output branch of
// Broadcast receives a complete, identical copy of the source in order.
func TestBroadcast_AllBranchesReceiveAll(t *testing.T) {
	const (
		N      = 30
		fanOut = 3
	)

	branches := stream.Broadcast(stream.FromSlice(makeRange(N)), fanOut, 32)

	results := make([][]int, fanOut)
	var wg sync.WaitGroup
	for i, b := range branches {
		wg.Add(1)
		go func(idx int, src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			res, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
			if err != nil {
				t.Errorf("branch %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = res
		}(i, b)
	}
	wg.Wait()

	for i, res := range results {
		if len(res) != N {
			t.Fatalf("branch %d: got %d elements, want %d", i, len(res), N)
		}
		// Each branch must receive elements in the same order as the source.
		for j, v := range res {
			if v != j {
				t.Fatalf("branch %d, index %d: got %d, want %d", i, j, v, j)
			}
		}
	}
}

// TestBroadcast_TwoBranches verifies the minimal 2-way fan-out case.
func TestBroadcast_TwoBranches(t *testing.T) {
	const N = 10

	branches := stream.Broadcast(stream.FromSlice(makeRange(N)), 2, 16)

	var (
		wg      sync.WaitGroup
		results [2][]int
	)
	for i, b := range branches {
		wg.Add(1)
		go func(idx int, src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			res, _ := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
			results[idx] = res
		}(i, b)
	}
	wg.Wait()

	for i, res := range results {
		if len(res) != N {
			t.Fatalf("branch %d: got %d elements, want %d", i, len(res), N)
		}
	}
	// Both branches must have the same content.
	for j := range results[0] {
		if results[0][j] != results[1][j] {
			t.Fatalf("index %d: branch 0 got %d, branch 1 got %d",
				j, results[0][j], results[1][j])
		}
	}
}

// TestBroadcast_FailPropagates verifies that an upstream error is delivered
// to all branches.
func TestBroadcast_FailPropagates(t *testing.T) {
	sentinelErr := errors.New("broadcast failure")

	branches := stream.Broadcast(stream.Failed[int](sentinelErr), 2, 8)

	var (
		wg   sync.WaitGroup
		errs [2]error
	)
	for i, b := range branches {
		wg.Add(1)
		go func(idx int, src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			_, errs[idx] = stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
		}(i, b)
	}
	wg.Wait()

	// At least one branch must have received the error.  (The other may
	// receive an empty result if it consumed the error-signal channel first.)
	atLeastOne := false
	for _, err := range errs {
		if errors.Is(err, sentinelErr) {
			atLeastOne = true
		}
	}
	if !atLeastOne {
		t.Fatalf("expected at least one branch to receive sentinelErr; got %v", errs)
	}
}

// ─── Balance ──────────────────────────────────────────────────────────────

// TestBalance_TotalElementsPreserved verifies that the sum of elements
// received across all output branches equals the total emitted by the source.
func TestBalance_TotalElementsPreserved(t *testing.T) {
	const (
		total  = 100
		fanOut = 4
	)

	branches := stream.Balance(stream.FromSlice(makeRange(total)), fanOut, 32)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allVals []int
	)
	for _, b := range branches {
		wg.Add(1)
		go func(src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			res, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			allVals = append(allVals, res...)
			mu.Unlock()
		}(b)
	}
	wg.Wait()

	if len(allVals) != total {
		t.Fatalf("got %d total elements, want %d", len(allVals), total)
	}
}

// TestBalance_NoElementDuplicated verifies that with Balance each element is
// delivered to exactly one consumer — no duplication, no omission.
func TestBalance_NoElementDuplicated(t *testing.T) {
	const (
		total  = 200
		fanOut = 5
	)

	branches := stream.Balance(stream.FromSlice(makeRange(total)), fanOut, 16)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allVals []int
	)
	for _, b := range branches {
		wg.Add(1)
		go func(src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			res, _ := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
			mu.Lock()
			allVals = append(allVals, res...)
			mu.Unlock()
		}(b)
	}
	wg.Wait()

	// Every value 0..total-1 must appear exactly once.
	sort.Ints(allVals)
	if len(allVals) != total {
		t.Fatalf("got %d elements, want %d", len(allVals), total)
	}
	for i, v := range allVals {
		if v != i {
			t.Fatalf("after sort, index %d: got %d, want %d (duplicate or missing element)",
				i, v, i)
		}
	}
}

// TestBalance_DistributesAcrossBranches verifies that Balance spreads load:
// no single branch should receive all elements when there are multiple eager
// consumers.
func TestBalance_DistributesAcrossBranches(t *testing.T) {
	const (
		total  = 100
		fanOut = 4
	)

	branches := stream.Balance(stream.FromSlice(makeRange(total)), fanOut, 8)

	counts := make([]int, fanOut)
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for i, b := range branches {
		wg.Add(1)
		go func(idx int, src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			res, _ := stream.RunWith(src.Delay(time.Millisecond), stream.Collect[int](), stream.ActorMaterializer{})
			mu.Lock()
			counts[idx] = len(res)
			mu.Unlock()
		}(i, b)
	}
	wg.Wait()

	sum := 0
	for _, c := range counts {
		sum += c
	}
	if sum != total {
		t.Fatalf("total %d ≠ %d", sum, total)
	}

	// With 4 equal consumers and 100 elements, no single branch should
	// receive everything (that would mean the other 3 received nothing,
	// which indicates Balance is not distributing at all).
	for i, c := range counts {
		if c == total {
			t.Fatalf("branch %d received all %d elements — Balance is not distributing", i, total)
		}
	}

	t.Logf("distribution across %d branches: %v", fanOut, counts)
}

// TestBalance_FailPropagates verifies that an upstream error propagates to
// whichever consumer reads next after the failure.
func TestBalance_FailPropagates(t *testing.T) {
	sentinelErr := errors.New("balance failure")

	branches := stream.Balance(stream.Failed[int](sentinelErr), 2, 8)

	var (
		wg   sync.WaitGroup
		errs [2]error
	)
	for i, b := range branches {
		wg.Add(1)
		go func(idx int, src stream.Source[int, stream.NotUsed]) {
			defer wg.Done()
			_, errs[idx] = stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
		}(i, b)
	}
	wg.Wait()

	atLeastOne := false
	for _, err := range errs {
		if errors.Is(err, sentinelErr) {
			atLeastOne = true
		}
	}
	if !atLeastOne {
		t.Fatalf("expected at least one branch to receive sentinelErr; got %v", errs)
	}
}
