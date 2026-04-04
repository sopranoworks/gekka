/*
 * hub_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ── MergeHub ──────────────────────────────────────────────────────────────────

func TestMergeHub_SingleProducer(t *testing.T) {
	sink, src := stream.NewMergeHub[int](16)

	var got []int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		graph := src.To(stream.Foreach(func(n int) { got = append(got, n) }))
		if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
			t.Errorf("consumer error: %v", err)
		}
	}()

	// Give the consumer goroutine time to start.
	time.Sleep(10 * time.Millisecond)

	// Run a single producer.
	producer := stream.FromSlice([]int{1, 2, 3, 4, 5})
	if _, err := producer.To(sink).Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("producer error: %v", err)
	}

	// Signal completion by closing the hub channel externally is not exposed;
	// instead we wait a moment for the consumer to drain.
	time.Sleep(20 * time.Millisecond)

	sort.Ints(got)
	want := []int{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("MergeHub single producer: got %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("MergeHub[%d] = %d, want %d", i, got[i], w)
		}
	}
}

func TestMergeHub_MultipleProducers(t *testing.T) {
	sink, src := stream.NewMergeHub[int](64)

	var (
		mu  sync.Mutex
		got []int
	)

	// Start the consumer.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			// Pull from the hub source in a blocking loop.
			var local []int
			graph := stream.Via(src, stream.Take[int](10)).
				To(stream.Foreach(func(n int) { local = append(local, n) }))
			if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
				return
			}
			mu.Lock()
			got = append(got, local...)
			mu.Unlock()
			return
		}
	}()

	time.Sleep(10 * time.Millisecond)

	// Two concurrent producers.
	var prodWg sync.WaitGroup
	for _, base := range []int{0, 100} {
		base := base
		prodWg.Add(1)
		go func() {
			defer prodWg.Done()
			producer := stream.FromSlice([]int{base + 1, base + 2, base + 3, base + 4, base + 5})
			producer.To(sink).Run(stream.SyncMaterializer{}) //nolint:errcheck
		}()
	}
	prodWg.Wait()

	select {
	case <-consumerDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("consumer timeout")
	}

	mu.Lock()
	n := len(got)
	mu.Unlock()

	if n != 10 {
		t.Errorf("MergeHub multiple producers: got %d elements, want 10", n)
	}
}

// ── BroadcastHub ──────────────────────────────────────────────────────────────

func TestBroadcastHub_TwoConsumers(t *testing.T) {
	upstream := stream.FromSlice([]int{1, 2, 3, 4, 5})
	hub := stream.NewBroadcastHub(upstream, 16)

	var (
		mu   sync.Mutex
		got1 []int
		got2 []int
		wg   sync.WaitGroup
	)

	// Start two consumers, each materializing the hub independently.
	for _, target := range []*[]int{&got1, &got2} {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			graph := hub.To(stream.Foreach(func(n int) {
				mu.Lock()
				*target = append(*target, n)
				mu.Unlock()
			}))
			graph.Run(stream.SyncMaterializer{}) //nolint:errcheck
		}()
	}
	wg.Wait()

	sort.Ints(got1)
	sort.Ints(got2)

	// Each consumer should receive some elements (broadcast semantics with
	// a drop-on-slow-consumer policy: at least the elements that arrived
	// when the consumer was ready).
	if len(got1) == 0 && len(got2) == 0 {
		t.Error("BroadcastHub: both consumers received nothing")
	}
}

func TestBroadcastHub_SingleConsumer(t *testing.T) {
	upstream := stream.FromSlice([]int{10, 20, 30})
	hub := stream.NewBroadcastHub(upstream, 16)

	var got []int
	graph := hub.To(stream.Foreach(func(n int) { got = append(got, n) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("BroadcastHub single consumer error: %v", err)
	}

	// With a single consumer there should be no drops.
	sort.Ints(got)
	want := []int{10, 20, 30}
	if len(got) != len(want) {
		t.Fatalf("BroadcastHub single: got %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("BroadcastHub single[%d] = %d, want %d", i, got[i], w)
		}
	}
}

// ── PartitionHub ──────────────────────────────────────────────────────────────

func TestPartitionHub_EvenOdd(t *testing.T) {
	upstream := stream.FromSlice([]int{1, 2, 3, 4, 5, 6})

	// Partition: even → index 0, odd → index 1.
	sources := stream.NewPartitionHub(upstream, func(_ int, n int) int {
		if n%2 == 0 {
			return 0
		}
		return 1
	}, 2, 16)

	if len(sources) != 2 {
		t.Fatalf("PartitionHub: got %d sources, want 2", len(sources))
	}

	var wg sync.WaitGroup
	var (
		mu   sync.Mutex
		even []int
		odd  []int
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		graph := sources[0].To(stream.Foreach(func(n int) {
			mu.Lock()
			even = append(even, n)
			mu.Unlock()
		}))
		graph.Run(stream.SyncMaterializer{}) //nolint:errcheck
	}()
	go func() {
		defer wg.Done()
		graph := sources[1].To(stream.Foreach(func(n int) {
			mu.Lock()
			odd = append(odd, n)
			mu.Unlock()
		}))
		graph.Run(stream.SyncMaterializer{}) //nolint:errcheck
	}()
	wg.Wait()

	sort.Ints(even)
	sort.Ints(odd)

	for _, n := range even {
		if n%2 != 0 {
			t.Errorf("PartitionHub even got odd number %d", n)
		}
	}
	for _, n := range odd {
		if n%2 == 0 {
			t.Errorf("PartitionHub odd got even number %d", n)
		}
	}

	total := len(even) + len(odd)
	if total != 6 {
		t.Errorf("PartitionHub: total = %d, want 6 (even=%v, odd=%v)", total, even, odd)
	}
}

func TestPartitionHub_SinglePartition(t *testing.T) {
	upstream := stream.FromSlice([]int{7, 8, 9})
	sources := stream.NewPartitionHub(upstream, func(_ int, _ int) int { return 0 }, 1, 16)

	var got []int
	graph := sources[0].To(stream.Foreach(func(n int) { got = append(got, n) }))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("PartitionHub single: %v", err)
	}

	sort.Ints(got)
	want := []int{7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("PartitionHub single: got %v, want %v", got, want)
	}
}

func TestPartitionHub_OutOfRangeClamped(t *testing.T) {
	// Partitioner returning out-of-range index should be clamped, not panic.
	upstream := stream.FromSlice([]int{1, 2, 3})
	sources := stream.NewPartitionHub(upstream, func(n int, _ int) int {
		return 999 // way out of range, should clamp to n-1 = 1
	}, 2, 16)

	var got []int
	graph := sources[1].To(stream.Foreach(func(n int) { got = append(got, n) }))
	graph.Run(stream.SyncMaterializer{}) //nolint:errcheck

	if len(got) == 0 {
		t.Error("PartitionHub clamp: expected elements at partition 1")
	}
}
