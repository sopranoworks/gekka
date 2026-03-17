/*
 * async_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// makeRange returns a []int{0, 1, …, n-1}.
func makeRange(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

// TestAsync_SourceAsync_DeliversAll verifies that Source.Async() delivers all
// elements in FIFO order with no drops.
func TestAsync_SourceAsync_DeliversAll(t *testing.T) {
	const N = 200
	result, err := stream.RunWith(
		stream.FromSlice(makeRange(N)).Async(),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestAsync_FlowAsync_DeliversAll verifies that a Flow with an async boundary
// delivers all transformed elements correctly.
func TestAsync_FlowAsync_DeliversAll(t *testing.T) {
	const N = 150
	doubleAsync := stream.Map(func(n int) int { return n * 2 }).Async()
	result, err := stream.RunWith(
		stream.Via(stream.FromSlice(makeRange(N)), doubleAsync),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i*2 {
			t.Fatalf("index %d: got %d, want %d", i, v, i*2)
		}
	}
}

// TestAsync_MultipleAsyncBoundaries verifies that chaining several async
// boundaries still delivers all elements in strict FIFO order.
func TestAsync_MultipleAsyncBoundaries(t *testing.T) {
	const N = 100
	src := stream.FromSlice(makeRange(N)).Async()
	add1 := stream.Map(func(n int) int { return n + 1 }).Async()
	mul2 := stream.Map(func(n int) int { return n * 2 }).Async()
	pipeline := stream.Via(stream.Via(src, add1), mul2)

	result, err := stream.RunWith(pipeline, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		want := (i + 1) * 2
		if v != want {
			t.Fatalf("index %d: got %d, want %d", i, v, want)
		}
	}
}

// TestAsync_SlowSink_NoDrops verifies that a slow sink separated from a fast
// source by an async boundary receives every element without drops.
func TestAsync_SlowSink_NoDrops(t *testing.T) {
	const N = 60
	var mu sync.Mutex
	received := make([]int, 0, N)

	sink := stream.Foreach(func(n int) {
		time.Sleep(time.Millisecond) // slow consumer
		mu.Lock()
		received = append(received, n)
		mu.Unlock()
	})

	graph := stream.FromSlice(makeRange(N)).Async().To(sink)
	if _, err := graph.Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != N {
		t.Fatalf("got %d elements, want %d", len(received), N)
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestAsync_BackPressureLimitsInflight confirms that the bounded async buffer
// prevents the upstream goroutine from running arbitrarily far ahead of a
// paused downstream sink.
//
// While the sink is blocked on the gate channel, at most
// DefaultAsyncBufSize + 2 elements should have been pulled from the source:
//
//   - DefaultAsyncBufSize  — sitting in the channel buffer
//   - 1                   — being processed (or just processed) by the sink
//   - 1                   — staged by the goroutine after the last send unblocked
func TestAsync_BackPressureLimitsInflight(t *testing.T) {
	const N = 200
	const bufSize = stream.DefaultAsyncBufSize

	var produced atomic.Int64
	i := 0
	src := stream.FromIteratorFunc(func() (int, bool, error) {
		if i >= N {
			return 0, false, nil
		}
		v := i
		i++
		produced.Add(1)
		return v, true, nil
	})

	// The sink blocks after receiving element 0, then resumes when gate closes.
	gate := make(chan struct{})
	var received []int
	sink := stream.Foreach(func(n int) {
		received = append(received, n)
		if n == 0 {
			<-gate
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		src.Async().To(sink).Run(stream.ActorMaterializer{}) //nolint:errcheck
	}()

	// Give the upstream goroutine time to fill the channel and block.
	time.Sleep(60 * time.Millisecond)

	p := produced.Load()
	// +2 slack: one element staged by goroutine after the last channel send
	// unblocks, plus one for scheduling jitter.
	const maxAllowed = int64(bufSize) + 3
	if p > maxAllowed {
		close(gate) // prevent goroutine leak
		<-done
		t.Fatalf(
			"back-pressure not applied: %d elements produced while sink was paused (max allowed %d, bufSize %d)",
			p, maxAllowed, bufSize,
		)
	}

	close(gate) // unblock sink
	<-done

	if len(received) != N {
		t.Fatalf("expected %d elements after resume, got %d", N, len(received))
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestAsync_ErrorPropagation confirms that an error from a failing upstream
// crosses the async boundary and is returned by Run.
func TestAsync_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("upstream error")
	graph := stream.Failed[int](sentinel).Async().To(stream.Ignore[int]())
	_, err := graph.Run(stream.ActorMaterializer{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

// TestAsync_SinkAsync_DeliversAll verifies that Sink.Async() (buffer before
// the sink) delivers all elements correctly even with a slow sink.
func TestAsync_SinkAsync_DeliversAll(t *testing.T) {
	const N = 80
	var mu sync.Mutex
	received := make([]int, 0, N)

	slowSink := stream.Foreach(func(n int) {
		time.Sleep(100 * time.Microsecond)
		mu.Lock()
		received = append(received, n)
		mu.Unlock()
	}).Async()

	if _, err := stream.FromSlice(makeRange(N)).To(slowSink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != N {
		t.Fatalf("got %d elements, want %d", len(received), N)
	}
}

// TestGraphInterpreter_Go verifies that goroutines registered with a
// GraphInterpreter are fully awaited by Wait.
func TestGraphInterpreter_Go(t *testing.T) {
	const workers = 8
	gi := stream.NewGraphInterpreter()
	counts := make([]atomic.Int64, workers)

	for i := 0; i < workers; i++ {
		idx := i
		gi.Go(func() {
			counts[idx].Store(int64(idx + 1))
		})
	}
	gi.Wait()

	for i := range counts {
		if got := counts[i].Load(); got != int64(i+1) {
			t.Fatalf("goroutine %d did not complete: got %d", i, got)
		}
	}
}
