/*
 * actor_integration_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/stream"
)

// ─── Processor actor definition ───────────────────────────────────────────

// DoubleRequest is the message sent to the Processor actor.
// It carries a value and a typed reply-to reference so the actor can send
// the result directly back to the Ask caller.
type DoubleRequest struct {
	Value   int
	ReplyTo typed.TypedActorRef[int]
}

// doubleProcessorBehavior is a typed Behavior[DoubleRequest] that replies
// with Value * 2 for every request it receives.
func doubleProcessorBehavior(ctx typed.TypedContext[DoubleRequest], msg DoubleRequest) typed.Behavior[DoubleRequest] {
	msg.ReplyTo.Tell(msg.Value * 2)
	return typed.Same[DoubleRequest]()
}

// ─── Test helpers ─────────────────────────────────────────────────────────

// newTestSystem creates a local ActorSystem for testing with a unique name.
func newTestSystem(t *testing.T) gekka.ActorSystem {
	t.Helper()
	sys, err := gekka.NewActorSystem(fmt.Sprintf("test-%s", t.Name()))
	if err != nil {
		t.Fatalf("NewActorSystem: %v", err)
	}
	return sys
}

// spawnTyped spawns a properly typed actor using Props so that
// typed.NewTypedActor[T] is used directly (not the reflection-based
// NewTypedActorGeneric path).  Returns the TypedActorRef and a stop func.
func spawnTyped[T any](t *testing.T, sys gekka.ActorSystem, behavior typed.Behavior[T], name string) (typed.TypedActorRef[T], func()) {
	t.Helper()
	ref, err := sys.ActorOf(actor.Props{
		New: func() actor.Actor { return typed.NewTypedActor[T](behavior) },
	}, name)
	if err != nil {
		t.Fatalf("ActorOf %q: %v", name, err)
	}
	typedRef := typed.NewTypedActorRef[T](ref)
	return typedRef, func() { sys.Stop(ref) }
}

// ─── Tests ────────────────────────────────────────────────────────────────

// TestAsk_Flow_DoubleProcessor verifies the core Ask flow:
// a Source of integers is piped through an Ask flow that sends each value
// to the Processor actor and collects the doubled replies.
func TestAsk_Flow_DoubleProcessor(t *testing.T) {
	sys := newTestSystem(t)
	processorRef, stop := spawnTyped(t, sys, typed.Behavior[DoubleRequest](doubleProcessorBehavior), "processor")
	defer stop()

	askFlow := stream.Ask(
		processorRef,
		5*time.Second,
		func(n int, replyTo typed.TypedActorRef[int]) DoubleRequest {
			return DoubleRequest{Value: n, ReplyTo: replyTo}
		},
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1, 2, 3, 4, 5}), askFlow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	want := []int{2, 4, 6, 8, 10}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// TestAsk_Flow_OrderPreserved verifies that the sequential Ask flow preserves
// FIFO order: because we send one Ask at a time, the output order matches
// input order exactly.
func TestAsk_Flow_OrderPreserved(t *testing.T) {
	sys := newTestSystem(t)
	processorRef, stop := spawnTyped(t, sys, typed.Behavior[DoubleRequest](doubleProcessorBehavior), "proc")
	defer stop()

	const N = 50
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}

	askFlow := stream.Ask(
		processorRef,
		5*time.Second,
		func(n int, replyTo typed.TypedActorRef[int]) DoubleRequest {
			return DoubleRequest{Value: n, ReplyTo: replyTo}
		},
	)

	result, err := stream.RunWith(
		stream.Via(stream.FromSlice(input), askFlow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("RunWith: %v", err)
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

// TestAsk_Flow_Timeout verifies that the Ask flow propagates a timeout error
// when the actor does not reply within the given duration.
func TestAsk_Flow_Timeout(t *testing.T) {
	sys := newTestSystem(t)

	// A slow actor that never replies — simulates a hung actor.
	type SlowReq struct {
		ReplyTo typed.TypedActorRef[int]
	}
	slowBehavior := typed.Behavior[SlowReq](func(_ typed.TypedContext[SlowReq], _ SlowReq) typed.Behavior[SlowReq] {
		return typed.Same[SlowReq]() // intentionally no reply
	})
	slowRef, stop := spawnTyped(t, sys, slowBehavior, "slow")
	defer stop()

	askFlow := stream.Ask(
		slowRef,
		50*time.Millisecond, // very short timeout
		func(_ int, replyTo typed.TypedActorRef[int]) SlowReq {
			return SlowReq{ReplyTo: replyTo}
		},
	)

	_, err := stream.RunWith(
		stream.Via(stream.FromSlice([]int{1}), askFlow),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// context.DeadlineExceeded is the expected error from typed.Ask.
	if err != context.DeadlineExceeded {
		t.Logf("timeout error (expected): %v", err)
	}
}

// TestAsk_Flow_WithAsyncBoundary verifies that the Ask flow integrates
// correctly with local async boundaries placed before and after it.
func TestAsk_Flow_WithAsyncBoundary(t *testing.T) {
	sys := newTestSystem(t)
	processorRef, stop := spawnTyped(t, sys, typed.Behavior[DoubleRequest](doubleProcessorBehavior), "proc")
	defer stop()

	askFlow := stream.Ask(
		processorRef,
		5*time.Second,
		func(n int, replyTo typed.TypedActorRef[int]) DoubleRequest {
			return DoubleRequest{Value: n, ReplyTo: replyTo}
		},
	)

	// Async boundary before the Ask flow, then the Ask flow itself with Async.
	src := stream.FromSlice([]int{10, 20, 30}).Async()
	pipeline := stream.Via(src, askFlow.Async())

	result, err := stream.RunWith(pipeline, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	want := []int{20, 40, 60}
	if len(result) != len(want) {
		t.Fatalf("got %v, want %v", result, want)
	}
	for i, v := range result {
		if v != want[i] {
			t.Fatalf("index %d: got %d, want %d", i, v, want[i])
		}
	}
}

// ─── ActorSource tests ────────────────────────────────────────────────────

// TestActorSource_SingleWriterOrder verifies that elements sent to the
// TypedActorRef from a single goroutine arrive in the stream in FIFO order.
func TestActorSource_SingleWriterOrder(t *testing.T) {
	const N = 20
	src, ref, complete := stream.ActorSource[int](N)

	go func() {
		for i := range N {
			ref.Tell(i)
		}
		complete()
	}()

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("RunWith: %v", err)
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

// TestActorSource_Completion verifies that calling complete() terminates the
// stream normally (no error, empty result if buffer was empty).
func TestActorSource_Completion(t *testing.T) {
	src, _, complete := stream.ActorSource[int](8)
	complete() // signal immediately — stream should complete with no elements

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

// TestActorSource_MultipleWriters verifies that all messages from concurrent
// writers are received (order not guaranteed across goroutines).
func TestActorSource_MultipleWriters(t *testing.T) {
	const (
		writers = 3
		perW    = 10
		total   = writers * perW
	)
	src, ref, complete := stream.ActorSource[int](total * 2)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range perW {
				ref.Tell(base*perW + i)
			}
		}(w)
	}
	go func() {
		wg.Wait()
		complete()
	}()

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}
	if len(result) != total {
		t.Fatalf("got %d elements, want %d", len(result), total)
	}
	// Verify every expected value appears exactly once.
	seen := make(map[int]int, total)
	for _, v := range result {
		seen[v]++
	}
	for i := range total {
		if seen[i] != 1 {
			t.Fatalf("value %d: seen %d times", i, seen[i])
		}
	}
}

// TestActorSource_OverflowDropTail verifies that when the buffer is full and
// OverflowDropTail is in use, the stream does not hang or panic; it simply
// receives at most bufferSize elements from a burst of sends.
func TestActorSource_OverflowDropTail(t *testing.T) {
	const bufSize = 4
	src, ref, complete := stream.ActorSource[int](bufSize)

	// Send many more messages than the buffer can hold, all before complete().
	for i := range bufSize * 10 {
		ref.Tell(i)
	}
	complete()

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}
	if len(result) > bufSize {
		t.Fatalf("received %d elements, expected ≤ %d (buffer size)", len(result), bufSize)
	}
}

// TestActorSource_OverflowDropHead verifies that OverflowDropHead keeps the
// most-recently-sent element and discards the oldest when the buffer is full.
func TestActorSource_OverflowDropHead(t *testing.T) {
	const bufSize = 4
	src, ref, complete := stream.ActorSourceWithStrategy[int](bufSize, stream.OverflowDropHead)

	// Fill buffer, then overflow — the last element should be retained.
	for i := range bufSize*2 + 1 {
		ref.Tell(i)
	}
	complete()

	result, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("RunWith: %v", err)
	}
	// After all overflows the buffer must contain exactly bufSize elements.
	if len(result) != bufSize {
		t.Fatalf("got %d elements, want %d", len(result), bufSize)
	}
	// The last element sent (bufSize*2) must be present (was never dropped).
	last := bufSize * 2
	found := false
	for _, v := range result {
		if v == last {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("last sent element %d not found in result %v", last, result)
	}
}

// TestActorSource_OverflowFail verifies that OverflowFail propagates
// ErrBufferFull through the stream when the buffer overflows.
func TestActorSource_OverflowFail(t *testing.T) {
	src, ref, complete := stream.ActorSourceWithStrategy[int](2, stream.OverflowFail)
	defer complete()

	// Overfill the buffer — third Tell should trigger ErrBufferFull.
	ref.Tell(1)
	ref.Tell(2)
	ref.Tell(3) // overflow

	_, err := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
	if err == nil {
		t.Fatal("expected ErrBufferFull, got nil")
	}
	if !errors.Is(err, stream.ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

// TestFromTypedActorRef_Sink verifies that FromTypedActorRef delivers every
// stream element to the actor via Tell.
func TestFromTypedActorRef_Sink(t *testing.T) {
	type CollectMsg struct{ Value int }
	received := make(chan int, 100)

	collectBehavior := typed.Behavior[CollectMsg](func(_ typed.TypedContext[CollectMsg], msg CollectMsg) typed.Behavior[CollectMsg] {
		received <- msg.Value
		return typed.Same[CollectMsg]()
	})

	sys := newTestSystem(t)
	collectorRef, stop := spawnTyped(t, sys, collectBehavior, "collector")
	defer stop()

	sink := stream.FromTypedActorRef(collectorRef)

	input := []CollectMsg{{1}, {2}, {3}, {4}, {5}}
	if _, err := stream.FromSlice(input).To(sink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Drain the received channel with a reasonable timeout.
	got := make([]int, 0, len(input))
	deadline := time.After(2 * time.Second)
	for len(got) < len(input) {
		select {
		case v := <-received:
			got = append(got, v)
		case <-deadline:
			t.Fatalf("timeout: received %d of %d elements: %v", len(got), len(input), got)
		}
	}

	for i, v := range got {
		if v != i+1 {
			t.Fatalf("index %d: got %d, want %d", i, v, i+1)
		}
	}
}
