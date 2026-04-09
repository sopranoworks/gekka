/*
 * sink_restart_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
)

// ─── ActorRefWithBackpressure ────────────────────────────────────────────

func TestActorRefWithBackpressure(t *testing.T) {
	var mu sync.Mutex
	var received []int
	ackCh := make(chan struct{}, 1)

	// Create a mock actor.Ref that records Tell calls
	mockRef := &bpTestActorRef{
		tellFn: func(msg any) {
			if v, ok := msg.(int); ok {
				mu.Lock()
				received = append(received, v)
				mu.Unlock()
				// Simulate slow processing then ack
				time.Sleep(10 * time.Millisecond)
				ackCh <- struct{}{}
			}
		},
	}

	ref := typed.NewTypedActorRef[int](mockRef)
	src := FromSlice([]int{1, 2, 3})
	sink := ActorRefWithBackpressure[int](ref, ackCh)

	start := time.Now()
	_, err := RunWith(src, sink, SyncMaterializer{})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("want 3 messages, got %d", len(received))
	}
	// With back-pressure, 3 elements * 10ms each = ~30ms minimum
	if elapsed < 25*time.Millisecond {
		t.Fatalf("too fast: %v (expected back-pressure delay)", elapsed)
	}
}

// bpTestActorRef implements actor.Ref for testing back-pressure sink.
type bpTestActorRef struct {
	tellFn func(any)
}

func (r *bpTestActorRef) Tell(msg any, _ ...actor.Ref) { r.tellFn(msg) }
func (r *bpTestActorRef) Path() string                  { return "/test/bp-actor" }

// ─��─ RestartSink ───────��─────────────────────��───────────────────────────

func TestRestartSink_RestartsOnFailure(t *testing.T) {
	failCount := 0
	var collected []int

	settings := RestartSettings{
		MinBackoff:  1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		MaxRestarts: 3,
	}

	src := FromSlice([]int{1, 2, 3})
	sink := RestartSink(settings, func() Sink[int, NotUsed] {
		return Sink[int, NotUsed]{
			runWith: func(upstream iterator[int]) (NotUsed, error) {
				for {
					elem, ok, err := upstream.next()
					if err != nil {
						return NotUsed{}, err
					}
					if !ok {
						return NotUsed{}, nil
					}
					collected = append(collected, elem)
					if failCount == 0 && elem == 2 {
						failCount++
						return NotUsed{}, errors.New("sink failure")
					}
				}
			},
		}
	})

	_, err := RunWith(src, sink, SyncMaterializer{})
	if err != nil {
		t.Fatal(err)
	}

	// First run: collected 1, 2, then failed
	// Second run: collected 3, then completed
	if len(collected) < 3 {
		t.Fatalf("want at least 3 collected elements, got %d: %v", len(collected), collected)
	}
}

func TestRestartSink_ExhaustsRestarts(t *testing.T) {
	boom := errors.New("persistent failure")

	settings := RestartSettings{
		MinBackoff:  1 * time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		MaxRestarts: 2,
	}

	src := FromSlice([]int{1})
	sink := RestartSink(settings, func() Sink[int, NotUsed] {
		return Sink[int, NotUsed]{
			runWith: func(upstream iterator[int]) (NotUsed, error) {
				return NotUsed{}, boom
			},
		}
	})

	_, err := RunWith(src, sink, SyncMaterializer{})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom error after exhausting restarts, got %v", err)
	}
}
