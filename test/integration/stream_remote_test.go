/*
 * stream_remote_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package integration_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/stream"
)

func setupTestNode(t *testing.T) *gekka.Cluster {
	node, err := gekka.NewCluster(gekka.ClusterConfig{
		SystemName: "TestSystem",
		Host:       "127.0.0.1",
		Port:       0,
	})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	return node
}

// ─── SourceRef tests ──────────────────────────────────────────────────────

func TestSourceRef_GoToGo_AllElementsDelivered(t *testing.T) {
	const N = 150
	node := setupTestNode(t)
	defer node.Terminate()

	src := stream.FromSlice(makeInts(N))
	ref, err := stream.ToSourceRef(gekka.AsActorContext(node.System, ""), src, intEncode, intDecode)
	if err != nil {
		t.Fatalf("ToSourceRef: %v", err)
	}

	result, err := stream.RunWith(
		stream.FromSourceRef(gekka.AsActorContext(node.System, ""), ref),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("FromSourceRef: %v", err)
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

func TestSourceRef_GoToGo_SlowConsumer_NoDrops(t *testing.T) {
	const N = 40
	node := setupTestNode(t)
	defer node.Terminate()

	ref, err := stream.ToSourceRef(
		gekka.AsActorContext(node.System, ""),
		stream.FromSlice(makeInts(N)),
		intEncode, intDecode,
	)
	if err != nil {
		t.Fatalf("ToSourceRef: %v", err)
	}

	var received []int
	sink := stream.Foreach(func(n int) {
		time.Sleep(2 * time.Millisecond)
		received = append(received, n)
	})
	if _, err := stream.FromSourceRef(gekka.AsActorContext(node.System, ""), ref).To(sink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(received) != N {
		t.Fatalf("got %d elements, want %d", len(received), N)
	}
}

func TestSourceRef_GoToGo_BackPressureLimitsInflight(t *testing.T) {
	const N = 100
	const bufSize = stream.DefaultAsyncBufSize
	node := setupTestNode(t)
	defer node.Terminate()

	var produced atomic.Int64
	src := stream.FromIteratorFunc(func() (int, bool, error) {
		n := int(produced.Add(1)) - 1
		if n >= N {
			return 0, false, nil
		}
		return n, true, nil
	})
	ref, err := stream.ToSourceRef(gekka.AsActorContext(node.System, ""), src, intEncode, intDecode)
	if err != nil {
		t.Fatalf("ToSourceRef: %v", err)
	}

	var consumed atomic.Int64
	sink := stream.Foreach(func(n int) {
		time.Sleep(10 * time.Millisecond)
		c := consumed.Add(1)
		p := produced.Load()
		// Producer must not be more than bufSize+5 ahead of consumer.
		if p-c > int64(bufSize)+5 {
			t.Errorf("too many in-flight: produced=%d consumed=%d diff=%d",
				p, c, p-c)
		}
	})

	if _, err := stream.FromSourceRef(gekka.AsActorContext(node.System, ""), ref).To(sink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSourceRef_GoToGo_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("source actor failure")
	node := setupTestNode(t)
	defer node.Terminate()

	ref, err := stream.ToSourceRef(
		gekka.AsActorContext(node.System, ""),
		stream.Failed[int](sentinelErr),
		intEncode, intDecode,
	)
	if err != nil {
		t.Fatalf("ToSourceRef: %v", err)
	}

	_, err = stream.RunWith(
		stream.FromSourceRef(gekka.AsActorContext(node.System, ""), ref),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ─── SinkRef tests ────────────────────────────────────────────────────────

func TestSinkRef_GoToGo_AllElementsDelivered(t *testing.T) {
	const N = 100
	node := setupTestNode(t)
	defer node.Terminate()

	resultCh := make(chan []int, 1)
	var received []int
	count := 0
	sink := stream.Foreach(func(v int) {
		received = append(received, v)
		count++
		if count == N {
			cp := make([]int, N)
			copy(cp, received)
			resultCh <- cp
		}
	})

	sinkRef, err := stream.ToSinkRef(gekka.AsActorContext(node.System, ""), sink, intDecode, intEncode)
	if err != nil {
		t.Fatalf("ToSinkRef: %v", err)
	}

	_, err = stream.RunWith(
		stream.FromSlice(makeInts(N)),
		stream.FromSinkRef(gekka.AsActorContext(node.System, ""), sinkRef),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	select {
	case result := <-resultCh:
		if len(result) != N {
			t.Fatalf("got %d elements, want %d", len(result), N)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for results")
	}
}

// ─── Backpressure Stress Test ─────────────────────────────────────────────

func TestSourceRef_Artery_StressBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	node1 := setupTestNode(t)
	defer node1.Terminate()
	node2 := setupTestNode(t)
	defer node2.Terminate()

	// Ensure they are connected (not strictly necessary for loopback but good for realism)
	node1.System.RemoteActorOf(node2.SelfAddress(), "/user/dummy")

	// Constrain the test to 100 elements to avoid excessive test duration.
	const N = 100
	var produced atomic.Int64
	src := stream.FromIteratorFunc(func() (int, bool, error) {
		n := int(produced.Add(1)) - 1
		return n, true, nil // Infinite fast source
	})

	ref, err := stream.ToSourceRef(gekka.AsActorContext(node1.System, ""), src, intEncode, intDecode)
	if err != nil {
		t.Fatalf("ToSourceRef: %v", err)
	}

	var consumed atomic.Int64
	// Very slow consumer: 10ms per element.
	sink := stream.Foreach(func(n int) {
		time.Sleep(10 * time.Millisecond)
		consumed.Add(1)
	})

	done := make(chan error, 1)
	go func() {
		_, err := stream.Via(stream.FromSourceRef(gekka.AsActorContext(node2.System, ""), ref), stream.Take[int](N)).To(sink).Run(stream.ActorMaterializer{})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Stress test timed out")
	}

	p := produced.Load()
	c := consumed.Load()
	t.Logf("Stress test finished: produced=%d, consumed=%d", p, c)

	// Producer should not be too far ahead.
	// Buffer is 16. Plus some slack for Artery delivery and staging.
	if p-c > 50 {
		t.Errorf("Backpressure failed: produced=%d, consumed=%d, diff=%d", p, c, p-c)
	}
}
