/*
 * queue_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"testing"
)

func TestQueue_OfferAndCollect(t *testing.T) {
	src := Queue[int](8, OverflowBackpressure)

	// We need to run in a goroutine because SyncMaterializer blocks.
	var result []int
	var runErr error
	done := make(chan struct{})

	// Get the queue from factory directly for testing.
	iter, q := src.factory()

	go func() {
		defer close(done)
		sink := Collect[int]()
		result, runErr = sink.runWith(iter)
	}()

	for i := 1; i <= 5; i++ {
		if err := q.Offer(i); err != nil {
			t.Fatalf("Offer(%d) failed: %v", i, err)
		}
	}
	q.Complete()
	<-done

	if runErr != nil {
		t.Fatalf("unexpected error: %v", runErr)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 elements, got %d", len(result))
	}
	for i, v := range result {
		if v != i+1 {
			t.Errorf("result[%d] = %d, want %d", i, v, i+1)
		}
	}
}

func TestQueue_OverflowDropHead(t *testing.T) {
	src := Queue[int](3, OverflowDropHead)
	iter, q := src.factory()

	// Fill buffer
	for i := 1; i <= 3; i++ {
		if err := q.Offer(i); err != nil {
			t.Fatalf("Offer(%d) failed: %v", i, err)
		}
	}

	// Overflow: should drop head (oldest)
	if err := q.Offer(4); err != nil {
		t.Fatalf("Offer(4) failed: %v", err)
	}
	if err := q.Offer(5); err != nil {
		t.Fatalf("Offer(5) failed: %v", err)
	}

	q.Complete()

	sink := Collect[int]()
	result, err := sink.runWith(iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With buffer size 3, after offering 1,2,3,4,5 with DropHead,
	// oldest elements are evicted. We should get recent elements.
	if len(result) == 0 {
		t.Fatal("expected some elements")
	}
	// The last element should always be 5
	if result[len(result)-1] != 5 {
		t.Errorf("last element = %d, want 5", result[len(result)-1])
	}
}

func TestQueue_CompletionTerminates(t *testing.T) {
	src := Queue[int](4, OverflowBackpressure)
	iter, q := src.factory()

	q.Offer(42)
	q.Complete()

	// After complete, Offer should return ErrQueueClosed
	if err := q.Offer(99); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Offer after Complete: got %v, want ErrQueueClosed", err)
	}

	sink := Collect[int]()
	result, err := sink.runWith(iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0] != 42 {
		t.Errorf("got %v, want [42]", result)
	}
}

func TestQueue_Fail(t *testing.T) {
	src := Queue[int](4, OverflowBackpressure)
	iter, q := src.factory()

	q.Offer(1)
	testErr := errors.New("test failure")
	q.Fail(testErr)

	sink := Collect[int]()
	result, err := sink.runWith(iter)
	// Should drain buffered element then get error
	if len(result) != 1 || result[0] != 1 {
		t.Errorf("got %v, want [1]", result)
	}
	if !errors.Is(err, testErr) {
		t.Errorf("got error %v, want %v", err, testErr)
	}
}

func TestQueue_OverflowFail(t *testing.T) {
	src := Queue[int](2, OverflowFail)
	_, q := src.factory()

	q.Offer(1)
	q.Offer(2)
	err := q.Offer(3)
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
	q.Complete()
}
