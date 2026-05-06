/*
 * system_message_redelivery_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package core

import (
	"errors"
	"testing"
	"time"
)

func TestSystemMessageOutbox_DefaultCapacity(t *testing.T) {
	o := NewSystemMessageOutbox(0)
	if got, want := o.Capacity(), DefaultSystemMessageBufferSize; got != want {
		t.Errorf("Capacity() with zero arg = %d, want %d (DefaultSystemMessageBufferSize)", got, want)
	}
	if got := o.Len(); got != 0 {
		t.Errorf("Len() on fresh outbox = %d, want 0", got)
	}
}

func TestSystemMessageOutbox_Enqueue_PreservesOrder(t *testing.T) {
	o := NewSystemMessageOutbox(8)
	now := time.Now()
	for i := uint64(1); i <= 5; i++ {
		if err := o.Enqueue(i, []byte{byte(i)}, now); err != nil {
			t.Fatalf("Enqueue(%d) returned %v", i, err)
		}
	}
	snap := o.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("Snapshot() len = %d, want 5", len(snap))
	}
	for i, e := range snap {
		if e.seqNo != uint64(i+1) {
			t.Errorf("Snapshot()[%d].seqNo = %d, want %d", i, e.seqNo, i+1)
		}
	}
}

func TestSystemMessageOutbox_Enqueue_ReturnsFullErrAtCapacity(t *testing.T) {
	o := NewSystemMessageOutbox(2)
	now := time.Now()
	if err := o.Enqueue(1, []byte("a"), now); err != nil {
		t.Fatalf("Enqueue(1) = %v", err)
	}
	if err := o.Enqueue(2, []byte("b"), now); err != nil {
		t.Fatalf("Enqueue(2) = %v", err)
	}
	err := o.Enqueue(3, []byte("c"), now)
	if !errors.Is(err, ErrSystemOutboxFull) {
		t.Errorf("Enqueue(3) on full buffer = %v, want ErrSystemOutboxFull", err)
	}
	// Buffer must be unchanged on overflow.
	if got := o.Len(); got != 2 {
		t.Errorf("Len() after overflow = %d, want 2", got)
	}
}

func TestSystemMessageOutbox_PruneAcked_Cumulative(t *testing.T) {
	o := NewSystemMessageOutbox(8)
	now := time.Now()
	for _, seq := range []uint64{1, 2, 3, 4, 5} {
		if err := o.Enqueue(seq, []byte{byte(seq)}, now); err != nil {
			t.Fatal(err)
		}
	}

	// Ack of seq=3 must prune {1,2,3}.
	if pruned := o.PruneAcked(3); pruned != 3 {
		t.Errorf("PruneAcked(3) = %d, want 3", pruned)
	}
	snap := o.Snapshot()
	if len(snap) != 2 || snap[0].seqNo != 4 || snap[1].seqNo != 5 {
		t.Errorf("after PruneAcked(3), snap = %+v, want [4,5]", snap)
	}

	// Ack of seq=10 (beyond all) prunes the rest.
	if pruned := o.PruneAcked(10); pruned != 2 {
		t.Errorf("PruneAcked(10) on 2-entry buffer = %d, want 2", pruned)
	}
	if got := o.Len(); got != 0 {
		t.Errorf("Len() after prune-all = %d, want 0", got)
	}
}

func TestSystemMessageOutbox_PruneAcked_NoMatchIsNoOp(t *testing.T) {
	o := NewSystemMessageOutbox(8)
	now := time.Now()
	if err := o.Enqueue(5, []byte("x"), now); err != nil {
		t.Fatal(err)
	}
	if pruned := o.PruneAcked(2); pruned != 0 {
		t.Errorf("PruneAcked(2) on buffer with min seqNo=5 = %d, want 0", pruned)
	}
	if got := o.Len(); got != 1 {
		t.Errorf("Len() after no-op prune = %d, want 1", got)
	}
}

func TestSystemMessageOutbox_Snapshot_IsDefensiveCopy(t *testing.T) {
	o := NewSystemMessageOutbox(4)
	now := time.Now()
	if err := o.Enqueue(1, []byte("orig"), now); err != nil {
		t.Fatal(err)
	}
	snap := o.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("Snapshot len = %d, want 1", len(snap))
	}
	// Mutate the snapshot — must not affect the buffer.
	snap[0].seqNo = 999
	snap2 := o.Snapshot()
	if snap2[0].seqNo != 1 {
		t.Errorf("buffer was affected by external mutation: seqNo = %d, want 1", snap2[0].seqNo)
	}
}

func TestSystemMessageOutbox_MarkResent_UpdatesLastAttempt(t *testing.T) {
	o := NewSystemMessageOutbox(4)
	first := time.Now()
	if err := o.Enqueue(7, []byte("z"), first); err != nil {
		t.Fatal(err)
	}
	later := first.Add(2 * time.Second)
	o.MarkResent(7, later)
	snap := o.Snapshot()
	if !snap[0].firstAttempt.Equal(first) {
		t.Errorf("firstAttempt = %v, want %v (must not change on resend)", snap[0].firstAttempt, first)
	}
	if !snap[0].lastAttempt.Equal(later) {
		t.Errorf("lastAttempt = %v, want %v", snap[0].lastAttempt, later)
	}
}

func TestSystemMessageOutbox_MarkResent_AbsentSeqIsNoOp(t *testing.T) {
	o := NewSystemMessageOutbox(4)
	now := time.Now()
	if err := o.Enqueue(1, []byte("a"), now); err != nil {
		t.Fatal(err)
	}
	o.MarkResent(99, now.Add(time.Minute)) // seqNo not present
	snap := o.Snapshot()
	if !snap[0].lastAttempt.Equal(now) {
		t.Errorf("lastAttempt of unrelated entry was modified: got %v, want %v", snap[0].lastAttempt, now)
	}
}

func TestSystemMessageOutbox_Drain_ClearsAndReportsCount(t *testing.T) {
	o := NewSystemMessageOutbox(4)
	now := time.Now()
	for _, seq := range []uint64{1, 2, 3} {
		if err := o.Enqueue(seq, []byte{byte(seq)}, now); err != nil {
			t.Fatal(err)
		}
	}
	if dropped := o.Drain(); dropped != 3 {
		t.Errorf("Drain() = %d, want 3", dropped)
	}
	if got := o.Len(); got != 0 {
		t.Errorf("Len() after Drain = %d, want 0", got)
	}
	// Drain on empty is 0.
	if dropped := o.Drain(); dropped != 0 {
		t.Errorf("Drain() on empty = %d, want 0", dropped)
	}
}

func TestSystemMessageOutbox_OldestFirstAttempt(t *testing.T) {
	o := NewSystemMessageOutbox(4)
	if got := o.OldestFirstAttempt(); !got.IsZero() {
		t.Errorf("OldestFirstAttempt() on empty = %v, want zero time", got)
	}
	t1 := time.Now()
	t2 := t1.Add(5 * time.Second)
	t3 := t1.Add(10 * time.Second)

	if err := o.Enqueue(1, []byte("a"), t1); err != nil {
		t.Fatal(err)
	}
	if err := o.Enqueue(2, []byte("b"), t2); err != nil {
		t.Fatal(err)
	}
	if err := o.Enqueue(3, []byte("c"), t3); err != nil {
		t.Fatal(err)
	}
	if got := o.OldestFirstAttempt(); !got.Equal(t1) {
		t.Errorf("OldestFirstAttempt() = %v, want %v (head entry's firstAttempt)", got, t1)
	}

	// Pruning the head must advance OldestFirstAttempt to the new head.
	o.PruneAcked(1)
	if got := o.OldestFirstAttempt(); !got.Equal(t2) {
		t.Errorf("OldestFirstAttempt() after prune = %v, want %v", got, t2)
	}
}

func TestSystemMessageOutbox_ConcurrentAccessIsSafe(t *testing.T) {
	// Smoke test for the mutex — exercise concurrent enqueue/prune/snapshot
	// to ensure no race trips under -race. Correctness is covered by the
	// other tests; this one just guards against lock-omission regressions.
	o := NewSystemMessageOutbox(1024)
	now := time.Now()
	done := make(chan struct{})
	go func() {
		for i := uint64(1); i <= 200; i++ {
			_ = o.Enqueue(i, []byte("p"), now)
		}
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		default:
			_ = o.Snapshot()
			_ = o.PruneAcked(50)
			_ = o.Len()
		}
	}
}
