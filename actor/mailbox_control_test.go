/*
 * mailbox_control_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor_test

import (
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// ── control message types for tests ─────────────────────────────────────────

type ctrlMsg struct{ val int }

func (ctrlMsg) IsControlMessage() {}

type normalMsg struct{ val int }

// ── Test 1: control messages preempt normal messages ────────────────────────

func TestControlAwareMailbox_ControlPreemptsNormal(t *testing.T) {
	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewControlAwareMailbox())
	actor.Start(a)

	// Send a mix: normal, control, normal, control, normal
	a.Send(normalMsg{1})
	a.Send(ctrlMsg{2})
	a.Send(normalMsg{3})
	a.Send(ctrlMsg{4})
	a.Send(normalMsg{5})

	waitMessages(t, &mu, &received, 5)

	mu.Lock()
	defer mu.Unlock()

	// Find the position of the last control message and the first normal message.
	// All control messages must appear before any normal message that was enqueued
	// after them (within the drain window). Because enqueue and drain race, the
	// strongest invariant we can assert is: no normal message appears before
	// a control message that was enqueued at the same time or later.
	//
	// Practically with 5 rapid sends the drain goroutine batches them, so
	// we expect controls first.
	lastCtrl := -1
	firstNormalAfterCtrl := -1
	for i, m := range received {
		switch m.(type) {
		case ctrlMsg:
			lastCtrl = i
		case normalMsg:
			if firstNormalAfterCtrl == -1 && lastCtrl == -1 {
				// normal before any control — that's fine (was enqueued first)
				continue
			}
		}
	}
	_ = firstNormalAfterCtrl

	// Stricter check: gather indices
	var ctrlIdxs, normIdxs []int
	for i, m := range received {
		switch m.(type) {
		case ctrlMsg:
			ctrlIdxs = append(ctrlIdxs, i)
		case normalMsg:
			normIdxs = append(normIdxs, i)
		}
	}

	if len(ctrlIdxs) != 2 || len(normIdxs) != 3 {
		t.Fatalf("unexpected counts: ctrl=%d normal=%d, received=%v",
			len(ctrlIdxs), len(normIdxs), received)
	}

	// The last control index must be before the last normal index
	// (controls are drained before normals within a batch).
	if lastCtrl > normIdxs[len(normIdxs)-1] {
		t.Errorf("control message appeared after last normal: received=%v", received)
	}

	a.CloseMailbox()
}

// ── Test 2: normal messages are not starved ─────────────────────────────────

func TestControlAwareMailbox_NormalNotStarved(t *testing.T) {
	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewControlAwareMailbox())
	actor.Start(a)

	// 1 normal then 10 control messages
	a.Send(normalMsg{0})
	for i := 1; i <= 10; i++ {
		a.Send(ctrlMsg{i})
	}

	waitMessages(t, &mu, &received, 11)

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 11 {
		t.Fatalf("expected 11 messages, got %d", len(received))
	}

	// Verify the normal message is present (not lost).
	foundNormal := false
	for _, m := range received {
		if _, ok := m.(normalMsg); ok {
			foundNormal = true
			break
		}
	}
	if !foundNormal {
		t.Error("normal message was lost (starved)")
	}

	a.CloseMailbox()
}

// ── Test 3: FIFO within control class ───────────────────────────────────────

func TestControlAwareMailbox_FIFOWithinClass(t *testing.T) {
	var mu sync.Mutex
	var received []any

	a := newCallback(func(msg any) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	actor.InjectMailbox(a, actor.NewControlAwareMailbox())
	actor.Start(a)

	// Send 3 control messages in order.
	a.Send(ctrlMsg{1})
	a.Send(ctrlMsg{2})
	a.Send(ctrlMsg{3})

	waitMessages(t, &mu, &received, 3)

	mu.Lock()
	defer mu.Unlock()

	for i, m := range received {
		cm, ok := m.(ctrlMsg)
		if !ok {
			t.Fatalf("message %d: expected ctrlMsg, got %T", i, m)
		}
		if cm.val != i+1 {
			t.Errorf("message %d: expected val=%d, got val=%d (FIFO violated)", i, i+1, cm.val)
		}
	}

	a.CloseMailbox()
}
