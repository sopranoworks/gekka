/*
 * dispatcher_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// collectActor records every message it receives into a channel.
type collectActor struct {
	actor.BaseActor
	received chan any
}

func newCollectActor() *collectActor {
	return &collectActor{
		BaseActor: actor.NewBaseActor(),
		received:  make(chan any, 64),
	}
}

func (a *collectActor) Receive(msg any) {
	a.received <- msg
}

// ── PinnedDispatcher ──────────────────────────────────────────────────────────

// TestPinnedDispatcher_ProcessesMessages verifies that an actor started with
// DispatcherPinned correctly receives and processes messages. The goroutine is
// pinned to an OS thread via runtime.LockOSThread inside startPinnedImpl.
func TestPinnedDispatcher_ProcessesMessages(t *testing.T) {
	a := newCollectActor()
	actor.StartWithDispatcher(a, actor.DispatcherPinned)

	const n = 10
	for i := 0; i < n; i++ {
		a.Send(i)
	}

	for i := 0; i < n; i++ {
		select {
		case msg := <-a.received:
			if msg != i {
				t.Errorf("message %d: got %v", i, msg)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

// TestPinnedDispatcher_ConcurrentSends verifies that the pinned actor handles
// concurrent senders without data races.
func TestPinnedDispatcher_ConcurrentSends(t *testing.T) {
	a := newCollectActor()
	actor.StartWithDispatcher(a, actor.DispatcherPinned)

	const senders = 5
	const msgsPerSender = 20

	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < msgsPerSender; i++ {
				a.Send(id*100 + i)
			}
		}(s)
	}
	wg.Wait()

	total := senders * msgsPerSender
	got := 0
	deadline := time.After(3 * time.Second)
	for got < total {
		select {
		case <-a.received:
			got++
		case <-deadline:
			t.Fatalf("timed out: received %d/%d messages", got, total)
		}
	}
}

// ── CallingThreadDispatcher ───────────────────────────────────────────────────

// TestCallingThreadDispatcher_Synchronous verifies that messages are processed
// synchronously: after Send returns, the message has already been delivered
// to Receive — no goroutine switch is needed.
func TestCallingThreadDispatcher_Synchronous(t *testing.T) {
	a := newCollectActor()
	actor.StartWithDispatcher(a, actor.DispatcherCallingThread)

	a.Send("first")

	// No timer/goroutine — if the dispatcher is truly synchronous the channel
	// already has the message.
	select {
	case msg := <-a.received:
		if msg != "first" {
			t.Errorf("expected 'first', got %v", msg)
		}
	default:
		t.Fatal("message was not processed synchronously before Send returned")
	}
}

// TestCallingThreadDispatcher_MultipleMessages verifies ordering is preserved
// under synchronous delivery.
func TestCallingThreadDispatcher_MultipleMessages(t *testing.T) {
	a := newCollectActor()
	actor.StartWithDispatcher(a, actor.DispatcherCallingThread)

	msgs := []string{"alpha", "beta", "gamma"}
	for _, m := range msgs {
		a.Send(m)
	}

	for _, want := range msgs {
		select {
		case got := <-a.received:
			if got != want {
				t.Errorf("expected %q, got %v", want, got)
			}
		default:
			t.Fatalf("expected %q to be already delivered", want)
		}
	}
}

// ── LoggingMailbox ────────────────────────────────────────────────────────────

// logCapture implements slog.Handler and records every log record.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}
func (c *logCapture) WithAttrs(_ []slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(_ string) slog.Handler      { return c }

func (c *logCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.records)
}

func (c *logCapture) level(i int) slog.Level {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.records[i].Level
}

// TestLoggingMailbox_LogsEachMessage verifies that NewLoggingMailbox emits one
// log record per enqueued message at the configured level.
func TestLoggingMailbox_LogsEachMessage(t *testing.T) {
	cap := &logCapture{}
	logger := slog.New(cap)

	mf := actor.NewLoggingMailbox(
		actor.NewBoundedMailbox(16, actor.DropNewest),
		slog.LevelDebug,
		logger,
	)

	a := newCollectActor()
	actor.InjectMailbox(a, mf)
	actor.Start(a)

	const n = 3
	for i := 0; i < n; i++ {
		a.Send(i)
	}

	// Drain all received messages to ensure they were processed.
	deadline := time.After(2 * time.Second)
	for got := 0; got < n; got++ {
		select {
		case <-a.received:
		case <-deadline:
			t.Fatalf("timed out waiting for message %d", got)
		}
	}

	if cap.count() != n {
		t.Errorf("expected %d log records, got %d", n, cap.count())
	}
	for i := 0; i < cap.count(); i++ {
		if cap.level(i) != slog.LevelDebug {
			t.Errorf("record %d: expected level DEBUG, got %v", i, cap.level(i))
		}
	}
}

// TestLoggingMailbox_WrapsDefaultChannel verifies that passing nil inner still
// wraps the default channel send (DropNewest semantics via the channel).
func TestLoggingMailbox_WrapsDefaultChannel(t *testing.T) {
	cap := &logCapture{}
	logger := slog.New(cap)

	mf := actor.NewLoggingMailbox(nil, slog.LevelInfo, logger)

	a := newCollectActor()
	actor.InjectMailbox(a, mf)
	actor.Start(a)

	a.Send("hello")
	a.Send("world")

	deadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-a.received:
		case <-deadline:
			t.Fatalf("timed out waiting for message %d", i)
		}
	}

	if cap.count() != 2 {
		t.Errorf("expected 2 log records, got %d", cap.count())
	}
	if cap.count() > 0 && cap.level(0) != slog.LevelInfo {
		t.Errorf("expected level INFO, got %v", cap.level(0))
	}
}
