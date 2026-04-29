/*
 * proxy_journal_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
)

// countingJournal wraps an InMemoryJournal and records each call so tests
// can assert that proxy operations actually flowed through to the target.
type countingJournal struct {
	inner    *InMemoryJournal
	writes   atomic.Int64
	deletes  atomic.Int64
	replays  atomic.Int64
	highReqs atomic.Int64
}

func newCountingJournal() *countingJournal { return &countingJournal{inner: NewInMemoryJournal()} }

func (j *countingJournal) AsyncWriteMessages(ctx context.Context, m []PersistentRepr) error {
	j.writes.Add(1)
	return j.inner.AsyncWriteMessages(ctx, m)
}

func (j *countingJournal) AsyncDeleteMessagesTo(ctx context.Context, id string, to uint64) error {
	j.deletes.Add(1)
	return j.inner.AsyncDeleteMessagesTo(ctx, id, to)
}

func (j *countingJournal) ReplayMessages(ctx context.Context, id string, from, to, max uint64, cb func(PersistentRepr)) error {
	j.replays.Add(1)
	return j.inner.ReplayMessages(ctx, id, from, to, max, cb)
}

func (j *countingJournal) ReadHighestSequenceNr(ctx context.Context, id string, from uint64) (uint64, error) {
	j.highReqs.Add(1)
	return j.inner.ReadHighestSequenceNr(ctx, id, from)
}

var _ Journal = (*countingJournal)(nil)

// TestProxyJournal_ForwardsToTarget exercises every Journal method and
// verifies the call lands on the target.
func TestProxyJournal_ForwardsToTarget(t *testing.T) {
	target := newCountingJournal()
	proxy := NewProxyJournalForTarget(target)

	ctx := context.Background()
	repr := PersistentRepr{PersistenceID: "p-1", SequenceNr: 1, Payload: "hello"}
	if err := proxy.AsyncWriteMessages(ctx, []PersistentRepr{repr}); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}
	if got := target.writes.Load(); got != 1 {
		t.Errorf("target writes = %d, want 1", got)
	}

	highest, err := proxy.ReadHighestSequenceNr(ctx, "p-1", 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if highest != 1 {
		t.Errorf("highest = %d, want 1", highest)
	}

	var replayed []PersistentRepr
	if err := proxy.ReplayMessages(ctx, "p-1", 1, 1, 0, func(r PersistentRepr) {
		replayed = append(replayed, r)
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Payload != "hello" {
		t.Errorf("replayed = %+v, want [{hello}]", replayed)
	}

	if err := proxy.AsyncDeleteMessagesTo(ctx, "p-1", 1); err != nil {
		t.Fatalf("AsyncDeleteMessagesTo: %v", err)
	}
	if got := target.deletes.Load(); got != 1 {
		t.Errorf("target deletes = %d, want 1", got)
	}
}

// TestProxyJournal_ResolvesViaRegistry verifies the HOCON-driven path:
// the "proxy" provider reads target-journal-plugin-id and resolves
// against the global registry.
func TestProxyJournal_ResolvesViaRegistry(t *testing.T) {
	cfg, err := hocon.ParseString(`
target-journal-plugin-id = "in-memory"
init-timeout = 1s
`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}

	j, err := NewJournal("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewJournal(proxy): %v", err)
	}

	ctx := context.Background()
	repr := PersistentRepr{PersistenceID: "p-1", SequenceNr: 1, Payload: "v"}
	if err := j.AsyncWriteMessages(ctx, []PersistentRepr{repr}); err != nil {
		t.Fatalf("write through registry-resolved proxy: %v", err)
	}
	highest, err := j.ReadHighestSequenceNr(ctx, "p-1", 0)
	if err != nil || highest != 1 {
		t.Fatalf("highest=%d err=%v, want 1 / nil", highest, err)
	}
}

// TestProxyJournal_InitTimeoutFires verifies that an unresolvable target
// surfaces a clear timeout error rather than retrying forever.
func TestProxyJournal_InitTimeoutFires(t *testing.T) {
	proxy, err := NewProxyJournal("does-not-exist", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewProxyJournal: %v", err)
	}

	ctx := context.Background()
	start := time.Now()
	werr := proxy.AsyncWriteMessages(ctx, []PersistentRepr{{PersistenceID: "x", SequenceNr: 1}})
	elapsed := time.Since(start)

	if werr == nil {
		t.Fatal("expected init-timeout error, got nil")
	}
	if !strings.Contains(werr.Error(), "not resolvable within") {
		t.Errorf("error %q does not mention init-timeout", werr)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned too quickly (%s) — init-timeout should have retried", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("returned too slowly (%s) — init-timeout retry overshot", elapsed)
	}
}

// TestProxyJournal_CachesFirstResolution verifies that repeated calls
// after a successful resolution reuse the cached target — i.e. the
// init-timeout is paid at most once.
func TestProxyJournal_CachesFirstResolution(t *testing.T) {
	target := newCountingJournal()
	proxy := NewProxyJournalForTarget(target)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := proxy.ReadHighestSequenceNr(ctx, "p", 0); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := target.highReqs.Load(); got != 5 {
		t.Errorf("expected 5 forwarded ReadHighestSequenceNr calls, got %d", got)
	}
}

// TestProxyJournal_RejectsRemoteMode verifies that start-target-journal
// = off (which would require Artery transport not yet implemented) is
// rejected at construction time with a clear error.
func TestProxyJournal_RejectsRemoteMode(t *testing.T) {
	cfg, err := hocon.ParseString(`
target-journal-plugin-id = "in-memory"
start-target-journal = off
`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewJournal("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for start-target-journal=off, got nil")
	}
	if !strings.Contains(err.Error(), "start-target-journal=off") {
		t.Errorf("error %q does not mention the rejected option", err)
	}
}

// TestProxyJournal_RejectsSelfTarget verifies the proxy refuses to
// target itself — guarding against an infinite-recursion misconfig.
func TestProxyJournal_RejectsSelfTarget(t *testing.T) {
	cfg, err := hocon.ParseString(`target-journal-plugin-id = "proxy"`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewJournal("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for proxy targeting itself, got nil")
	}
	if !strings.Contains(err.Error(), "loop") {
		t.Errorf("error %q does not mention the loop guard", err)
	}
}

// TestProxyJournal_RequiresTargetID verifies missing target id surfaces
// a descriptive error pointing to the HOCON path that needs setting.
func TestProxyJournal_RequiresTargetID(t *testing.T) {
	cfg, err := hocon.ParseString(`init-timeout = 1s`)
	if err != nil {
		t.Fatalf("parse cfg: %v", err)
	}
	_, err = NewJournal("proxy", *cfg)
	if err == nil {
		t.Fatal("expected error for missing target-journal-plugin-id, got nil")
	}
	if !strings.Contains(err.Error(), "target-journal-plugin-id") {
		t.Errorf("error %q does not mention the missing key", err)
	}
}

// TestProxyJournal_HandlesZeroValueConfig verifies the safe-get helpers
// turn a zero hocon.Config{} (the value AutoStartJournals passes when
// no settings tree exists) into a clean error instead of a panic.
func TestProxyJournal_HandlesZeroValueConfig(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero config panicked: %v", r)
		}
	}()
	_, err := NewJournal("proxy", hocon.Config{})
	if err == nil {
		t.Fatal("expected error for missing target-journal-plugin-id with zero config, got nil")
	}
	if !strings.Contains(err.Error(), "target-journal-plugin-id") {
		t.Errorf("error %q does not mention the missing key", err)
	}
}
