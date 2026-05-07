/*
 * proxy_transport_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
)

func mustParseHOCON(t *testing.T, s string) hocon.Config {
	t.Helper()
	cfg, err := hocon.ParseString(s)
	if err != nil {
		t.Fatalf("parse hocon: %v", err)
	}
	return *cfg
}

// inMemoryTransport is a test ProxyTransport that routes requests
// directly to an in-process handler keyed by target address.  It is the
// equivalent of "we already trust the bytes" — no Artery, no
// serialization — so the unit tests can exercise the wire-format
// roundtrip and the proxy plumbing without spinning up clusters.
type inMemoryTransport struct {
	mu       sync.Mutex
	handlers map[string]func(ctx context.Context, req []byte) ([]byte, error)
	hangs    map[string]bool // address → drop request, never reply
}

func newInMemoryTransport() *inMemoryTransport {
	return &inMemoryTransport{
		handlers: make(map[string]func(ctx context.Context, req []byte) ([]byte, error)),
		hangs:    make(map[string]bool),
	}
}

func (t *inMemoryTransport) register(address string, h func(ctx context.Context, req []byte) ([]byte, error)) {
	t.mu.Lock()
	t.handlers[address] = h
	t.mu.Unlock()
}

func (t *inMemoryTransport) hang(address string) {
	t.mu.Lock()
	t.hangs[address] = true
	t.mu.Unlock()
}

func (t *inMemoryTransport) Ask(ctx context.Context, targetAddress string, payload []byte, timeout time.Duration) ([]byte, error) {
	t.mu.Lock()
	hang := t.hangs[targetAddress]
	h, ok := t.handlers[targetAddress]
	t.mu.Unlock()
	if hang {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, fmt.Errorf("persistence: proxy transport timeout (%s)", timeout)
		}
	}
	if !ok {
		return nil, fmt.Errorf("persistence: no handler for %s", targetAddress)
	}
	type result struct {
		bytes []byte
		err   error
	}
	done := make(chan result, 1)
	go func() {
		b, err := h(ctx, payload)
		done <- result{b, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(timeout):
		return nil, fmt.Errorf("persistence: proxy transport timeout (%s)", timeout)
	case r := <-done:
		return r.bytes, r.err
	}
}

// ── RemoteJournal roundtrip ────────────────────────────────────────────────────

func TestRemoteJournal_WriteReplayHighestDelete(t *testing.T) {
	target := NewInMemoryJournal()
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:1/system/persistence/journal-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeJournalRequest(ctx, target, req)
	})

	r := NewRemoteJournal(transport, addr, 5*time.Second)
	ctx := context.Background()

	msgs := []PersistentRepr{
		{PersistenceID: "a", SequenceNr: 1, Payload: "one"},
		{PersistenceID: "a", SequenceNr: 2, Payload: "two"},
	}
	if err := r.AsyncWriteMessages(ctx, msgs); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}

	hi, err := r.ReadHighestSequenceNr(ctx, "a", 0)
	if err != nil {
		t.Fatalf("ReadHighestSequenceNr: %v", err)
	}
	if hi != 2 {
		t.Errorf("highest seq = %d, want 2", hi)
	}

	var got []PersistentRepr
	if err := r.ReplayMessages(ctx, "a", 1, 2, 100, func(m PersistentRepr) {
		got = append(got, m)
	}); err != nil {
		t.Fatalf("ReplayMessages: %v", err)
	}
	if len(got) != 2 || got[0].SequenceNr != 1 || got[1].SequenceNr != 2 {
		t.Fatalf("replayed = %+v, want seq 1 and 2", got)
	}
	if got[0].Payload != "one" || got[1].Payload != "two" {
		t.Errorf("payloads = %v / %v, want \"one\" / \"two\"", got[0].Payload, got[1].Payload)
	}

	if err := r.AsyncDeleteMessagesTo(ctx, "a", 1); err != nil {
		t.Fatalf("AsyncDeleteMessagesTo: %v", err)
	}
	got = nil
	if err := r.ReplayMessages(ctx, "a", 1, 2, 100, func(m PersistentRepr) {
		if !m.Deleted {
			got = append(got, m)
		}
	}); err != nil {
		t.Fatalf("ReplayMessages after delete: %v", err)
	}
	if len(got) != 1 || got[0].SequenceNr != 2 {
		t.Errorf("after delete-to-1, replay = %+v, want only seq 2", got)
	}
}

func TestRemoteJournal_TimeoutMapsToError(t *testing.T) {
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:2/system/persistence/journal-target"
	transport.hang(addr)

	r := NewRemoteJournal(transport, addr, 50*time.Millisecond)
	err := r.AsyncWriteMessages(context.Background(), []PersistentRepr{{PersistenceID: "x", SequenceNr: 1}})
	if err == nil {
		t.Fatal("AsyncWriteMessages on hung transport: want error, got nil")
	}
	if !strings.Contains(err.Error(), "timeout") && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want timeout-flavoured", err)
	}
}

func TestRemoteJournal_ReplayPropagatesRemoteError(t *testing.T) {
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:3/system/persistence/journal-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeJournalRequest(ctx, &errorJournal{err: errors.New("disk on fire")}, req)
	})
	r := NewRemoteJournal(transport, addr, time.Second)
	err := r.ReplayMessages(context.Background(), "a", 1, 1, 1, func(PersistentRepr) {})
	if err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Errorf("ReplayMessages: err = %v, want \"disk on fire\"", err)
	}
}

// ── RemoteSnapshotStore roundtrip ──────────────────────────────────────────────

func TestRemoteSnapshotStore_SaveLoadDelete(t *testing.T) {
	target := NewInMemorySnapshotStore()
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:4/system/persistence/snapshot-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeSnapshotRequest(ctx, target, req)
	})
	r := NewRemoteSnapshotStore(transport, addr, 5*time.Second)
	ctx := context.Background()

	meta := SnapshotMetadata{PersistenceID: "p", SequenceNr: 7, Timestamp: 1234}
	if err := r.SaveSnapshot(ctx, meta, "snap-payload"); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	sel, err := r.LoadSnapshot(ctx, "p", LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if sel == nil {
		t.Fatal("LoadSnapshot returned nil")
	}
	if sel.Metadata.SequenceNr != 7 || sel.Metadata.Timestamp != 1234 {
		t.Errorf("metadata = %+v, want seq=7 ts=1234", sel.Metadata)
	}
	if sel.Snapshot != "snap-payload" {
		t.Errorf("snapshot = %v, want \"snap-payload\"", sel.Snapshot)
	}

	if err := r.DeleteSnapshot(ctx, meta); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	sel, err = r.LoadSnapshot(ctx, "p", LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot after delete: %v", err)
	}
	if sel != nil {
		t.Errorf("after DeleteSnapshot: load returned %+v, want nil", sel)
	}
}

func TestRemoteSnapshotStore_DeleteSnapshotsByCriteria(t *testing.T) {
	target := NewInMemorySnapshotStore()
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:5/system/persistence/snapshot-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeSnapshotRequest(ctx, target, req)
	})
	r := NewRemoteSnapshotStore(transport, addr, 5*time.Second)
	ctx := context.Background()

	for seq := uint64(1); seq <= 3; seq++ {
		if err := r.SaveSnapshot(ctx, SnapshotMetadata{PersistenceID: "p", SequenceNr: seq, Timestamp: int64(seq * 10)}, fmt.Sprintf("snap-%d", seq)); err != nil {
			t.Fatalf("SaveSnapshot %d: %v", seq, err)
		}
	}
	if err := r.DeleteSnapshots(ctx, "p", SnapshotSelectionCriteria{MaxSequenceNr: 2, MaxTimestamp: int64(1 << 62)}); err != nil {
		t.Fatalf("DeleteSnapshots: %v", err)
	}
	sel, err := r.LoadSnapshot(ctx, "p", LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if sel == nil || sel.Metadata.SequenceNr != 3 {
		t.Errorf("after DeleteSnapshots(<=2): latest = %+v, want seq=3", sel)
	}
}

// ── Off-mode HOCON wiring ──────────────────────────────────────────────────────

func TestProxyJournalOffMode_RoutesViaRegisteredTransport(t *testing.T) {
	target := NewInMemoryJournal()
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:6/system/persistence/journal-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeJournalRequest(ctx, target, req)
	})

	prev := CurrentProxyTransport()
	RegisterProxyTransport(transport)
	defer RegisterProxyTransport(prev)

	cfgStr := fmt.Sprintf(`
target-journal-plugin-id = ignored
start-target-journal = off
target-journal-address = "%s"
init-timeout = 1s
request-timeout = 5s
`, addr)
	cfg := mustParseHOCON(t, cfgStr)
	j, err := NewJournal("proxy", cfg)
	if err != nil {
		t.Fatalf("NewJournal proxy off-mode: %v", err)
	}
	if err := j.AsyncWriteMessages(context.Background(), []PersistentRepr{{PersistenceID: "k", SequenceNr: 1, Payload: "v"}}); err != nil {
		t.Fatalf("AsyncWriteMessages: %v", err)
	}
	hi, err := target.ReadHighestSequenceNr(context.Background(), "k", 0)
	if err != nil || hi != 1 {
		t.Errorf("target highest = %d (err %v), want 1", hi, err)
	}
}

func TestProxyJournalOffMode_RequiresRegisteredTransport(t *testing.T) {
	prev := CurrentProxyTransport()
	RegisterProxyTransport(nil)
	defer RegisterProxyTransport(prev)

	cfg := mustParseHOCON(t, `
target-journal-plugin-id = ignored
start-target-journal = off
target-journal-address = "pekko://Sys@127.0.0.1:7/system/persistence/journal-target"
`)
	if _, err := NewJournal("proxy", cfg); err == nil || !strings.Contains(strings.ToLower(err.Error()), "transport") {
		t.Errorf("expected transport-required error, got %v", err)
	}
}

func TestProxyJournalOffMode_RequiresAddress(t *testing.T) {
	prev := CurrentProxyTransport()
	RegisterProxyTransport(newInMemoryTransport())
	defer RegisterProxyTransport(prev)

	cfg := mustParseHOCON(t, `
target-journal-plugin-id = ignored
start-target-journal = off
`)
	if _, err := NewJournal("proxy", cfg); err == nil || !strings.Contains(err.Error(), "target-journal-address") {
		t.Errorf("expected address-required error, got %v", err)
	}
}

func TestProxySnapshotOffMode_RoutesViaRegisteredTransport(t *testing.T) {
	target := NewInMemorySnapshotStore()
	transport := newInMemoryTransport()
	const addr = "pekko://Sys@127.0.0.1:8/system/persistence/snapshot-target"
	transport.register(addr, func(ctx context.Context, req []byte) ([]byte, error) {
		return ServeSnapshotRequest(ctx, target, req)
	})

	prev := CurrentProxyTransport()
	RegisterProxyTransport(transport)
	defer RegisterProxyTransport(prev)

	cfgStr := fmt.Sprintf(`
target-snapshot-store-plugin-id = ignored
start-target-snapshot-store = off
target-snapshot-store-address = "%s"
init-timeout = 1s
request-timeout = 5s
`, addr)
	cfg := mustParseHOCON(t, cfgStr)
	s, err := NewSnapshotStore("proxy", cfg)
	if err != nil {
		t.Fatalf("NewSnapshotStore proxy off-mode: %v", err)
	}
	if err := s.SaveSnapshot(context.Background(), SnapshotMetadata{PersistenceID: "p", SequenceNr: 1, Timestamp: 99}, "x"); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := target.LoadSnapshot(context.Background(), "p", LatestSnapshotCriteria())
	if err != nil || got == nil || got.Metadata.SequenceNr != 1 {
		t.Errorf("target load = %+v (err %v), want seq=1", got, err)
	}
}

func TestProxySnapshotOffMode_RequiresAddress(t *testing.T) {
	prev := CurrentProxyTransport()
	RegisterProxyTransport(newInMemoryTransport())
	defer RegisterProxyTransport(prev)

	cfg := mustParseHOCON(t, `
target-snapshot-store-plugin-id = ignored
start-target-snapshot-store = off
`)
	if _, err := NewSnapshotStore("proxy", cfg); err == nil || !strings.Contains(err.Error(), "target-snapshot-store-address") {
		t.Errorf("expected address-required error, got %v", err)
	}
}

// ── helpers ────────────────────────────────────────────────────────────────────

// errorJournal returns the same error from every call.
type errorJournal struct{ err error }

func (e *errorJournal) AsyncWriteMessages(context.Context, []PersistentRepr) error { return e.err }
func (e *errorJournal) AsyncDeleteMessagesTo(context.Context, string, uint64) error {
	return e.err
}
func (e *errorJournal) ReplayMessages(context.Context, string, uint64, uint64, uint64, func(PersistentRepr)) error {
	return e.err
}
func (e *errorJournal) ReadHighestSequenceNr(context.Context, string, uint64) (uint64, error) {
	return 0, e.err
}
