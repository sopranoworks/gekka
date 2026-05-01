/*
 * replicator_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplicator_Entries_Empty(t *testing.T) {
	r := NewReplicator("node-1", nil)
	if got := r.Entries(); len(got) != 0 {
		t.Errorf("want empty, got %v", got)
	}
}

func TestReplicator_Entries_OneOfEach(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.GCounter("counter-1")
	r.ORSet("set-1")
	r.LWWMap("map-1")
	r.PNCounter("pn-1")
	r.ORFlag("flag-1")
	r.LWWRegister("reg-1")

	got := r.Entries()
	if len(got) != 6 {
		t.Fatalf("want 6 entries, got %d: %v", len(got), got)
	}
	byType := map[string]string{}
	for _, e := range got {
		byType[e.Type] = e.Key
	}
	want := map[string]string{
		"gcounter":    "counter-1",
		"orset":       "set-1",
		"lwwmap":      "map-1",
		"pncounter":   "pn-1",
		"orflag":      "flag-1",
		"lwwregister": "reg-1",
	}
	for typ, expectedKey := range want {
		if byType[typ] != expectedKey {
			t.Errorf("type %q: want key %q, got %q", typ, expectedKey, byType[typ])
		}
	}
}

func TestReplicator_LookupGCounter_HitAndMiss(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.GCounter("existing")

	got, ok := r.LookupGCounter("existing")
	if !ok || got == nil {
		t.Errorf("hit: want (non-nil, true), got (%v, %v)", got, ok)
	}
	got, ok = r.LookupGCounter("missing")
	if ok || got != nil {
		t.Errorf("miss: want (nil, false), got (%v, %v)", got, ok)
	}
	// Crucial: Lookup must NOT have created "missing".
	if _, exists := r.counters["missing"]; exists {
		t.Error("Lookup on miss must not create the entry")
	}
}

func TestReplicator_LookupORSet_HitAndMiss(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.ORSet("existing")
	got, ok := r.LookupORSet("existing")
	if !ok || got == nil {
		t.Errorf("hit: want (non-nil, true), got (%v, %v)", got, ok)
	}
	_, ok = r.LookupORSet("missing")
	if ok {
		t.Error("miss: expected ok=false")
	}
	if _, exists := r.sets["missing"]; exists {
		t.Error("Lookup on miss must not create the entry")
	}
}

// TestReplicator_LogDataSizeExceeding_FiresAtThreshold verifies the round-2
// session 16 acceptance behavior for
// `pekko.cluster.distributed-data.log-data-size-exceeding`: when the
// JSON-serialized gossip payload exceeds the configured threshold, the warn
// hook fires with the offending key and observed size.
func TestReplicator_LogDataSizeExceeding_FiresAtThreshold(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.LogDataSizeExceeding = 128

	var (
		mu    sync.Mutex
		fires []struct {
			Type string
			Key  string
			Size int
		}
	)
	r.LogDataSizeExceedingHook = func(msgType, key string, size int) {
		mu.Lock()
		fires = append(fires, struct {
			Type string
			Key  string
			Size int
		}{msgType, key, size})
		mu.Unlock()
	}

	// Below threshold: a tiny gossip envelope with no payload serializes to
	// well under 128 bytes — hook must NOT fire.
	r.sendToPeers(context.Background(), ReplicatorMsg{Type: "x", Key: "k"}, WriteAll)
	mu.Lock()
	if got := len(fires); got != 0 {
		mu.Unlock()
		t.Fatalf("below threshold: hook fired %d time(s), want 0", got)
	}
	mu.Unlock()

	// Above threshold: stuff Payload with valid JSON well over 128 bytes.
	bigPayload := []byte(`{"data":"` + strings.Repeat("a", 200) + `"}`)
	r.sendToPeers(context.Background(), ReplicatorMsg{
		Type:    "gcounter-gossip",
		Key:     "huge-counter",
		Payload: bigPayload,
	}, WriteAll)

	mu.Lock()
	defer mu.Unlock()
	if len(fires) != 1 {
		t.Fatalf("above threshold: hook fired %d time(s), want 1", len(fires))
	}
	got := fires[0]
	if got.Type != "gcounter-gossip" {
		t.Errorf("Type = %q, want gcounter-gossip", got.Type)
	}
	if got.Key != "huge-counter" {
		t.Errorf("Key = %q, want huge-counter", got.Key)
	}
	if got.Size <= r.LogDataSizeExceeding {
		t.Errorf("Size = %d, want > %d (threshold)", got.Size, r.LogDataSizeExceeding)
	}
}

// TestReplicator_LogDataSizeExceeding_DisabledByZero verifies that setting
// the threshold to zero suppresses the warn hook entirely.
func TestReplicator_LogDataSizeExceeding_DisabledByZero(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.LogDataSizeExceeding = 0

	called := false
	r.LogDataSizeExceedingHook = func(_, _ string, _ int) { called = true }

	bigPayload := []byte(`"` + strings.Repeat("a", 4096) + `"`)
	r.sendToPeers(context.Background(), ReplicatorMsg{
		Type:    "gcounter-gossip",
		Key:     "any",
		Payload: bigPayload,
	}, WriteAll)
	if called {
		t.Error("hook fired with threshold=0; want suppressed")
	}
}

// TestReplicator_WaitForRecovery_TimesOutWithoutPeers covers the round-2
// session 16 wiring of `pekko.cluster.distributed-data.recovery-timeout`:
// without any registered peers WaitForRecovery returns true (timed out).
func TestReplicator_WaitForRecovery_TimesOutWithoutPeers(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.RecoveryTimeout = 30 * time.Millisecond
	timedOut := r.WaitForRecovery(context.Background())
	if !timedOut {
		t.Fatal("WaitForRecovery() = false, want true (deadline elapsed)")
	}
}

// TestReplicator_WaitForRecovery_ReturnsImmediatelyWithPeer verifies that
// WaitForRecovery returns false (not timed out) when at least one peer is
// already registered before Start.
func TestReplicator_WaitForRecovery_ReturnsImmediatelyWithPeer(t *testing.T) {
	r := NewReplicator("node-1", nil)
	r.RecoveryTimeout = 5 * time.Second
	r.AddPeer("pekko://Sys@127.0.0.1:2552/user/goReplicator")
	timedOut := r.WaitForRecovery(context.Background())
	if timedOut {
		t.Error("WaitForRecovery() = true, want false when a peer is registered")
	}
}
