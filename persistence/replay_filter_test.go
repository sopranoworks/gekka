/*
 * replay_filter_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// scriptedJournal lets each test compose an exact PersistentRepr stream
// that ReplayMessages will deliver to the callback. Other Journal
// methods are no-ops.
type scriptedJournal struct {
	stream    []PersistentRepr
	delay     time.Duration   // per-event delay
	startWait time.Duration   // delay before first event
}

func (s *scriptedJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	return nil
}
func (s *scriptedJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return 0, nil
}
func (s *scriptedJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return nil
}
func (s *scriptedJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	if s.startWait > 0 {
		select {
		case <-time.After(s.startWait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, repr := range s.stream {
		if s.delay > 0 {
			select {
			case <-time.After(s.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		callback(repr)
	}
	return nil
}

func collectReplay(t *testing.T, j Journal) []PersistentRepr {
	t.Helper()
	var got []PersistentRepr
	if err := j.ReplayMessages(context.Background(), "p", 1, ^uint64(0), ^uint64(0), func(r PersistentRepr) {
		got = append(got, r)
	}); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	return got
}

func TestReplayFilter_Off_PassesEverythingThrough(t *testing.T) {
	stream := []PersistentRepr{
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "B"}, // duplicate
		{PersistenceID: "p", SequenceNr: 2, WriterUuid: "B"},
	}
	jr := NewFilteringJournal(&scriptedJournal{stream: stream}, ReplayFilterConfig{Mode: ReplayFilterOff})
	got := collectReplay(t, jr)
	if len(got) != len(stream) {
		t.Fatalf("expected %d events through, got %d", len(stream), len(got))
	}
}

func TestReplayFilter_RepairByDiscardOld_DropsOldWriter(t *testing.T) {
	stream := []PersistentRepr{
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 2, WriterUuid: "A"},
		// Newer writer overwrites the same seq=2 → A becomes "old".
		{PersistenceID: "p", SequenceNr: 2, WriterUuid: "B"},
		// Any subsequent event from A should be discarded.
		{PersistenceID: "p", SequenceNr: 3, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 3, WriterUuid: "B"},
	}
	jr := NewFilteringJournal(&scriptedJournal{stream: stream}, ReplayFilterConfig{
		Mode: ReplayFilterRepairByDiscardOld, WindowSize: 100, MaxOldWriters: 10,
	})
	got := collectReplay(t, jr)
	for _, r := range got {
		if r.WriterUuid == "A" && r.SequenceNr >= 3 {
			t.Fatalf("filter let A through at seq>=3: %+v", r)
		}
	}
	// Newer writer's seq=3 must still arrive.
	saw := false
	for _, r := range got {
		if r.WriterUuid == "B" && r.SequenceNr == 3 {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("filter dropped newer writer's seq=3")
	}
}

func TestReplayFilter_FailMode_StopsOnAnomaly(t *testing.T) {
	stream := []PersistentRepr{
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "B"},
	}
	jr := NewFilteringJournal(&scriptedJournal{stream: stream}, ReplayFilterConfig{Mode: ReplayFilterFail, WindowSize: 100, MaxOldWriters: 10})
	err := jr.ReplayMessages(context.Background(), "p", 1, ^uint64(0), ^uint64(0), func(PersistentRepr) {})
	if !errors.Is(err, ErrReplayFilterFailed) {
		t.Fatalf("expected ErrReplayFilterFailed, got %v", err)
	}
}

func TestReplayFilter_WarnMode_PassesEverything(t *testing.T) {
	stream := []PersistentRepr{
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "B"},
	}
	jr := NewFilteringJournal(&scriptedJournal{stream: stream}, ReplayFilterConfig{Mode: ReplayFilterWarn, WindowSize: 100, MaxOldWriters: 10})
	got := collectReplay(t, jr)
	if len(got) != 2 {
		t.Fatalf("expected 2 events through; got %d", len(got))
	}
}

func TestReplayFilter_ConfigSettersAreStable(t *testing.T) {
	prev := GetReplayFilterConfig()
	defer SetReplayFilterConfig(prev)
	SetReplayFilterConfig(ReplayFilterConfig{Mode: ReplayFilterFail, WindowSize: 50, MaxOldWriters: 7})
	got := GetReplayFilterConfig()
	if got.Mode != ReplayFilterFail || got.WindowSize != 50 || got.MaxOldWriters != 7 {
		t.Fatalf("config not applied: %+v", got)
	}
}

func TestRecoveryEventTimeout_FiresOnSilence(t *testing.T) {
	inner := &scriptedJournal{
		stream:    []PersistentRepr{{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"}},
		startWait: 200 * time.Millisecond,
	}
	wrap := NewRecoveryEventTimeoutJournal(inner, 30*time.Millisecond)
	err := wrap.ReplayMessages(context.Background(), "p", 1, ^uint64(0), ^uint64(0), func(PersistentRepr) {})
	if !errors.Is(err, ErrRecoveryEventTimeout) {
		t.Fatalf("expected ErrRecoveryEventTimeout, got %v", err)
	}
}

func TestRecoveryEventTimeout_NoSilenceNoFire(t *testing.T) {
	stream := []PersistentRepr{
		{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 2, WriterUuid: "A"},
		{PersistenceID: "p", SequenceNr: 3, WriterUuid: "A"},
	}
	inner := &scriptedJournal{stream: stream, delay: 5 * time.Millisecond}
	wrap := NewRecoveryEventTimeoutJournal(inner, 100*time.Millisecond)

	var got int
	var mu sync.Mutex
	if err := wrap.ReplayMessages(context.Background(), "p", 1, ^uint64(0), ^uint64(0), func(PersistentRepr) {
		mu.Lock()
		got++
		mu.Unlock()
	}); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if got != len(stream) {
		t.Fatalf("expected %d events, got %d", len(stream), got)
	}
}

func TestWrapJournalWithFallbacks_AssemblesChain(t *testing.T) {
	inner := newFakeJournal()
	wrapped := WrapJournalWithFallbacks(inner,
		CircuitBreakerConfig{MaxFailures: 3, CallTimeout: time.Second, ResetTimeout: time.Second},
		ReplayFilterConfig{Mode: ReplayFilterRepairByDiscardOld, WindowSize: 100, MaxOldWriters: 10},
		200*time.Millisecond,
	)
	// Outer is a CircuitBreakerJournal — confirm AsyncWriteMessages
	// reaches the fakeJournal exactly once.
	if err := wrapped.AsyncWriteMessages(context.Background(), []PersistentRepr{{PersistenceID: "p", SequenceNr: 1, WriterUuid: "A"}}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("expected 1 inner call, got %d", inner.calls)
	}
}

func TestWrapJournalWithFallbacks_ZeroValuesArePassThrough(t *testing.T) {
	inner := newFakeJournal()
	wrapped := WrapJournalWithFallbacks(inner, CircuitBreakerConfig{}, ReplayFilterConfig{Mode: ReplayFilterOff}, 0)
	if wrapped != Journal(inner) {
		t.Fatalf("expected pass-through; got wrapper %T", wrapped)
	}
}

func TestWrapSnapshotStoreWithFallbacks_ZeroValueIsPassThrough(t *testing.T) {
	inner := &fakeSnapshotStore{}
	wrapped := WrapSnapshotStoreWithFallbacks(inner, CircuitBreakerConfig{})
	if wrapped != SnapshotStore(inner) {
		t.Fatalf("expected pass-through; got %T", wrapped)
	}
}
