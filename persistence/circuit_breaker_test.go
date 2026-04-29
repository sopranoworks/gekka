/*
 * circuit_breaker_test.go
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

// fakeJournal is a test double that lets each operation be programmed
// to fail or hang. It records all calls for the assertion phase. The
// implementation is the minimum the tests need — no batching, no
// ordering guarantees beyond append order.
type fakeJournal struct {
	mu             sync.Mutex
	calls          int
	failTimes      int
	hang           bool
	hangCh         chan struct{}
	storedMessages []PersistentRepr
}

func newFakeJournal() *fakeJournal { return &fakeJournal{hangCh: make(chan struct{})} }

func (f *fakeJournal) ReplayMessages(ctx context.Context, persistenceId string, fromSequenceNr, toSequenceNr uint64, max uint64, callback func(PersistentRepr)) error {
	return f.do(ctx, func() error {
		for _, m := range f.storedMessages {
			callback(m)
		}
		return nil
	})
}

func (f *fakeJournal) ReadHighestSequenceNr(ctx context.Context, persistenceId string, fromSequenceNr uint64) (uint64, error) {
	return uint64(len(f.storedMessages)), f.do(ctx, func() error { return nil })
}

func (f *fakeJournal) AsyncWriteMessages(ctx context.Context, messages []PersistentRepr) error {
	return f.do(ctx, func() error {
		f.storedMessages = append(f.storedMessages, messages...)
		return nil
	})
}

func (f *fakeJournal) AsyncDeleteMessagesTo(ctx context.Context, persistenceId string, toSequenceNr uint64) error {
	return f.do(ctx, func() error { return nil })
}

func (f *fakeJournal) do(ctx context.Context, body func() error) error {
	f.mu.Lock()
	f.calls++
	hang := f.hang
	failNow := f.failTimes > 0
	if failNow {
		f.failTimes--
	}
	f.mu.Unlock()
	if hang {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.hangCh:
		}
	}
	if failNow {
		return errors.New("fakeJournal: simulated failure")
	}
	return body()
}

func TestCircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 3, ResetTimeout: 50 * time.Millisecond})
	for i := 0; i < 3; i++ {
		err := cb.Call(context.Background(), func(context.Context) error { return errors.New("boom") })
		if err == nil {
			t.Fatalf("call %d: expected error, got nil", i)
		}
	}
	if got := cb.State(); got != "open" {
		t.Fatalf("after %d failures want state=open, got %s", 3, got)
	}
	err := cb.Call(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("breaker should fast-fail; got err=%v", err)
	}
}

func TestCircuitBreaker_HalfOpenAfterReset(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 20 * time.Millisecond})
	_ = cb.Call(context.Background(), func(context.Context) error { return errors.New("boom") })
	if cb.State() != "open" {
		t.Fatalf("expected breaker open after first failure")
	}
	time.Sleep(40 * time.Millisecond)
	// One trial call permitted; success should re-close.
	if err := cb.Call(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("trial call failed: %v", err)
	}
	if cb.State() != "closed" {
		t.Fatalf("breaker should have closed after successful trial; got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenTrialFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 10 * time.Millisecond})
	_ = cb.Call(context.Background(), func(context.Context) error { return errors.New("boom") })
	time.Sleep(20 * time.Millisecond)
	err := cb.Call(context.Background(), func(context.Context) error { return errors.New("trial fail") })
	if err == nil {
		t.Fatalf("expected trial call to surface error")
	}
	if cb.State() != "open" {
		t.Fatalf("breaker should re-open after failed trial; got %s", cb.State())
	}
}

func TestCircuitBreaker_CallTimeoutCountsAsFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 1, CallTimeout: 5 * time.Millisecond, ResetTimeout: time.Hour})
	err := cb.Call(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, ErrCircuitBreakerCallTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected call-timeout error; got %v", err)
	}
	if cb.State() != "open" {
		t.Fatalf("breaker should be open after timeout; got %s", cb.State())
	}
}

func TestCircuitBreakerJournal_ForwardsToInner(t *testing.T) {
	inner := newFakeJournal()
	cbj := NewCircuitBreakerJournal(inner, CircuitBreakerConfig{MaxFailures: 5, CallTimeout: time.Second, ResetTimeout: time.Second})
	if err := cbj.AsyncWriteMessages(context.Background(), []PersistentRepr{{PersistenceID: "p", SequenceNr: 1}}); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	hi, err := cbj.ReadHighestSequenceNr(context.Background(), "p", 0)
	if err != nil {
		t.Fatalf("highest failed: %v", err)
	}
	if hi != 1 {
		t.Fatalf("expected highest=1, got %d", hi)
	}
	if inner.calls != 2 {
		t.Fatalf("expected 2 inner calls, got %d", inner.calls)
	}
}

func TestCircuitBreakerJournal_OpensAndFastFails(t *testing.T) {
	inner := newFakeJournal()
	inner.failTimes = 100
	cbj := NewCircuitBreakerJournal(inner, CircuitBreakerConfig{MaxFailures: 2, CallTimeout: 100 * time.Millisecond, ResetTimeout: 200 * time.Millisecond})
	for i := 0; i < 2; i++ {
		_ = cbj.AsyncWriteMessages(context.Background(), nil)
	}
	if cbj.Breaker().State() != "open" {
		t.Fatalf("expected breaker open; got %s", cbj.Breaker().State())
	}
	before := inner.calls
	err := cbj.AsyncWriteMessages(context.Background(), nil)
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("expected fast-fail; got %v", err)
	}
	if inner.calls != before {
		t.Fatalf("inner journal was called while breaker open (calls before=%d after=%d)", before, inner.calls)
	}
}

// fakeSnapshotStore is the SnapshotStore counterpart of fakeJournal.
type fakeSnapshotStore struct {
	mu        sync.Mutex
	calls     int
	failTimes int
}

func (s *fakeSnapshotStore) LoadSnapshot(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) (*SelectedSnapshot, error) {
	if err := s.tick(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *fakeSnapshotStore) SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, snapshot any) error {
	return s.tick()
}

func (s *fakeSnapshotStore) DeleteSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	return s.tick()
}

func (s *fakeSnapshotStore) DeleteSnapshots(ctx context.Context, persistenceId string, criteria SnapshotSelectionCriteria) error {
	return s.tick()
}

func (s *fakeSnapshotStore) tick() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failTimes > 0 {
		s.failTimes--
		return errors.New("fakeSnapshot: simulated failure")
	}
	return nil
}

func TestCircuitBreakerSnapshotStore_OpensAfterMaxFailures(t *testing.T) {
	inner := &fakeSnapshotStore{failTimes: 100}
	wrap := NewCircuitBreakerSnapshotStore(inner, CircuitBreakerConfig{MaxFailures: 3, CallTimeout: 50 * time.Millisecond, ResetTimeout: time.Hour})
	for i := 0; i < 3; i++ {
		_ = wrap.SaveSnapshot(context.Background(), SnapshotMetadata{PersistenceID: "p", SequenceNr: uint64(i)}, "snap")
	}
	if wrap.Breaker().State() != "open" {
		t.Fatalf("expected breaker open; got %s", wrap.Breaker().State())
	}
	err := wrap.SaveSnapshot(context.Background(), SnapshotMetadata{PersistenceID: "p", SequenceNr: 99}, "x")
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("expected fast-fail; got %v", err)
	}
}

func TestPackageLevelBreakerConfig_UpdatesAndReverts(t *testing.T) {
	prev := GetJournalBreakerConfig()
	defer SetJournalBreakerConfig(prev)
	SetJournalBreakerConfig(CircuitBreakerConfig{MaxFailures: 7, CallTimeout: 500 * time.Millisecond, ResetTimeout: 5 * time.Second})
	got := GetJournalBreakerConfig()
	if got.MaxFailures != 7 || got.CallTimeout != 500*time.Millisecond || got.ResetTimeout != 5*time.Second {
		t.Fatalf("config not applied: %+v", got)
	}
}
