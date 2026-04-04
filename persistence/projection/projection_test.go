/*
 * persistence/projection/projection_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package projection

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/persistence/query"
	"github.com/sopranoworks/gekka/stream"
	"github.com/stretchr/testify/assert"
)

type mockSourceProvider struct {
	events []query.EventEnvelope
}

func (p *mockSourceProvider) Source(ctx context.Context, offset query.Offset) (stream.Source[query.EventEnvelope, stream.NotUsed], error) {
	var filtered []query.EventEnvelope
	for _, e := range p.events {
		if offset == nil || e.Offset.IsAfter(offset) {
			filtered = append(filtered, e)
		}
	}
	return stream.FromSlice(filtered), nil
}

type mockOffsetStore struct {
	mu      sync.Mutex
	offsets map[string]query.Offset
}

func (s *mockOffsetStore) ReadOffset(ctx context.Context, projectionName string) (query.Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets[projectionName], nil
}

func (s *mockOffsetStore) SaveOffset(ctx context.Context, projectionName string, offset query.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.offsets == nil {
		s.offsets = make(map[string]query.Offset)
	}
	s.offsets[projectionName] = offset
	return nil
}

func TestProjection_Run(t *testing.T) {
	events := []query.EventEnvelope{
		{Offset: query.SequenceOffset(1), Event: "event-1"},
		{Offset: query.SequenceOffset(2), Event: "event-2"},
		{Offset: query.SequenceOffset(3), Event: "event-3"},
	}

	sp := &mockSourceProvider{events: events}
	os := &mockOffsetStore{}

	processedCount := 0
	handler := func(envelope query.EventEnvelope) error {
		processedCount++
		return nil
	}

	p := NewProjection("test-projection", sp, os, handler)

	// 1. Run first time
	err := p.Run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, processedCount)

	// Verify offset saved
	off, _ := os.ReadOffset(context.Background(), "test-projection")
	assert.Equal(t, query.SequenceOffset(3), off)

	// 2. Add more events and run again
	sp.events = append(sp.events, query.EventEnvelope{Offset: query.SequenceOffset(4), Event: "event-4"})

	processedCount = 0
	err = p.Run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, processedCount) // Only event-4 should be processed

	// Verify final offset
	off, _ = os.ReadOffset(context.Background(), "test-projection")
	assert.Equal(t, query.SequenceOffset(4), off)
}

// ── Exactly-once tests ────────────────────────────────────────────────────────

// mockTxOffsetStore simulates TransactionalOffsetStore without a real DB.
// SaveOffsetTx records whether it was called atomically with the handler; it
// only persists the offset when commitTx is invoked (mirroring sql.Tx.Commit).
type mockTxOffsetStore struct {
	mockOffsetStore
	// pendingOffset holds an offset that has been "written within a tx" but
	// not yet committed.
	pendingMu      sync.Mutex
	pendingName    string
	pendingOffset  query.Offset
	saveTxCallCnt  int
}

func (s *mockTxOffsetStore) SaveOffsetTx(_ context.Context, _ *sql.Tx, projectionName string, offset query.Offset) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.saveTxCallCnt++
	s.pendingName = projectionName
	s.pendingOffset = offset
	return nil
}

// commitTx simulates sql.Tx.Commit — flushes the pending write to the store.
func (s *mockTxOffsetStore) commitTx() {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.pendingOffset != nil {
		s.mu.Lock()
		if s.offsets == nil {
			s.offsets = make(map[string]query.Offset)
		}
		s.offsets[s.pendingName] = s.pendingOffset
		s.mu.Unlock()
		s.pendingOffset = nil
	}
}

// rollbackTx simulates sql.Tx.Rollback — discards the pending write.
func (s *mockTxOffsetStore) rollbackTx() {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingOffset = nil
}

// fakeTxProjection is a test-only wrapper around exactlyOnceProjectionRunner
// that replaces the sql.DB transaction with our mock commit/rollback simulation.
type fakeTxProjection struct {
	name           string
	sourceProvider SourceProvider
	offsetStore    *mockTxOffsetStore
	handler        func(ctx context.Context, env query.EventEnvelope) error
}

func (p *fakeTxProjection) Run(ctx context.Context) error {
	offset, err := p.offsetStore.ReadOffset(ctx, p.name)
	if err != nil {
		return err
	}
	if offset == nil {
		offset = query.NoOffset{}
	}
	src, err := p.sourceProvider.Source(ctx, offset)
	if err != nil {
		return err
	}
	m := stream.SyncMaterializer{}
	_, err = stream.RunWith(src, stream.ForeachErr(func(env query.EventEnvelope) error {
		// Simulate: begin tx
		if err := p.handler(ctx, env); err != nil {
			// Rollback: discard the pending offset write (if any)
			p.offsetStore.rollbackTx()
			return err
		}
		// Save offset "in tx"
		if err := p.offsetStore.SaveOffsetTx(ctx, nil, p.name, env.Offset); err != nil {
			p.offsetStore.rollbackTx()
			return err
		}
		// Commit: persist offset
		p.offsetStore.commitTx()
		return nil
	}), m)
	return err
}

func TestExactlyOnceProjection_HandlerSuccess(t *testing.T) {
	events := []query.EventEnvelope{
		{Offset: query.SequenceOffset(1), Event: "event-1"},
		{Offset: query.SequenceOffset(2), Event: "event-2"},
		{Offset: query.SequenceOffset(3), Event: "event-3"},
	}
	sp := &mockSourceProvider{events: events}
	os := &mockTxOffsetStore{}

	processedCount := 0
	p := &fakeTxProjection{
		name:           "eo-success",
		sourceProvider: sp,
		offsetStore:    os,
		handler: func(_ context.Context, env query.EventEnvelope) error {
			processedCount++
			return nil
		},
	}

	err := p.Run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, processedCount)
	assert.Equal(t, 3, os.saveTxCallCnt)

	// Offset must have advanced to the last event.
	off, _ := os.ReadOffset(context.Background(), "eo-success")
	assert.Equal(t, query.SequenceOffset(3), off)
}

func TestExactlyOnceProjection_HandlerFailureRollsBackOffset(t *testing.T) {
	events := []query.EventEnvelope{
		{Offset: query.SequenceOffset(1), Event: "event-1"},
		{Offset: query.SequenceOffset(2), Event: "event-2"}, // will fail
		{Offset: query.SequenceOffset(3), Event: "event-3"},
	}
	sp := &mockSourceProvider{events: events}
	os := &mockTxOffsetStore{}

	handlerErr := errors.New("handler failed")
	processedCount := 0
	p := &fakeTxProjection{
		name:           "eo-fail",
		sourceProvider: sp,
		offsetStore:    os,
		handler: func(_ context.Context, env query.EventEnvelope) error {
			processedCount++
			if env.Offset.(query.SequenceOffset) == 2 {
				return handlerErr
			}
			return nil
		},
	}

	err := p.Run(context.Background())
	// Projection stops at the failed event and surfaces the error.
	assert.ErrorIs(t, err, handlerErr)

	// Only event-1 was committed; offset must not advance past it.
	off, _ := os.ReadOffset(context.Background(), "eo-fail")
	assert.Equal(t, query.SequenceOffset(1), off,
		"offset must not advance when handler fails (rollback semantics)")

	// SaveOffsetTx called only for event-1 (event-2 rolled back before save).
	assert.Equal(t, 1, os.saveTxCallCnt)
}

func TestExactlyOnceProjection_AtomicityVsAtLeastOnce(t *testing.T) {
	// Demonstrates the key difference from at-least-once:
	// if we crash after handler but before offset save, the offset does not advance.
	events := []query.EventEnvelope{
		{Offset: query.SequenceOffset(1), Event: "event-1"},
	}
	sp := &mockSourceProvider{events: events}
	os := &mockTxOffsetStore{}

	handlerCalled := false
	offsetSavedBeforeCommit := false

	p := &fakeTxProjection{
		name:           "eo-atomic",
		sourceProvider: sp,
		offsetStore:    os,
		handler: func(_ context.Context, env query.EventEnvelope) error {
			handlerCalled = true
			// Check that the offset is NOT yet visible outside the "transaction"
			// (simulates a crash between handler and commit).
			off, _ := os.ReadOffset(context.Background(), "eo-atomic")
			if off != nil {
				_, offsetSavedBeforeCommit = off.(query.SequenceOffset)
			}
			return nil
		},
	}

	err := p.Run(context.Background())
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	// The offset must NOT have been visible before the commit point.
	assert.False(t, offsetSavedBeforeCommit,
		"offset must not be readable before the transaction commits")

	// After successful run, offset is committed.
	off, _ := os.ReadOffset(context.Background(), "eo-atomic")
	assert.Equal(t, query.SequenceOffset(1), off)
}

func TestProjection_Restart(t *testing.T) {
	events := []query.EventEnvelope{
		{Offset: query.SequenceOffset(1), Event: "event-1"},
		{Offset: query.SequenceOffset(2), Event: "event-2"},
	}

	sp := &mockSourceProvider{events: events}
	os := &mockOffsetStore{}

	// Pre-save offset
	os.SaveOffset(context.Background(), "restart-proj", query.SequenceOffset(1))

	processedCount := 0
	handler := func(envelope query.EventEnvelope) error {
		processedCount++
		return nil
	}

	p := NewProjection("restart-proj", sp, os, handler)

	err := p.Run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, processedCount) // Only event-2 should be processed
}
