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
