/*
 * event_adapter_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package persistence

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEventV1 struct {
	Value string
}

type testEventV2 struct {
	Data string
}

type testEventAdapter struct{}

func (a *testEventAdapter) ToJournal(event any) []any {
	switch v := event.(type) {
	case testEventV2:
		return []any{testEventV1{Value: v.Data}} // Down-convert for journal if needed
	default:
		return []any{event}
	}
}

func (a *testEventAdapter) FromJournal(event any, manifest string) (any, error) {
	switch v := event.(type) {
	case testEventV1:
		return testEventV2{Data: v.Value}, nil // Up-convert for application
	default:
		return event, nil
	}
}

func (a *testEventAdapter) Manifest(event any) string {
	switch event.(type) {
	case testEventV1:
		return "v1"
	case testEventV2:
		return "v2"
	default:
		return ""
	}
}

func TestAdaptedJournal(t *testing.T) {
	inner := NewInMemoryJournal()
	aj := NewAdaptedJournal(inner)
	aj.AddAdapter("p1", &testEventAdapter{})

	ctx := context.Background()
	events := []PersistentRepr{
		{PersistenceID: "p1", SequenceNr: 1, Payload: testEventV2{Data: "hello"}},
	}

	err := aj.AsyncWriteMessages(ctx, events)
	require.NoError(t, err)

	// Check inner journal has V1
	inner.ReplayMessages(ctx, "p1", 1, 1, 1, func(m PersistentRepr) {
		assert.IsType(t, testEventV1{}, m.Payload)
		assert.Equal(t, "hello", m.Payload.(testEventV1).Value)
		assert.Equal(t, "v1", m.Manifest)
	})

	// Replay through adapted journal should give V2
	var replayed any
	err = aj.ReplayMessages(ctx, "p1", 1, 1, 1, func(m PersistentRepr) {
		replayed = m.Payload
	})
	require.NoError(t, err)
	assert.IsType(t, testEventV2{}, replayed)
	assert.Equal(t, "hello", replayed.(testEventV2).Data)
}
