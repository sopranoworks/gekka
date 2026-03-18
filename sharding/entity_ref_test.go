/*
 * entity_ref_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

type counterMsg struct {
	Value int
}

func TestEntityRef_Tell(t *testing.T) {
	region := &mockRegion{
		received: make(chan ShardingEnvelope, 10),
	}

	ref := NewEntityRef[counterMsg]("Counter", "entity-1", region)
	
	ref.Tell(counterMsg{Value: 42})

	select {
	case env := <-region.received:
		assert.Equal(t, "entity-1", env.EntityId)
		assert.Equal(t, "sharding.counterMsg", env.MessageManifest)
		// Note: message is JSON encoded in current implementation
		assert.Contains(t, string(env.Message), `"Value":42`)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message not received by region")
	}
}

func TestEntityRef_Ask(t *testing.T) {
	// Ask is currently a stub in entity_ref.go
	region := &mockRegion{}
	ref := NewEntityRef[string]("TestType", "entity-1", region)
	
	_, err := ref.Ask(context.Background(), 100*time.Millisecond, "ping")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not fully implemented")
}

type mockRegion struct {
	actor.Ref
	received chan ShardingEnvelope
}

func (m *mockRegion) Tell(msg any, _ ...actor.Ref) {
	if env, ok := msg.(ShardingEnvelope); ok {
		if m.received != nil {
			m.received <- env
		}
	}
}

func (m *mockRegion) Path() string { return "/user/test-region" }
