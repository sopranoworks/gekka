/*
 * entity_ref_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/cluster/sharding"
	"github.com/stretchr/testify/assert"
)

type counterMsg struct {
	Value int
}

func TestEntityRef_Tell(t *testing.T) {
	region := &MockRegion{
		Received: make(chan sharding.ShardingEnvelope, 10),
	}

	ref := NewEntityRef[counterMsg]("Counter", "entity-1", region)

	ref.Tell(counterMsg{Value: 42})

	select {
	case env := <-region.Received:
		assert.Equal(t, "entity-1", env.EntityId)
		// manifest is derived from reflect.TypeOf(msg).String()
		assert.Equal(t, "typed.counterMsg", env.MessageManifest)
		// Note: message is JSON encoded
		assert.Contains(t, string(env.Message), `"Value":42`)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message not received by region")
	}
}

func TestEntityRef_Ask(t *testing.T) {
	// Ask is currently a stub in entity_ref.go
	region := &MockRegion{}
	ref := NewEntityRef[string]("TestType", "entity-1", region)

	_, err := ref.Ask(context.Background(), 100*time.Millisecond, "ping")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not fully implemented")
}

type MockRegion struct {
	actor.Ref
	Received chan sharding.ShardingEnvelope
}

func (m *MockRegion) Tell(msg any, _ ...actor.Ref) {
	if env, ok := msg.(sharding.ShardingEnvelope); ok {
		if m.Received != nil {
			m.Received <- env
		}
	}
}

func (m *MockRegion) Path() string { return "/user/test-region" }
