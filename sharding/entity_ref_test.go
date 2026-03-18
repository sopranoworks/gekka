/*
 * entity_ref_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package sharding

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

func TestEntityRef_Tell(t *testing.T) {
	region := &mockRegion{
		received: make(chan ShardingEnvelope, 10),
	}

	ref := NewEntityRef[string]("TestType", "entity-1", region)
	
	ref.Tell("hello-sharding")

	select {
	case env := <-region.received:
		assert.Equal(t, "entity-1", env.EntityId)
		assert.Equal(t, "string", env.MessageManifest)
		// Note: message is JSON encoded in current implementation
		assert.Contains(t, string(env.Message), "hello-sharding")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message not received by region")
	}
}

type mockRegion struct {
	actor.Ref
	received chan ShardingEnvelope
}

func (m *mockRegion) Tell(msg any, _ ...actor.Ref) {
	if env, ok := msg.(ShardingEnvelope); ok {
		m.received <- env
	}
}

func (m *mockRegion) Path() string { return "/user/test-region" }
