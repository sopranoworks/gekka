/*
 * receptionist_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/crdt"
	"github.com/stretchr/testify/assert"
)

func TestReceptionist_Register(t *testing.T) {
	replicator := crdt.NewReplicator("node1", nil)
	behavior := receptionistBehavior(replicator)
	
	// Create typed actor manually for test
	a := typed.NewTypedActor(behavior)
	
	key := NewServiceKey[string]("test-service")
	
	// Mock ref
	mref := &mockTypedRef{path: "/user/service1"}
	typedServiceRef := typed.NewTypedActorRef[string](mref)

	msg := Register[string]{
		Key:     key,
		Service: typedServiceRef,
	}

	a.Receive(msg)

	elements := replicator.ORSet("test-service").Elements()
	assert.Len(t, elements, 1)
	assert.Equal(t, "/user/service1", elements[0])
}

type mockTypedRef struct {
	actor.Ref
	path string
}

func (r *mockTypedRef) Path() string { return r.path }
