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

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	"github.com/sopranoworks/gekka/cluster/ddata"
	"github.com/stretchr/testify/assert"
)

func TestReceptionist_Register(t *testing.T) {
	replicator := ddata.NewReplicator("node1", nil)
	cfg := receptionist.DefaultConfig()
	cfg.PruningInterval = 0 // disable timer for direct-Receive tests
	behavior := receptionist.Behavior(replicator, cfg)

	// Create typed actor manually for test
	a := typed.NewTypedActor(behavior).(*typed.TypedActor[any])

	key := NewServiceKey[string]("test-service")

	// Mock ref
	mref := &mockTypedRef{path: "/user/service1"}
	typedServiceRef := typed.NewTypedActorRef[string](mref)

	msg := receptionist.Register[string]{
		Key:     key,
		Service: typedServiceRef,
	}

	a.Receive(msg)

	bucket := receptionist.ShardKey("test-service", cfg.DistributedKeyCount)
	elements := replicator.ORSet(bucket).Elements()
	assert.Len(t, elements, 1)
	assert.Equal(t, "/user/service1", elements[0])
}

type mockTypedRef struct {
	actor.Ref
	path string
}

func (r *mockTypedRef) Path() string                      { return r.path }
func (r *mockTypedRef) Tell(msg any, sender ...actor.Ref) {}
