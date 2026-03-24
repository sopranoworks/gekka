/*
 * router_receptionist_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package receptionist

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/stretchr/testify/assert"
)

func TestReceptionistGroupRouter(t *testing.T) {
	received := make(chan string, 10)

	// Manual setup of receptionist-like environment
	sys := &receptionistTestBridge{
		callbacks: make(map[string]func([]string)),
		actors:    make(map[string]actor.Ref),
	}

	router := NewReceptionistGroup[string]("test-service", &actor.RoundRobinRoutingLogic{})

	// Create router actor
	rActor := typed.NewTypedActor(router.Behavior()).(*typed.TypedActor[any])
	rActor.SetSystem(sys)
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	rActor.PreStart()

	// 1. Initial state (no routees)
	rActor.Receive("ping1")
	select {
	case <-received:
		t.Fatal("Message should have been dropped")
	default:
	}

	// 2. Setup mock workers
	sys.actors["/user/worker1"] = &workerMock{path: "/user/worker1", out: received}
	sys.actors["/user/worker2"] = &workerMock{path: "/user/worker2", out: received}

	// 3. Simulate discovery update
	sys.notify("test-service", []string{"/user/worker1", "/user/worker2"})
	rActor.Receive(listing[string]{paths: []string{"/user/worker1", "/user/worker2"}})

	// 4. Route messages
	rActor.Receive("msg1")
	rActor.Receive("msg2")

	assert.Equal(t, "msg1", <-received)
	assert.Equal(t, "msg2", <-received)
}

type receptionistTestBridge struct {
	actor.ActorContext
	callbacks map[string]func([]string)
	actors    map[string]actor.Ref
}

func (b *receptionistTestBridge) SubscribeToReceptionist(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) {
	b.callbacks[keyID] = callback
}

func (b *receptionistTestBridge) notify(keyID string, paths []string) {
	if cb, ok := b.callbacks[keyID]; ok {
		cb(paths)
	}
}

func (b *receptionistTestBridge) Resolve(path string) (actor.Ref, error) {
	if r, ok := b.actors[path]; ok {
		return r, nil
	}
	return &typedMockRef{path: path}, nil
}

type workerMock struct {
	actor.Ref
	path string
	out  chan string
}

func (m *workerMock) Path() string { return m.path }
func (m *workerMock) Tell(msg any, sender ...actor.Ref) {
	if s, ok := msg.(string); ok {
		m.out <- s
	}
}

type typedMockRef struct {
	actor.Ref
	path string
}

func (r *typedMockRef) Path() string                      { return r.path }
func (r *typedMockRef) Tell(msg any, sender ...actor.Ref) {}
