/*
 * router_receptionist_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReceptionistGroupRouter(t *testing.T) {
	received := make(chan string, 10)

	// Mock behavior for routees
	workerBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		received <- msg
		return Same[string]()
	}

	// Manual setup of receptionist-like environment
	sys := &receptionistTestBridge{
		callbacks: make(map[string]func([]string)),
	}

	router := NewReceptionistGroup[string]("test-service", &RoundRobinRoutingLogic{})
	
	// Create router actor
	rActor := &typedActor[any]{
		BaseActor: NewBaseActor(),
		behavior:  router.Behavior(),
	}
	rActor.ctx = &typedContext[any]{actor: rActor}
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	rActor.setSystem(sys) // Bridge interface requirement

	// 1. Initial state (no routees)
	rActor.Receive("ping1")
	// Should log warn and drop (checked via absence of receipt)
	select {
	case <-received:
		t.Fatal("Message should have been dropped")
	default:
	}

	// 2. Simulate discovery update
	sys.notify("test-service", []string{"/user/worker1", "/user/worker2"})
	
	// Process listing message (internal)
	rActor.Receive(listing[string]{paths: []string{"/user/worker1", "/user/worker2"}})

	// 3. Setup mock workers in the bridge
	w1 := &typedActor[string]{BaseActor: NewBaseActor(), behavior: workerBehavior}
	w1.SetSelf(&typedMockRef{path: "/user/worker1"})
	w2 := &typedActor[string]{BaseActor: NewBaseActor(), behavior: workerBehavior}
	w2.SetSelf(&typedMockRef{path: "/user/worker2"})
	sys.actors = map[string]Actor{"/user/worker1": w1, "/user/worker2": w2}

	// 4. Route messages
	rActor.Receive("msg1")
	rActor.Receive("msg2")

	assert.Equal(t, "msg1", <-received)
	assert.Equal(t, "msg2", <-received)
}

type receptionistTestBridge struct {
	ActorContext
	callbacks map[string]func([]string)
	actors    map[string]Actor
}

func (b *receptionistTestBridge) SubscribeToReceptionist(keyID string, subscriber TypedActorRef[any], callback func([]string)) {
	b.callbacks[keyID] = callback
}

func (b *receptionistTestBridge) notify(keyID string, paths []string) {
	if cb, ok := b.callbacks[keyID]; ok {
		cb(paths)
	}
}

func (b *receptionistTestBridge) Resolve(path string) (Ref, error) {
	if a, ok := b.actors[path]; ok {
		return a.Self(), nil
	}
	return &typedMockRef{path: path}, nil
}
