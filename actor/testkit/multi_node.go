/*
 * multi_node.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// MultiNodeTestKit creates N in-process actor systems and provides
// synchronization barriers for cluster-style integration tests.
type MultiNodeTestKit struct {
	t     testing.TB
	nodes []*multiNodeSystem

	mu       sync.Mutex
	barriers map[string]chan struct{}
}

// NewMultiNodeTestKit creates a test kit with n independent actor systems.
func NewMultiNodeTestKit(t testing.TB, n int) *MultiNodeTestKit {
	if n < 1 {
		t.Fatal("MultiNodeTestKit: need at least 1 node")
	}
	mk := &MultiNodeTestKit{
		t:        t,
		nodes:    make([]*multiNodeSystem, n),
		barriers: make(map[string]chan struct{}),
	}
	for i := 0; i < n; i++ {
		mk.nodes[i] = newMultiNodeSystem(i)
	}
	return mk
}

// System returns the actor system for node i.
func (mk *MultiNodeTestKit) System(i int) actor.ActorContext {
	mk.t.Helper()
	if i < 0 || i >= len(mk.nodes) {
		mk.t.Fatalf("MultiNodeTestKit: node index %d out of range [0,%d)", i, len(mk.nodes))
	}
	return mk.nodes[i]
}

// NodeCount returns the number of nodes in the test kit.
func (mk *MultiNodeTestKit) NodeCount() int {
	return len(mk.nodes)
}

// Barrier synchronizes all nodes at a named barrier point. It blocks until
// all nodes have called Barrier with the same name. This is useful for
// coordinating test phases across multiple simulated nodes.
func (mk *MultiNodeTestKit) Barrier(name string) {
	mk.mu.Lock()
	ch, ok := mk.barriers[name]
	if !ok {
		ch = make(chan struct{})
		mk.barriers[name] = ch
	}
	mk.mu.Unlock()

	// Use a simple counter-based barrier.
	barrierKey := name + "-count"
	mk.mu.Lock()
	if _, exists := mk.barriers[barrierKey]; !exists {
		mk.barriers[barrierKey] = make(chan struct{}, len(mk.nodes))
	}
	countCh := mk.barriers[barrierKey]
	mk.mu.Unlock()

	// Signal this node has reached the barrier.
	countCh <- struct{}{}

	// Check if all nodes have arrived.
	mk.mu.Lock()
	arrived := len(countCh)
	needed := len(mk.nodes)
	if arrived >= needed {
		// All arrived — close the gate.
		close(ch)
	}
	mk.mu.Unlock()

	// Wait for the barrier to open.
	<-ch
}

// RunOn executes fn on the specified node's actor system. Panics from fn
// are recovered and reported as test failures.
func (mk *MultiNodeTestKit) RunOn(node int, fn func(system actor.ActorContext)) {
	mk.t.Helper()
	if node < 0 || node >= len(mk.nodes) {
		mk.t.Fatalf("MultiNodeTestKit: node %d out of range", node)
	}
	defer func() {
		if r := recover(); r != nil {
			mk.t.Fatalf("MultiNodeTestKit: panic on node %d: %v", node, r)
		}
	}()
	fn(mk.nodes[node])
}

// NewProbe creates a TestProbe. The probe is not tied to any specific node.
func (mk *MultiNodeTestKit) NewProbe(t testing.TB) *TestProbe {
	tk := NewTestKit(nil)
	return tk.NewProbe(t)
}

// multiNodeSystem is a lightweight in-process actor system for testing.
type multiNodeSystem struct {
	nodeIndex int
	mu        sync.Mutex
	actors    map[string]actor.Ref
	ctx       context.Context
	cancel    context.CancelFunc
}

func newMultiNodeSystem(index int) *multiNodeSystem {
	ctx, cancel := context.WithCancel(context.Background())
	return &multiNodeSystem{
		nodeIndex: index,
		actors:    make(map[string]actor.Ref),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *multiNodeSystem) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	a := props.New()
	path := fmt.Sprintf("/user/%s", name)
	ref := &multiNodeRef{path: path, actor: a}

	s.mu.Lock()
	s.actors[path] = ref
	s.mu.Unlock()

	actor.InjectSystem(a, s)
	type selfSetter interface{ SetSelf(actor.Ref) }
	if ss, ok := a.(selfSetter); ok {
		ss.SetSelf(ref)
	}
	actor.StartWithDispatcher(a, actor.DispatcherCallingThread)
	return ref, nil
}

func (s *multiNodeSystem) Spawn(behavior any, name string) (actor.Ref, error) {
	return &multiNodeRef{path: "/user/" + name}, nil
}

func (s *multiNodeSystem) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return &multiNodeRef{path: "/user/anon"}, nil
}

func (s *multiNodeSystem) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return &multiNodeRef{path: "/system/" + name}, nil
}

func (s *multiNodeSystem) Context() context.Context { return s.ctx }

func (s *multiNodeSystem) Watch(watcher actor.Ref, target actor.Ref) {}

func (s *multiNodeSystem) Resolve(path string) (actor.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref, ok := s.actors[path]; ok {
		return ref, nil
	}
	return nil, fmt.Errorf("actor not found: %s", path)
}

func (s *multiNodeSystem) Stop(ref actor.Ref) {
	if r, ok := ref.(*multiNodeRef); ok && r.actor != nil {
		r.actor.PostStop()
	}
}

func (s *multiNodeSystem) ActorSelection(path string) actor.ActorSelection {
	return actor.ActorSelection{
		Anchor: &multiNodeRef{path: "/"},
		Path:   actor.ParseSelectionElements(path),
		System: s,
	}
}

func (s *multiNodeSystem) DeliverSelection(sel actor.ActorSelection, msg any, sender ...actor.Ref) {}
func (s *multiNodeSystem) ResolveSelection(sel actor.ActorSelection, ctx context.Context) (actor.Ref, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *multiNodeSystem) AskSelection(sel actor.ActorSelection, ctx context.Context, msg any) (any, error) {
	return nil, fmt.Errorf("not implemented")
}

// multiNodeRef is a lightweight actor.Ref for the multi-node test system.
type multiNodeRef struct {
	path  string
	actor actor.Actor
}

func (r *multiNodeRef) Path() string { return r.path }
func (r *multiNodeRef) Tell(msg any, sender ...actor.Ref) {
	if r.actor != nil {
		r.actor.Receive(msg)
	}
}
