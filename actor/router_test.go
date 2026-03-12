/*
 * router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// collectActor records every message it receives.
type collectActor struct {
	BaseActor
	mu       sync.Mutex
	received []any
	notify   chan struct{}
}

func newCollectActor() *collectActor {
	return &collectActor{
		BaseActor: NewBaseActor(),
		notify:    make(chan struct{}, 256),
	}
}

func (a *collectActor) Receive(msg any) {
	a.mu.Lock()
	a.received = append(a.received, msg)
	a.mu.Unlock()
	select {
	case a.notify <- struct{}{}:
	default:
	}
}

// routeeMock is a Ref that captures Tell calls.
type routeeMock struct {
	path     string
	mu       sync.Mutex
	messages []any
	senders  []Ref
}

func (m *routeeMock) Path() string { return m.path }
func (m *routeeMock) Tell(msg any, sender ...Ref) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	var s Ref
	if len(sender) > 0 {
		s = sender[0]
	}
	m.senders = append(m.senders, s)
}
func (m *routeeMock) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

// ── RoutingLogic interface ────────────────────────────────────────────────────

func TestRoutingLogic_IsInterface(t *testing.T) {
	// Verify that a custom struct can implement RoutingLogic.
	var _ RoutingLogic = &RoundRobinRoutingLogic{}
}

// ── RoundRobinRoutingLogic ────────────────────────────────────────────────────

func TestRoundRobin_SelectRotates(t *testing.T) {
	logic := &RoundRobinRoutingLogic{}
	r0 := &routeeMock{path: "r0"}
	r1 := &routeeMock{path: "r1"}
	r2 := &routeeMock{path: "r2"}
	routees := []Ref{r0, r1, r2}

	got := []string{
		logic.Select(nil, routees).Path(),
		logic.Select(nil, routees).Path(),
		logic.Select(nil, routees).Path(),
		logic.Select(nil, routees).Path(), // wraps back to r0
	}

	want := []string{"r0", "r1", "r2", "r0"}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("Select[%d] = %q, want %q", i, g, want[i])
		}
	}
}

func TestRoundRobin_EmptyRoutees_ReturnsNil(t *testing.T) {
	logic := &RoundRobinRoutingLogic{}
	if got := logic.Select(nil, []Ref{}); got != nil {
		t.Errorf("Select on empty routees = %v, want nil", got)
	}
}

func TestRoundRobin_ConcurrentSelect_NoPanic(t *testing.T) {
	logic := &RoundRobinRoutingLogic{}
	routees := []Ref{&routeeMock{path: "a"}, &routeeMock{path: "b"}, &routeeMock{path: "c"}}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logic.Select("msg", routees)
		}()
	}
	wg.Wait()
}

// ── NewRouterActor ────────────────────────────────────────────────────────────

func TestNewRouterActor_CopiesRoutees(t *testing.T) {
	originals := []Ref{&routeeMock{path: "a"}, &routeeMock{path: "b"}}
	router := NewRouterActor(&RoundRobinRoutingLogic{}, originals)

	// Mutating the original slice must not affect the router.
	originals[0] = &routeeMock{path: "mutated"}

	routees := router.Routees
	if routees[0].Path() != "a" {
		t.Errorf("router routee[0] = %q, want %q (slice was not copied)", routees[0].Path(), "a")
	}
}

func TestNewRouterActor_HasValidMailbox(t *testing.T) {
	router := NewRouterActor(&RoundRobinRoutingLogic{}, nil)
	if router.Mailbox() == nil {
		t.Error("router mailbox is nil")
	}
}

// ── RouterActor.AddRoutee / RemoveRoutee ──────────────────────────────────────

func TestRouterActor_AddRoutee(t *testing.T) {
	router := NewRouterActor(&RoundRobinRoutingLogic{}, nil)
	router.AddRoutee(&routeeMock{path: "x"})
	if len(router.Routees) != 1 {
		t.Errorf("routees len = %d, want 1", len(router.Routees))
	}
}

func TestRouterActor_RemoveRoutee(t *testing.T) {
	r := &routeeMock{path: "remove-me"}
	router := NewRouterActor(&RoundRobinRoutingLogic{}, []Ref{r, &routeeMock{path: "keep"}})
	router.RemoveRoutee(r)

	routees := router.Routees
	if len(routees) != 1 || routees[0].Path() != "keep" {
		t.Errorf("after RemoveRoutee: %v", routees)
	}
}

func TestRouterActor_RemoveRoutee_NotPresent_IsNoop(t *testing.T) {
	router := NewRouterActor(&RoundRobinRoutingLogic{}, []Ref{&routeeMock{path: "a"}})
	router.RemoveRoutee(&routeeMock{path: "not-there"})
	if len(router.Routees) != 1 {
		t.Error("RemoveRoutee on absent ref changed routees list")
	}
}

// ── RouterActor.Receive — forwarding ─────────────────────────────────────────

func TestRouterActor_Receive_ForwardsToSelectedRoutee(t *testing.T) {
	r0 := &routeeMock{path: "r0"}
	r1 := &routeeMock{path: "r1"}
	router := NewRouterActor(&RoundRobinRoutingLogic{}, []Ref{r0, r1})
	Start(router)

	router.Mailbox() <- "msg-a" // → r0
	router.Mailbox() <- "msg-b" // → r1

	// Give the goroutine time to process.
	time.Sleep(20 * time.Millisecond)

	if r0.count() != 1 {
		t.Errorf("r0 received %d messages, want 1", r0.count())
	}
	if r1.count() != 1 {
		t.Errorf("r1 received %d messages, want 1", r1.count())
	}
}

func TestRouterActor_Receive_EmptyRoutees_DoesNotPanic(t *testing.T) {
	router := NewRouterActor(&RoundRobinRoutingLogic{}, nil)
	Start(router)

	// Should not panic.
	router.Mailbox() <- "msg"
	time.Sleep(10 * time.Millisecond)
}

func TestRouterActor_Receive_PreservesSender(t *testing.T) {
	target := &routeeMock{path: "target"}
	sender := &routeeMock{path: "the-sender"}
	router := NewRouterActor(&RoundRobinRoutingLogic{}, []Ref{target})
	Start(router)

	// Deliver via Envelope so the sender is visible inside Receive.
	router.Mailbox() <- Envelope{Payload: "hello", Sender: sender}
	time.Sleep(20 * time.Millisecond)

	target.mu.Lock()
	defer target.mu.Unlock()
	if len(target.senders) == 0 {
		t.Fatal("target received no messages")
	}
	if target.senders[0] == nil || target.senders[0].Path() != "the-sender" {
		t.Errorf("sender = %v, want the-sender", target.senders[0])
	}
}

// ── RouterActor.Receive — Broadcast ──────────────────────────────────────────

func TestRouterActor_Receive_Broadcast_SendsToAll(t *testing.T) {
	routees := make([]*collectActor, 3)
	refs := make([]Ref, 3)
	for i := range routees {
		routees[i] = newCollectActor()
		Start(routees[i])
		refs[i] = routees[i].Self() // Self is nil before SpawnActor — use a mock below
	}

	// Since Self() is nil without a node, use routeeMocks as the routees.
	mr0 := &routeeMock{path: "b0"}
	mr1 := &routeeMock{path: "b1"}
	mr2 := &routeeMock{path: "b2"}
	router := NewRouterActor(&RoundRobinRoutingLogic{}, []Ref{mr0, mr1, mr2})
	Start(router)

	router.Mailbox() <- Broadcast{Message: "broadcast!"}
	time.Sleep(20 * time.Millisecond)

	for i, mr := range []*routeeMock{mr0, mr1, mr2} {
		if mr.count() != 1 {
			t.Errorf("routee[%d] received %d messages, want 1", i, mr.count())
		}
	}
}

func TestRouterActor_Receive_Broadcast_DoesNotUseLogic(t *testing.T) {
	// A logic that panics ensures Select is never called for Broadcast.
	panicLogic := &panicOnSelectLogic{}
	mr0 := &routeeMock{path: "x"}
	mr1 := &routeeMock{path: "y"}
	router := NewRouterActor(panicLogic, []Ref{mr0, mr1})
	Start(router)

	// Should not panic.
	router.Mailbox() <- Broadcast{Message: "all"}
	time.Sleep(20 * time.Millisecond)

	if mr0.count() != 1 || mr1.count() != 1 {
		t.Error("expected both routees to receive the broadcast")
	}
}

// ── ConsistentHashRoutingLogic ──────────────────────────────────────────────

type hashMsg struct {
	key string
}

func (m hashMsg) ConsistentHashKey() any { return m.key }

func TestConsistentHash_Selection(t *testing.T) {
	logic := &ConsistentHashRoutingLogic{VirtualNodesFactor: 10}
	r0 := &routeeMock{path: "r0"}
	r1 := &routeeMock{path: "r1"}
	r2 := &routeeMock{path: "r2"}
	routees := []Ref{r0, r1, r2}

	// 1. Same key must go to same routee
	keyA := "user-1"
	target1 := logic.Select(hashMsg{key: keyA}, routees)
	target2 := logic.Select(hashMsg{key: keyA}, routees)
	if target1.Path() != target2.Path() {
		t.Errorf("keyA: got %q then %q, want same", target1.Path(), target2.Path())
	}

	// 2. Different keys should (likely) go to different routees
	// We'll test a few to increase chance of hitting different buckets
	targets := make(map[string]struct{})
	for i := 0; i < 20; i++ {
		msg := hashMsg{key: fmt.Sprintf("user-%d", i)}
		targets[logic.Select(msg, routees).Path()] = struct{}{}
	}
	if len(targets) <= 1 {
		t.Errorf("expected messages to be distributed across routees, got only %v", targets)
	}

	// 3. Messages without ConsistentHashable should still work
	target3 := logic.Select("some-string", routees)
	target4 := logic.Select("some-string", routees)
	if target3.Path() != target4.Path() {
		t.Errorf("raw string key: got %q then %q, want same", target3.Path(), target4.Path())
	}
}

func TestConsistentHash_RebuildRing(t *testing.T) {
	logic := &ConsistentHashRoutingLogic{VirtualNodesFactor: 10}
	r0 := &routeeMock{path: "r0"}
	routees := []Ref{r0}

	logic.Select("msg", routees)
	initialRingSize := len(logic.ring)
	if initialRingSize != 10 {
		t.Errorf("expected 10 virtual nodes, got %d", initialRingSize)
	}

	// Add a routee
	r1 := &routeeMock{path: "r1"}
	routees = append(routees, r1)
	logic.Select("msg", routees)
	if len(logic.ring) != 20 {
		t.Errorf("expected 20 virtual nodes after adding routee, got %d", len(logic.ring))
	}
}

type panicOnSelectLogic struct{}

func (p *panicOnSelectLogic) Select(_ any, _ []Ref) Ref {
	panic("Select must not be called for Broadcast messages")
}

// ── RoundRobin distribution evenness ─────────────────────────────────────────

func TestRoundRobin_EvenDistribution(t *testing.T) {
	const n = 3
	const msgs = 300

	logic := &RoundRobinRoutingLogic{}
	counts := make([]atomic.Int64, n)
	routees := make([]Ref, n)
	for i := range routees {
		idx := i
		routees[i] = &funcRef{path: func() string { return "" }, tell: func(msg any, _ ...Ref) {
			counts[idx].Add(1)
		}}
	}

	for i := 0; i < msgs; i++ {
		target := logic.Select(i, routees)
		target.Tell(i)
	}

	for i := range counts {
		if got := counts[i].Load(); got != msgs/n {
			t.Errorf("routee[%d] got %d messages, want %d", i, got, msgs/n)
		}
	}
}

// funcRef is a Ref backed by function values — useful for unit-testing logic.
type funcRef struct {
	path func() string
	tell func(msg any, sender ...Ref)
}

func (f *funcRef) Path() string           { return f.path() }
func (f *funcRef) Tell(msg any, s ...Ref) { f.tell(msg, s...) }
