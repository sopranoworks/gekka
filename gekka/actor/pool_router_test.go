/*
 * pool_router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── Minimal ActorContext for pool tests ───────────────────────────────────────

// testActorContext is a lightweight ActorContext that supports ActorOf and
// Watch without a full GekkaNode. It records Watch calls for assertions.
type testActorContext struct {
	mu      sync.Mutex
	spawned map[string]Actor // path → actor instance
	watches [][2]string      // [watcher.Path(), target.Path()]
	self    *testNodeRef     // owner node ref (the pool router itself)
}

func newTestActorContext() *testActorContext {
	return &testActorContext{spawned: make(map[string]Actor)}
}

func (c *testActorContext) ActorOf(props Props, name string) (Ref, error) {
	if props.New == nil {
		return nil, fmt.Errorf("Props.New is nil")
	}
	path := "/user/" + name
	c.mu.Lock()
	if _, exists := c.spawned[path]; exists {
		c.mu.Unlock()
		return nil, fmt.Errorf("already registered at %q", path)
	}
	a := props.New()
	c.spawned[path] = a
	c.mu.Unlock()

	ref := &testNodeRef{path: path}
	Start(a)
	return ref, nil
}

func (c *testActorContext) Context() context.Context {
	return context.Background()
}

func (c *testActorContext) Watch(watcher Ref, target Ref) {
	if watcher == nil || target == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watches = append(c.watches, [2]string{watcher.Path(), target.Path()})
}

func (c *testActorContext) Resolve(path string) (Ref, error) {
	return &testNodeRef{path: path}, nil
}

func (c *testActorContext) watchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.watches)
}

func (c *testActorContext) spawnedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spawned)
}

// testNodeRef is a lightweight Ref whose Tell records delivered messages.
type testNodeRef struct {
	path string
	mu   sync.Mutex
	msgs []any
}

func (r *testNodeRef) Path() string { return r.path }
func (r *testNodeRef) Tell(msg any, _ ...Ref) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, msg)
}
func (r *testNodeRef) msgCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

// ── testTerminated satisfies TerminatedMessage ─────────────────────────────────

type testTerminated struct{ ref Ref }

func (t testTerminated) TerminatedActor() Ref { return t.ref }

// ── TerminatedMessage interface ────────────────────────────────────────────────

func TestTerminatedMessage_Interface(t *testing.T) {
	var _ TerminatedMessage = testTerminated{}
}

// ── NewPoolRouter ─────────────────────────────────────────────────────────────

func TestNewPoolRouter_Fields(t *testing.T) {
	props := Props{New: func() Actor { return &simpleTestActor{BaseActor: NewBaseActor()} }}
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 3, props)

	if pool.nrOfInstances != 3 {
		t.Errorf("nrOfInstances = %d, want 3", pool.nrOfInstances)
	}
	if pool.props.New == nil {
		t.Error("props.New is nil")
	}
	if pool.Mailbox() == nil {
		t.Error("mailbox is nil")
	}
}

// ── PreStart populates Routees ────────────────────────────────────────────────

func TestPoolRouter_PreStart_SpawnsChildren(t *testing.T) {
	const n = 3
	ctx := newTestActorContext()

	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, n, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}

	// Inject system and self before calling PreStart (as SpawnActor would).
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}

	pool.PreStart()

	if ctx.spawnedCount() != n {
		t.Errorf("spawned %d actors, want %d", ctx.spawnedCount(), n)
	}
	if len(pool.Routees) != n {
		t.Errorf("Routees len = %d, want %d", len(pool.Routees), n)
	}
	if ctx.watchCount() != n {
		t.Errorf("Watch called %d times, want %d", ctx.watchCount(), n)
	}
}

func TestPoolRouter_PreStart_ChildNamesAreUnique(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 3, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}

	pool.PreStart()

	paths := map[string]bool{}
	for _, r := range pool.Routees {
		if paths[r.Path()] {
			t.Errorf("duplicate routee path: %s", r.Path())
		}
		paths[r.Path()] = true
	}
}

func TestPoolRouter_PreStart_ChildNamesContainPoolPrefix(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}

	pool.PreStart()

	for _, r := range pool.Routees {
		if !strings.Contains(r.Path(), "$pool-") {
			t.Errorf("routee path %q does not contain $pool-", r.Path())
		}
	}
}

func TestPoolRouter_PreStart_NilSystem_IsNoop(t *testing.T) {
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 3, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	// No System injected — PreStart should not panic.
	pool.PreStart()

	if len(pool.Routees) != 0 {
		t.Errorf("expected no Routees when System is nil, got %d", len(pool.Routees))
	}
}

func TestPoolRouter_PreStart_ZeroInstances_IsNoop(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 0, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	InjectSystem(pool, ctx)

	pool.PreStart()

	if ctx.spawnedCount() != 0 {
		t.Error("expected no children for nrOfInstances=0")
	}
}

// ── Receive: TerminatedMessage removes routee ─────────────────────────────────

func TestPoolRouter_Receive_TerminatedMessage_RemovesRoutee(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}
	pool.PreStart()

	if len(pool.Routees) != 2 {
		t.Fatalf("expected 2 Routees before termination, got %d", len(pool.Routees))
	}

	// Simulate one child stopping.
	dyingRef := pool.Routees[0]
	pool.Receive(testTerminated{ref: dyingRef})

	if len(pool.Routees) != 1 {
		t.Errorf("Routees after termination = %d, want 1", len(pool.Routees))
	}
	if pool.Routees[0].Path() == dyingRef.Path() {
		t.Error("terminated routee was not removed")
	}
}

func TestPoolRouter_Receive_TerminatedMessage_AllRemoved(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}
	pool.PreStart()

	Routees := pool.Routees
	for _, r := range Routees {
		pool.Receive(testTerminated{ref: r})
	}

	if len(pool.Routees) != 0 {
		t.Errorf("expected empty routee list, got %d", len(pool.Routees))
	}
}

// ── Receive: AdjustPoolSize ────────────────────────────────────────────────────

func TestPoolRouter_Receive_AdjustPoolSize_Grows(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	selfRef := &testNodeRef{path: "/user/pool"}
	InjectSystem(pool, ctx)
	type selfSet interface{ SetSelf(Ref) }
	if ss, ok := any(pool).(selfSet); ok {
		ss.SetSelf(selfRef)
	}
	pool.PreStart()

	pool.Receive(AdjustPoolSize{Delta: 3})

	if len(pool.Routees) != 5 {
		t.Errorf("Routees after +3 = %d, want 5", len(pool.Routees))
	}
	if pool.nrOfInstances != 5 {
		t.Errorf("nrOfInstances after +3 = %d, want 5", pool.nrOfInstances)
	}
	if ctx.spawnedCount() != 5 {
		t.Errorf("total spawned = %d, want 5", ctx.spawnedCount())
	}
}

func TestPoolRouter_Receive_AdjustPoolSize_NegativeDelta_IsNoop(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	InjectSystem(pool, ctx)
	pool.PreStart()

	pool.Receive(AdjustPoolSize{Delta: -1})

	if len(pool.Routees) != 2 {
		t.Errorf("Routees after negative delta = %d, want 2 (unchanged)", len(pool.Routees))
	}
}

func TestPoolRouter_Receive_AdjustPoolSize_ZeroDelta_IsNoop(t *testing.T) {
	ctx := newTestActorContext()
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 2, Props{New: func() Actor {
		return &simpleTestActor{BaseActor: NewBaseActor()}
	}})
	InjectSystem(pool, ctx)
	pool.PreStart()
	initial := len(pool.Routees)

	pool.Receive(AdjustPoolSize{Delta: 0})

	if len(pool.Routees) != initial {
		t.Errorf("Routees after zero delta = %d, want %d (unchanged)", len(pool.Routees), initial)
	}
}

// ── Receive: normal messages delegated to RouterActor ─────────────────────────

func TestPoolRouter_Receive_NormalMessage_RoutedViaLogic(t *testing.T) {
	r0 := &routeeMock{path: "target-0"}
	r1 := &routeeMock{path: "target-1"}
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 0, Props{})
	pool.Routees = []Ref{r0, r1}

	pool.Receive("hello")

	if r0.count() != 1 {
		t.Errorf("r0 received %d, want 1", r0.count())
	}
	pool.Receive("world")
	if r1.count() != 1 {
		t.Errorf("r1 received %d, want 1", r1.count())
	}
}

func TestPoolRouter_Receive_Broadcast_SendsToAll(t *testing.T) {
	r0 := &routeeMock{path: "bcast-0"}
	r1 := &routeeMock{path: "bcast-1"}
	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 0, Props{})
	pool.Routees = []Ref{r0, r1}

	pool.Receive(Broadcast{Message: "all"})

	if r0.count() != 1 || r1.count() != 1 {
		t.Errorf("broadcast: r0=%d r1=%d, want both 1", r0.count(), r1.count())
	}
}

// ── End-to-end: PoolRouter started by Start ───────────────────────────────────

func TestPoolRouter_StartedByStart_DeliversMessages(t *testing.T) {
	// Wire up the pool without a full GekkaNode using collectActor Routees.
	received := make([]chan any, 3)
	Routees := make([]Ref, 3)
	for i := range received {
		ch := make(chan any, 8)
		received[i] = ch
		idx := i
		Routees[idx] = &funcRef{
			path: func() string { return fmt.Sprintf("routee-%d", idx) },
			tell: func(msg any, _ ...Ref) { ch <- msg },
		}
	}

	pool := NewPoolRouter(&RoundRobinRoutingLogic{}, 0, Props{})
	pool.Routees = append(pool.Routees, Routees...)
	Start(pool)

	for i := 0; i < 6; i++ {
		pool.Mailbox() <- fmt.Sprintf("msg-%d", i)
	}

	// Collect replies with a generous timeout.
	counts := make([]int, 3)
	deadline := time.After(500 * time.Millisecond)
outer:
	for total := 0; total < 6; {
		for i, ch := range received {
			select {
			case <-ch:
				counts[i]++
				total++
			case <-deadline:
				break outer
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i, c := range counts {
		if c != 2 {
			t.Errorf("routee[%d] received %d messages, want 2", i, c)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

type simpleTestActor struct{ BaseActor }

func (a *simpleTestActor) Receive(_ any) {}
