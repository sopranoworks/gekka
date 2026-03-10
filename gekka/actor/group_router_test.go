/*
 * group_router_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ── NewGroupRouter (direct refs) ──────────────────────────────────────────────

func TestNewGroupRouter_WithRefs(t *testing.T) {
	r0 := &routeeMock{path: "g0"}
	r1 := &routeeMock{path: "g1"}

	group := NewGroupRouter(&RoundRobinRoutingLogic{}, []Ref{r0, r1})

	if len(group.Routees) != 2 {
		t.Errorf("Routees len = %d, want 2", len(group.Routees))
	}
	if group.Mailbox() == nil {
		t.Error("mailbox is nil")
	}
	// Mutating the caller's slice must not affect the router.
	Routees := []Ref{r0, r1}
	g2 := NewGroupRouter(&RoundRobinRoutingLogic{}, Routees)
	Routees[0] = &routeeMock{path: "tampered"}
	if g2.Routees[0].Path() != "g0" {
		t.Error("NewGroupRouter must copy the slice")
	}
}

func TestNewGroupRouter_EmptyRefs(t *testing.T) {
	group := NewGroupRouter(&RoundRobinRoutingLogic{}, nil)
	if len(group.Routees) != 0 {
		t.Errorf("Routees len = %d, want 0", len(group.Routees))
	}
	// Sending to an empty group must not panic.
	group.Receive("ping")
}

// ── NewGroupRouterWithPaths ───────────────────────────────────────────────────

func TestNewGroupRouterWithPaths_StoresPaths(t *testing.T) {
	paths := []string{"/user/w1", "/user/w2"}
	group := NewGroupRouterWithPaths(&RoundRobinRoutingLogic{}, paths)

	got := group.RouteePathsForTest()
	if len(got) != 2 {
		t.Errorf("paths len = %d, want 2", len(got))
	}
	// Verify paths are copied (mutation safety).
	paths[0] = "tampered"
	if group.Paths[0] != "/user/w1" {
		t.Error("NewGroupRouterWithPaths must copy the slice")
	}
}

func TestGroupRouter_PreStart_ResolvesPathsViaSystem(t *testing.T) {
	ctx := newTestActorContext()
	group := NewGroupRouterWithPaths(&RoundRobinRoutingLogic{}, []string{"/user/w1", "/user/w2"})
	InjectSystem(group, ctx)

	group.PreStart()

	// testActorContext.Resolve returns a testNodeRef for any path.
	if len(group.Routees) != 2 {
		t.Errorf("Routees after PreStart = %d, want 2", len(group.Routees))
	}
}

func TestGroupRouter_PreStart_NilSystem_IsNoop(t *testing.T) {
	group := NewGroupRouterWithPaths(&RoundRobinRoutingLogic{}, []string{"/user/w1"})
	// No system injected — must not panic.
	group.PreStart()
	if len(group.Routees) != 0 {
		t.Errorf("expected 0 Routees without system, got %d", len(group.Routees))
	}
}

func TestGroupRouter_PreStart_EmptyPaths_IsNoop(t *testing.T) {
	ctx := newTestActorContext()
	group := NewGroupRouterWithPaths(&RoundRobinRoutingLogic{}, nil)
	InjectSystem(group, ctx)

	group.PreStart()
	if len(group.Routees) != 0 {
		t.Errorf("expected 0 Routees for empty paths, got %d", len(group.Routees))
	}
}

// ── Receive: routing ─────────────────────────────────────────────────────────

func TestGroupRouter_Receive_RoundRobin(t *testing.T) {
	r0 := &routeeMock{path: "g0"}
	r1 := &routeeMock{path: "g1"}
	group := NewGroupRouter(&RoundRobinRoutingLogic{}, []Ref{r0, r1})

	group.Receive("a")
	group.Receive("b")
	group.Receive("c")
	group.Receive("d")

	if r0.count() != 2 {
		t.Errorf("r0 received %d, want 2", r0.count())
	}
	if r1.count() != 2 {
		t.Errorf("r1 received %d, want 2", r1.count())
	}
}

func TestGroupRouter_Receive_Broadcast(t *testing.T) {
	r0 := &routeeMock{path: "g0"}
	r1 := &routeeMock{path: "g1"}
	r2 := &routeeMock{path: "g2"}
	group := NewGroupRouter(&RoundRobinRoutingLogic{}, []Ref{r0, r1, r2})

	group.Receive(Broadcast{Message: "event"})

	if r0.count() != 1 || r1.count() != 1 || r2.count() != 1 {
		t.Errorf("broadcast: r0=%d r1=%d r2=%d, want all 1", r0.count(), r1.count(), r2.count())
	}
}

func TestGroupRouter_Receive_EmptyRoutees_NoOp(t *testing.T) {
	group := NewGroupRouter(&RoundRobinRoutingLogic{}, nil)
	// Must not panic.
	group.Receive("silent")
	group.Receive(Broadcast{Message: "silent"})
}

// ── Start integration ─────────────────────────────────────────────────────────

func TestGroupRouter_StartedByStart_DeliversMessages(t *testing.T) {
	const n = 3
	received := make([]chan any, n)
	routees := make([]Ref, n)
	for i := range received {
		ch := make(chan any, 8)
		received[i] = ch
		idx := i
		routees[idx] = &funcRef{
			path: func() string { return fmt.Sprintf("g-%d", idx) },
			tell: func(msg any, _ ...Ref) { ch <- msg },
		}
	}

	group := NewGroupRouter(&RoundRobinRoutingLogic{}, routees)
	Start(group)

	for i := 0; i < n*2; i++ {
		group.Mailbox() <- fmt.Sprintf("m%d", i)
	}

	counts := make([]int, n)
	deadline := time.After(400 * time.Millisecond)
outer:
	for total := 0; total < n*2; {
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

// ── RandomRoutingLogic ────────────────────────────────────────────────────────

func TestRandomRoutingLogic_EmptyRoutees_ReturnsNil(t *testing.T) {
	l := &RandomRoutingLogic{}
	if got := l.Select("msg", nil); got != nil {
		t.Errorf("Select on empty = %v, want nil", got)
	}
}

func TestRandomRoutingLogic_SingleRoutee(t *testing.T) {
	r := &routeeMock{path: "only"}
	l := &RandomRoutingLogic{}
	for i := 0; i < 10; i++ {
		if got := l.Select(i, []Ref{r}); got != r {
			t.Fatalf("Select returned wrong ref")
		}
	}
}

func TestRandomRoutingLogic_ConcurrentSelect_NoPanic(t *testing.T) {
	routees := []Ref{
		&routeeMock{path: "a"},
		&routeeMock{path: "b"},
		&routeeMock{path: "c"},
	}
	l := &RandomRoutingLogic{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Select(i, routees)
		}(i)
	}
	wg.Wait()
}

func TestRandomRoutingLogic_AllRouteesReachable(t *testing.T) {
	// With enough iterations, every routee should be selected at least once.
	routees := []Ref{
		&routeeMock{path: "x"},
		&routeeMock{path: "y"},
		&routeeMock{path: "z"},
	}
	l := &RandomRoutingLogic{}
	seen := make(map[string]bool)
	for i := 0; i < 300; i++ {
		r := l.Select(i, routees)
		seen[r.Path()] = true
	}
	for _, rt := range routees {
		if !seen[rt.Path()] {
			t.Errorf("routee %q was never selected in 300 iterations", rt.Path())
		}
	}
}

// ── helpers (reused from router_test.go via same package) ────────────────────

// funcRef is also declared in pool_router_test.go in package actor.
// We use the routeeMock from router_test.go here directly.
