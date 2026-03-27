/*
 * router_advanced_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

func TestRouter_Broadcast(t *testing.T) {
	received := make(chan string, 10)

	workerBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		received <- msg
		return Same[string]()
	}

	w1 := NewTypedActorInternal(workerBehavior)
	w1.SetSelf(&actor.FunctionalMockRef{Handler: func(m any) { w1.Receive(m) }, PathURI: "/user/w1"})

	w2 := NewTypedActorInternal(workerBehavior)
	w2.SetSelf(&actor.FunctionalMockRef{Handler: func(m any) { w2.Receive(m) }, PathURI: "/user/w2"})

	group := actor.NewGroupRouter(&actor.BroadcastRoutingLogic{}, []actor.Ref{w1.Self(), w2.Self()})

	rActor := NewTypedActorInternal(func(ctx TypedContext[any], msg any) Behavior[any] {
		group.Receive(msg)
		return Same[any]()
	})
	rActor.SetSelf(&typedMockRef{path: "/user/router"})

	rActor.Receive("broadcast-msg")

	// Both should receive
	for i := 0; i < 2; i++ {
		select {
		case msg := <-received:
			assert.Equal(t, "broadcast-msg", msg)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Message %d not received", i+1)
		}
	}
}

func TestRouter_ScatterGather(t *testing.T) {
	received := make(chan string, 10)

	sys := &actor.ScatterGatherTestSystem{
		Received: received,
		T:        t,
	}

	fastWorker := &scatterGatherTestWorker{reply: "fast-reply", delay: 0, t: t}
	slowWorker := &scatterGatherTestWorker{reply: "slow-reply", delay: 200 * time.Millisecond, t: t}

	sg := actor.NewGroupRouter(&actor.ScatterGatherRoutingLogic{Within: 500 * time.Millisecond}, []actor.Ref{fastWorker, slowWorker})

	rActor := NewTypedActorInternal(RouterBehavior(&sg.RouterActor))
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	rActor.SetSystem(sys)

	sender := &actor.FunctionalMockRef{
		PathURI: "/user/sender",
		Handler: func(m any) {
			t.Logf("Test sender received: %v", m)
			if s, ok := m.(string); ok {
				received <- s
			}
		},
	}
	actor.InjectSender(rActor, sender)

	rActor.Receive("query")

	// First completed should be fast-reply
	select {
	case msg := <-received:
		t.Logf("Test confirmed receipt: %s", msg)
		assert.Equal(t, "fast-reply", msg)
	case <-time.After(2 * time.Second):
		t.Fatal("No response from ScatterGather")
	}
}

func TestRouter_TailChopping(t *testing.T) {
	received := make(chan string, 10)

	sys := &actor.ScatterGatherTestSystem{
		Received: received,
		T:        t,
	}

	slowWorker := &scatterGatherTestWorker{reply: "slow-reply", delay: 200 * time.Millisecond, t: t}
	fastWorker := &scatterGatherTestWorker{reply: "fast-reply", delay: 0, t: t}

	tc := actor.NewGroupRouter(&actor.TailChoppingRoutingLogic{Within: 100 * time.Millisecond}, []actor.Ref{slowWorker, fastWorker})

	rActor := NewTypedActorInternal(RouterBehavior(&tc.RouterActor))
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	rActor.SetSystem(sys)

	sender := &actor.FunctionalMockRef{
		PathURI: "/user/sender",
		Handler: func(m any) {
			t.Logf("Test sender received: %v", m)
			if s, ok := m.(string); ok {
				received <- s
			}
		},
	}
	actor.InjectSender(rActor, sender)

	rActor.Receive("query")

	// Even though slowWorker was first, fastWorker (sent after 100ms) should win
	// because it has 0 delay vs slow's 200ms (total 200ms vs 100ms+0ms).
	select {
	case msg := <-received:
		t.Logf("Received response: %s", msg)
		assert.Equal(t, "fast-reply", msg)
	case <-time.After(2 * time.Second):
		t.Fatal("No response from TailChopping")
	}
}

func TestRouter_ClassicAdaptive(t *testing.T) {
	// Mock metrics provider
	mockProvider := &mockClusterMetricsProvider{
		pressure: map[string]actor.NodePressure{
			"node1:2552": {Score: 0.9},
			"node2:2552": {Score: 0.1},
		},
	}
	actor.SetClusterMetricsProvider(mockProvider)

	w1 := &actor.FunctionalMockRef{PathURI: "pekko://Sys@node1:2552/user/w1", Handler: func(any) {}}
	w2 := &actor.FunctionalMockRef{PathURI: "pekko://Sys@node2:2552/user/w2", Handler: func(any) {}}

	// Classic PoolRouter with Adaptive logic
	pool := actor.NewPoolRouter(&actor.AdaptiveLoadBalancingRoutingLogic{}, 0, actor.Props{})
	pool.Routees = []actor.Ref{w1, w2}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		target := pool.Logic.Select("test", pool.Routees)
		counts[target.Path()]++
	}

	t.Logf("Classic Adaptive counts: w1=%d, w2=%d", counts[w1.PathURI], counts[w2.PathURI])
	assert.True(t, counts[w2.PathURI] > counts[w1.PathURI]*2)
}

type scatterGatherTestWorker struct {
	actor.Ref
	reply string
	delay time.Duration
	t     *testing.T
}

func (w *scatterGatherTestWorker) Tell(msg any, sender ...actor.Ref) {
	w.t.Logf("Worker received message, will reply with %s", w.reply)
	if len(sender) > 0 {
		s := sender[0]
		go func() {
			if w.delay > 0 {
				time.Sleep(w.delay)
			}
			w.t.Logf("Worker sending reply: %s", w.reply)
			s.Tell(w.reply)
		}()
	}
}

func (w *scatterGatherTestWorker) Path() string { return "/user/worker" }

type mockClusterMetricsProvider struct {
	pressure map[string]actor.NodePressure
}

func (m *mockClusterMetricsProvider) GetClusterPressure() map[string]actor.NodePressure {
	return m.pressure
}
