/*
 * router_advanced_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRouter_Broadcast(t *testing.T) {
	received := make(chan string, 10)
	
	workerBehavior := func(ctx TypedContext[string], msg string) Behavior[string] {
		received <- msg
		return Same[string]()
	}

	w1 := &typedActor[string]{BaseActor: NewBaseActor(), behavior: workerBehavior}
	w1.ctx = &typedContext[string]{actor: w1}
	w1.SetSelf(&functionalMockRef{handler: func(m any) { w1.Receive(m) }, path: "/user/w1"})
	
	w2 := &typedActor[string]{BaseActor: NewBaseActor(), behavior: workerBehavior}
	w2.ctx = &typedContext[string]{actor: w2}
	w2.SetSelf(&functionalMockRef{handler: func(m any) { w2.Receive(m) }, path: "/user/w2"})

	group := NewGroupRouter(&BroadcastRoutingLogic{}, []Ref{w1.Self(), w2.Self()})
	
	rActor := &typedActor[any]{
		BaseActor: NewBaseActor(),
		behavior:  group.Behavior(),
	}
	rActor.ctx = &typedContext[any]{actor: rActor}
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
	
	sys := &scatterGatherTestSystem{
		received: received,
		t:        t,
	}

	fastWorker := &scatterGatherTestWorker{reply: "fast-reply", delay: 0, t: t}
	slowWorker := &scatterGatherTestWorker{reply: "slow-reply", delay: 200 * time.Millisecond, t: t}

	sg := NewScatterGatherFirstCompleted([]Ref{fastWorker, slowWorker}, 500*time.Millisecond)
	
	rActor := &typedActor[any]{
		BaseActor: NewBaseActor(),
		behavior:  sg.Behavior(),
	}
	rActor.ctx = &typedContext[any]{actor: rActor}
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	InjectSystem(rActor, sys)

	sender := &functionalMockRef{
		path: "/user/sender",
		handler: func(m any) {
			t.Logf("Test sender received: %v", m)
			if s, ok := m.(string); ok {
				received <- s
			}
		},
	}
	rActor.currentSender = sender

	t.Log("Sending query to router...")
	rActor.Receive("query")

	// First completed should be fast-reply
	t.Log("Waiting for fast response...")
	select {
	case msg := <-received:
		t.Logf("Test confirmed receipt: %s", msg)
		assert.Equal(t, "fast-reply", msg)
	case <-time.After(2 * time.Second):
		t.Fatal("No response from ScatterGather")
	}

	// Ensure no second response arrives
	t.Log("Checking for unexpected second response...")
	select {
	case msg := <-received:
		t.Fatalf("Unexpected second response: %s", msg)
	case <-time.After(500 * time.Millisecond):
		t.Log("No second response, OK.")
	}
}

func TestRouter_TailChopping(t *testing.T) {
	received := make(chan string, 10)
	
	sys := &scatterGatherTestSystem{
		received: received,
		t:        t,
	}

	slowWorker := &scatterGatherTestWorker{reply: "slow-reply", delay: 200 * time.Millisecond, t: t}
	fastWorker := &scatterGatherTestWorker{reply: "fast-reply", delay: 0, t: t}

	// We order them so slow is first.
	tc := NewTailChoppingFirstCompleted([]Ref{slowWorker, fastWorker}, 100*time.Millisecond)
	
	rActor := &typedActor[any]{
		BaseActor: NewBaseActor(),
		behavior:  tc.Behavior(),
	}
	rActor.ctx = &typedContext[any]{actor: rActor}
	rActor.SetSelf(&typedMockRef{path: "/user/router"})
	InjectSystem(rActor, sys)

	sender := &functionalMockRef{
		path: "/user/sender",
		handler: func(m any) {
			t.Logf("Test sender received: %v", m)
			if s, ok := m.(string); ok {
				received <- s
			}
		},
	}
	rActor.currentSender = sender

	t.Log("Sending query to TailChopping router...")
	rActor.Receive("query")

	// Even though slowWorker was first, fastWorker (sent after 100ms) should win
	// because it has 0 delay vs slow's 200ms (total 200ms vs 100ms+0ms).
	select {
	case msg := <-received:
		t.Logf("Test confirmed receipt: %s", msg)
		assert.Equal(t, "fast-reply", msg)
	case <-time.After(2 * time.Second):
		t.Fatal("No response from TailChopping")
	}
}

type functionalMockRef struct {
	Ref
	handler func(any)
	path    string
}

func (r *functionalMockRef) Tell(msg any, sender ...Ref) { r.handler(msg) }
func (r *functionalMockRef) Path() string          { return r.path }

type scatterGatherTestSystem struct {
	ActorContext
	received chan string
	t        *testing.T
}

func (s *scatterGatherTestSystem) ActorOf(props Props, name string) (Ref, error) {
	s.t.Log("System spawning aggregator...")
	
	agg := props.New()
	
	// Use functionalMockRef for agg so it can receive replies
	ref := &functionalMockRef{
		path:    "/temp/agg",
		handler: func(m any) { 
			s.t.Logf("Aggregator received message: %T", m)
			agg.Receive(m) 
		},
	}
	
	if a, ok := agg.(interface{ SetSelf(Ref) }); ok {
		a.SetSelf(ref)
	}
	InjectSystem(agg, s)
	
	Start(agg)
	return ref, nil
}

func (s *scatterGatherTestSystem) Stop(ref Ref) {
	// No-op for mock
}

type scatterGatherTestWorker struct {
	Ref
	reply string
	delay time.Duration
	t     *testing.T
}

func (w *scatterGatherTestWorker) Tell(msg any, sender ...Ref) {
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
