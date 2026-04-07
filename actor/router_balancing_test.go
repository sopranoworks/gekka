/*
 * router_balancing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ─── SmallestMailboxRoutingLogic ──────────────────────────────────────────

type sizableRef struct {
	path string
	size int
}

func (r *sizableRef) Tell(msg any, sender ...Ref) {}
func (r *sizableRef) Path() string                { return r.path }
func (r *sizableRef) MailboxLen() int             { return r.size }

type plainRef struct {
	path string
}

func (r *plainRef) Tell(msg any, sender ...Ref) {}
func (r *plainRef) Path() string                { return r.path }

func TestSmallestMailbox_SelectsSmallest(t *testing.T) {
	logic := &SmallestMailboxRoutingLogic{}

	routees := []Ref{
		&sizableRef{path: "/a", size: 10},
		&sizableRef{path: "/b", size: 2},
		&sizableRef{path: "/c", size: 5},
	}

	got := logic.Select("msg", routees)
	assert.Equal(t, "/b", got.Path())
}

func TestSmallestMailbox_Empty(t *testing.T) {
	logic := &SmallestMailboxRoutingLogic{}
	got := logic.Select("msg", nil)
	assert.Nil(t, got)
}

func TestSmallestMailbox_FallbackWhenNotSizable(t *testing.T) {
	logic := &SmallestMailboxRoutingLogic{}
	routees := []Ref{
		&plainRef{path: "/a"},
		&plainRef{path: "/b"},
	}

	// All have size 0, so first one wins
	got := logic.Select("msg", routees)
	assert.NotNil(t, got)
}

func TestSmallestMailbox_MixedRefs(t *testing.T) {
	logic := &SmallestMailboxRoutingLogic{}
	routees := []Ref{
		&sizableRef{path: "/a", size: 5},
		&plainRef{path: "/b"},         // size = 0 (not sizable)
		&sizableRef{path: "/c", size: 3},
	}

	got := logic.Select("msg", routees)
	// /b has effective size 0 (not sizable), which is smallest
	assert.Equal(t, "/b", got.Path())
}

// ─── BalancingPoolRouter ─────────────────────────────────────────────────

type balancingTestActor struct {
	BaseActor
}

func (a *balancingTestActor) Receive(msg any) {}

func TestBalancingPool_SharedChannel(t *testing.T) {
	// Test that messages go to the shared channel
	router := NewBalancingPoolRouter(3, Props{
		New: func() Actor { return &balancingTestActor{BaseActor: NewBaseActor()} },
	})

	assert.NotNil(t, router.sharedCh)

	// Manually send a message to verify it goes to the shared channel
	router.Receive("hello")

	select {
	case msg := <-router.sharedCh:
		assert.Equal(t, "hello", msg)
	default:
		t.Fatal("expected message in shared channel")
	}
}

func TestBalancingPool_MultipleMessages(t *testing.T) {
	router := NewBalancingPoolRouter(2, Props{
		New: func() Actor { return &balancingTestActor{BaseActor: NewBaseActor()} },
	})

	for i := 0; i < 10; i++ {
		router.Receive(i)
	}

	// All 10 messages should be in the shared channel
	var count int
	for {
		select {
		case <-router.sharedCh:
			count++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, 10, count)
}

// ─── BalancingRoutingLogic (via shared channel concept) ──────────────────

func TestSmallestMailbox_ConcurrentAccess(t *testing.T) {
	logic := &SmallestMailboxRoutingLogic{}
	routees := []Ref{
		&sizableRef{path: "/a", size: 1},
		&sizableRef{path: "/b", size: 2},
		&sizableRef{path: "/c", size: 3},
	}

	var selected atomic.Int64
	done := make(chan struct{})

	for i := 0; i < 100; i++ {
		go func() {
			r := logic.Select("msg", routees)
			if r.Path() == "/a" {
				selected.Add(1)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	// All 100 should select /a (smallest mailbox)
	assert.Equal(t, int64(100), selected.Load())
}
