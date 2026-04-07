/*
 * multi_node_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Test actor ──

type echoActor struct {
	actor.BaseActor
	received []any
}

func (a *echoActor) Receive(msg any) {
	a.received = append(a.received, msg)
	if sender := a.Sender(); sender != nil {
		sender.Tell(msg, a.Self())
	}
}

func TestMultiNodeTestKit_CreateNodes(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 3)
	assert.Equal(t, 3, mk.NodeCount())

	for i := 0; i < 3; i++ {
		sys := mk.System(i)
		assert.NotNil(t, sys)
		assert.NotNil(t, sys.Context())
	}
}

func TestMultiNodeTestKit_SpawnAndMessage(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 2)

	// Spawn an actor on node 0
	mk.RunOn(0, func(sys actor.ActorContext) {
		ref, err := sys.ActorOf(actor.Props{
			New: func() actor.Actor {
				return &echoActor{BaseActor: actor.NewBaseActor()}
			},
		}, "echo")
		require.NoError(t, err)
		assert.Equal(t, "/user/echo", ref.Path())
	})

	// Resolve actor on node 0
	mk.RunOn(0, func(sys actor.ActorContext) {
		ref, err := sys.Resolve("/user/echo")
		require.NoError(t, err)
		assert.Equal(t, "/user/echo", ref.Path())
	})
}

func TestMultiNodeTestKit_RunOnAllNodes(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 3)
	var count atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mk.RunOn(n, func(sys actor.ActorContext) {
				count.Add(1)
			})
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(3), count.Load())
}

func TestMultiNodeTestKit_Barrier(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 3)

	var order []int
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// All nodes reach the barrier before proceeding.
			mk.Barrier("phase-1")
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	// All 3 nodes should have passed the barrier.
	assert.Len(t, order, 3)
}

func TestMultiNodeTestKit_ProbeIntegration(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 1)
	probe := mk.NewProbe(t)

	// Send a message directly to the probe (simulating an actor reply).
	probe.Tell("hello")
	probe.ExpectMsg("hello", time.Second)

	// Verify no stray messages.
	probe.ExpectNoMsg(50 * time.Millisecond)
}

func TestMultiNodeTestKit_IndependentSystems(t *testing.T) {
	mk := NewMultiNodeTestKit(t, 2)

	// Spawn same-named actor on both nodes
	mk.RunOn(0, func(sys actor.ActorContext) {
		_, err := sys.ActorOf(actor.Props{
			New: func() actor.Actor { return &echoActor{BaseActor: actor.NewBaseActor()} },
		}, "shared-name")
		require.NoError(t, err)
	})

	mk.RunOn(1, func(sys actor.ActorContext) {
		_, err := sys.ActorOf(actor.Props{
			New: func() actor.Actor { return &echoActor{BaseActor: actor.NewBaseActor()} },
		}, "shared-name")
		require.NoError(t, err)
	})

	// Both should resolve independently.
	ref0, err := mk.System(0).Resolve("/user/shared-name")
	require.NoError(t, err)
	ref1, err := mk.System(1).Resolve("/user/shared-name")
	require.NoError(t, err)

	// They are different refs (different systems).
	assert.NotEqual(t, ref0, ref1)
}
