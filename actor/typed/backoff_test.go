/*
 * backoff_supervisor_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/stretchr/testify/assert"
)

type backoffTestSystem struct {
	actor.ActorContext
	t *testing.T
}

func (s *backoffTestSystem) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	s.t.Logf("TestSystem spawning %s", name)
	_ = props.New() // Call the factory to increment spawnCount
	return &typedMockRef{path: "/temp/child"}, nil
}

func (s *backoffTestSystem) Stop(ref actor.Ref) {}

func (s *backoffTestSystem) Watch(watcher actor.Ref, target actor.Ref) {
	s.t.Logf("TestSystem watching %s -> %s", watcher.Path(), target.Path())
}

func TestBackoffOptions_NextDelay(t *testing.T) {
	opts := BackoffOptions{
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   1 * time.Second,
		RandomFactor: 0.0, // No jitter for deterministic test
	}

	assert.Equal(t, 100*time.Millisecond, opts.NextDelay(0))
	assert.Equal(t, 200*time.Millisecond, opts.NextDelay(1))
	assert.Equal(t, 400*time.Millisecond, opts.NextDelay(2))
	assert.Equal(t, 800*time.Millisecond, opts.NextDelay(3))
	assert.Equal(t, 1000*time.Millisecond, opts.NextDelay(4)) // Capped at MaxBackoff
}

func TestBackoffOptions_Jitter(t *testing.T) {
	opts := BackoffOptions{
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   1 * time.Second,
		RandomFactor: 0.1,
	}

	for i := 0; i < 100; i++ {
		delay := opts.NextDelay(1)
		assert.True(t, delay >= 100*time.Millisecond)
		assert.True(t, delay <= 220*time.Millisecond)
	}
}

func TestBackoffSupervisor_Lifecycle(t *testing.T) {
	opts := BackoffOptions{
		MinBackoff:    50 * time.Millisecond,
		MaxBackoff:    200 * time.Millisecond,
		RandomFactor:  0.0,
		ResetInterval: 500 * time.Millisecond,
	}

	var spawnCount atomic.Int32
	childProps := actor.Props{
		New: func() actor.Actor {
			spawnCount.Add(1)
			return &untypedMockActor{BaseActor: actor.NewBaseActor()}
		},
	}

	supBehavior := NewBackoffSupervisor[string](opts, childProps)
	sup := NewTypedActorInternal(supBehavior)
	
	supRef := &actor.FunctionalMockRef{
		PathURI: "/user/sup",
		Handler: func(m any) {
			sup.Mailbox() <- m
		},
	}
	sup.SetSelf(supRef)
	
	sys := &backoffTestSystem{t: t}
	sup.SetSystem(sys)
	
	actor.Start(sup)
	supRef.Tell("trigger-setup")

	// 1. Initial spawn
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(1), spawnCount.Load())

	childRef := &typedMockRef{path: "/temp/child"}
	
	// 2. Simulate child termination (failure 1)
	supRef.Tell(mockTerminated{actor: childRef})
	
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), spawnCount.Load())
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, int32(2), spawnCount.Load())

	// 3. Simulate another termination (failure 2)
	supRef.Tell(mockTerminated{actor: childRef})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), spawnCount.Load())
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(3), spawnCount.Load())

	// 4. Test Reset
	time.Sleep(600 * time.Millisecond)
	supRef.Tell(mockTerminated{actor: childRef})
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(3), spawnCount.Load())
	time.Sleep(250 * time.Millisecond)
	assert.Equal(t, int32(4), spawnCount.Load())
}

type mockTerminated struct {
	actor actor.Ref
}

func (m mockTerminated) TerminatedActor() actor.Ref { return m.actor }
