/*
 * typed.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/internal/core"
)

var askId uint64

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
type TypedActorRef[T any] = typed.TypedActorRef[T]

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] = typed.EventSourcedBehavior[Command, Event, State]

// Spawn creates a new typed actor as a top-level actor in the system.
func Spawn[T any](sys ActorSystem, behavior typed.Behavior[T], name string, props ...actor.Props) (TypedActorRef[T], error) {
	ref, err := typed.Spawn(asActorContext(sys, ""), behavior, name, props...)
	return ref, err
}

// SpawnPersistent creates a new persistent typed actor.
func SpawnPersistent[Command any, Event any, State any](sys ActorSystem, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	p := actor.Props{
		New: func() actor.Actor { return typed.NewPersistentActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := sys.ActorOf(p, name)
	if err != nil {
		return TypedActorRef[Command]{}, err
	}
	return typed.NewTypedActorRef[Command](ref), nil
}

// Ask sends a message to a typed actor and waits for a reply.
func Ask[T any, R any](ctx context.Context, target TypedActorRef[T], timeout time.Duration, msgFactory func(replyTo TypedActorRef[R]) T) (R, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var zero R
	untyped := target.Untyped()

	type systemProvider interface {
		System() ActorSystem
	}

	var sys ActorSystem
	if sp, ok := untyped.(systemProvider); ok {
		sys = sp.System()
	}

	// If we can't find the system, fall back to the old behavior (only works locally)
	if sys == nil {
		return typed.Ask(ctx, target, timeout, msgFactory)
	}

	cluster, isCluster := sys.(*Cluster)
	if !isCluster {
		return typed.Ask(ctx, target, timeout, msgFactory)
	}

	// Cluster implementation using NodeManager's pending reply mechanism
	tempPath := fmt.Sprintf("/temp/typed-ask/%d", atomic.AddUint64(&askId, 1))
	replyCh := make(chan *core.ArteryMetadata, 1)
	cluster.nm.RegisterPendingReply(tempPath, replyCh)
	defer cluster.nm.UnregisterPendingReply(tempPath)

	// Create a typed ref for the temporary path
	fullTempPath := cluster.SelfPathURI(tempPath)
	replyTo := typed.NewTypedActorRef[R](&remoteAskRef{path: fullTempPath})

	msg := msgFactory(replyTo)
	target.Tell(msg)

	select {
	case meta := <-replyCh:
		payload := meta.DeserializedMessage
		if payload == nil {
			return zero, fmt.Errorf("sharding: received raw payload in Ask, cannot deserialize")
		}

		if result, ok := payload.(R); ok {
			return result, nil
		}
		return zero, fmt.Errorf("sharding: received unexpected reply type: %T (expected %T)", payload, zero)
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type remoteAskRef struct {
	path string
}

func (r *remoteAskRef) Tell(msg any, sender ...actor.Ref) {
	// Not used locally
}
func (r *remoteAskRef) Path() string { return r.path }
