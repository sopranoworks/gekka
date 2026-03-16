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
	"github.com/sopranoworks/gekka/internal/core"
)

var askId uint64

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
type TypedActorRef[T any] = actor.TypedActorRef[T]

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] = actor.EventSourcedBehavior[Command, Event, State]

// Spawn creates a new typed actor as a top-level actor in the system.
// It is a type-safe wrapper around ActorSystem.ActorOf.
func Spawn[T any](sys ActorSystem, behavior actor.Behavior[T], name string, props ...actor.Props) (TypedActorRef[T], error) {
	return actor.Spawn(asActorContext(sys, ""), behavior, name, props...)
}

// SpawnPersistent creates a new persistent actor.
func SpawnPersistent[Command any, Event any, State any](sys ActorSystem, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	return actor.SpawnPersistent(asActorContext(sys, ""), behavior, name, props...)
}

// Ask sends a message to a typed actor and waits for a reply.
// It follows the Akka Typed 'Ask' pattern where a message factory is provided
// that takes a 'replyTo' reference and returns the message to be sent.
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
		return actor.Ask(ctx, target, timeout, msgFactory)
	}

	cluster, isCluster := sys.(*Cluster)
	if !isCluster {
		return actor.Ask(ctx, target, timeout, msgFactory)
	}

	// Cluster implementation using NodeManager's pending reply mechanism
	tempPath := fmt.Sprintf("/temp/typed-ask/%d", atomic.AddUint64(&askId, 1))
	replyCh := make(chan *core.ArteryMetadata, 1)
	cluster.nm.RegisterPendingReply(tempPath, replyCh)
	defer cluster.nm.UnregisterPendingReply(tempPath)

	// Create a typed ref for the temporary path
	fullTempPath := cluster.selfPathURI(tempPath)
	replyTo := actor.NewTypedActorRef[R](&remoteAskRef{path: fullTempPath})

	msg := msgFactory(replyTo)
	target.Tell(msg)

	select {
	case meta := <-replyCh:
		payload := meta.DeserializedMessage
		if payload == nil {
			// Try to deserialize if possible, though it should already be done by handleUserMessage
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
