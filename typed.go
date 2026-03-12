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
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
type TypedActorRef[T any] = actor.TypedActorRef[T]

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] = actor.EventSourcedBehavior[Command, Event, State]

// SpawnTyped creates a new typed actor as a top-level actor in the system.
// It is a type-safe wrapper around ActorSystem.ActorOf.
func SpawnTyped[T any](sys ActorSystem, behavior actor.Behavior[T], name string, props ...actor.Props) (TypedActorRef[T], error) {
	return actor.SpawnTyped(asActorContext(sys, ""), behavior, name, props...)
}

// SpawnPersistent creates a new persistent actor.
func SpawnPersistent[Command any, Event any, State any](sys ActorSystem, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	return actor.SpawnPersistent(asActorContext(sys, ""), behavior, name, props...)
}


// Ask sends a message to a typed actor and waits for a reply.
// It follows the Akka Typed 'Ask' pattern where a message factory is provided
// that takes a 'replyTo' reference and returns the message to be sent.
func Ask[T any, R any](ctx context.Context, target TypedActorRef[T], timeout time.Duration, msgFactory func(replyTo TypedActorRef[R]) T) (R, error) {
	return actor.Ask(ctx, target, timeout, msgFactory)
}
