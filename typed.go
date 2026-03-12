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

// SpawnTyped creates a new typed actor as a top-level actor in the system.
// It is a type-safe wrapper around ActorSystem.ActorOf.
func SpawnTyped[T any](sys ActorSystem, behavior actor.Behavior[T], name string, props ...actor.Props) (TypedActorRef[T], error) {
	p := actor.Props{
		New: func() actor.Actor { return actor.NewTypedActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := sys.ActorOf(p, name)
	if err != nil {
		return TypedActorRef[T]{}, err
	}
	return actor.NewTypedActorRef[T](ref), nil
}

// Ask sends a message to a typed actor and waits for a reply.
// It follows the Akka Typed 'Ask' pattern where a message factory is provided
// that takes a 'replyTo' reference and returns the message to be sent.
func Ask[T any, R any](ctx context.Context, target TypedActorRef[T], timeout time.Duration, msgFactory func(replyTo TypedActorRef[R]) T) (R, error) {
	return actor.Ask(ctx, target, timeout, msgFactory)
}
