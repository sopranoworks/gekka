/*
 * spawn_protocol.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"github.com/sopranoworks/gekka/actor"
)

// SpawnRequest is a message that asks the SpawnProtocol guardian to spawn a
// new typed actor.  The spawned actor's reference is delivered to ReplyTo.
type SpawnRequest[T any] struct {
	// Behavior is the behavior for the new actor.
	Behavior Behavior[T]
	// Name is the name for the new actor.
	Name string
	// ReplyTo receives the spawned TypedActorRef[T].
	ReplyTo TypedActorRef[SpawnResponse[T]]
}

// SpawnResponse carries the result of a [SpawnRequest].
type SpawnResponse[T any] struct {
	Ref TypedActorRef[T]
	Err error
}

// SpawnProtocolBehavior returns a [Behavior] that handles [SpawnRequest]
// messages, spawning new typed child actors on demand.
//
// This behavior accepts messages of type `any` so it can handle SpawnRequest
// for any actor type T:
//
//	guardian := typed.SpawnProtocolBehavior()
//	ref, _ := typed.Spawn(system, guardian, "guardian")
func SpawnProtocolBehavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		switch req := msg.(type) {
		case spawnRequestHandler:
			req.handle(ctx)
		default:
			ctx.Log().Warn("SpawnProtocol received unexpected message")
		}
		return Same[any]()
	}
}

// spawnRequestHandler is an internal interface to allow SpawnRequest[T] of any
// T to be handled by the untyped SpawnProtocol guardian.
type spawnRequestHandler interface {
	handle(ctx TypedContext[any])
}

// handle implements spawnRequestHandler for SpawnRequest[T].
func (sr SpawnRequest[T]) handle(ctx TypedContext[any]) {
	props := actor.Props{
		New: func() actor.Actor { return NewTypedActor(sr.Behavior) },
	}
	ref, err := ctx.System().ActorOf(props, sr.Name)
	var typedRef TypedActorRef[T]
	if err == nil {
		typedRef = NewTypedActorRef[T](ref)
	}
	sr.ReplyTo.Tell(SpawnResponse[T]{Ref: typedRef, Err: err})
}
