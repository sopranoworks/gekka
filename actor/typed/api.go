/*
 * typed_api.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
// It wraps an untyped Ref and provides a type-safe Tell method.
type TypedActorRef[T any] struct {
	ref actor.Ref
}

func (r TypedActorRef[T]) MarshalJSON() ([]byte, error) {
	if r.ref == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(r.ref.Path())
}

func (r *TypedActorRef[T]) UnmarshalJSON(data []byte) error {
	var path string
	if err := json.Unmarshal(data, &path); err != nil {
		return err
	}
	if path == "" {
		r.ref = nil
		return nil
	}
	// We can't easily resolve the path back to a Ref here without a system.
	// However, we can use a placeholder Ref that only has a path.
	// The sharding logic uses System().Resolve(path) anyway.
	r.ref = &pathOnlyRef{path: path}
	return nil
}

var globalMessagingProvider actor.RemoteMessagingProvider

// SetGlobalMessagingProvider sets the provider used by deserialized TypedActorRefs.
func SetGlobalMessagingProvider(p actor.RemoteMessagingProvider) {
	globalMessagingProvider = p
}

type pathOnlyRef struct {
	path string
}

func (r *pathOnlyRef) Tell(msg any, sender ...actor.Ref) {
	if globalMessagingProvider == nil {
		return
	}
	router := actor.NewRouter(globalMessagingProvider)
	var senderPath string
	if len(sender) > 0 && sender[0] != nil {
		senderPath = sender[0].Path()
	}

	// Tell is fire-and-forget; use background context for remote delivery.
	if senderPath != "" {
		_ = router.SendWithSender(context.Background(), r.path, senderPath, msg)
	} else {
		_ = router.Send(context.Background(), r.path, msg)
	}
}
func (r *pathOnlyRef) Path() string { return r.path }

// NewTypedActorRef creates a new TypedActorRef wrapping the given untyped Ref.
func NewTypedActorRef[T any](ref actor.Ref) TypedActorRef[T] {
	return TypedActorRef[T]{ref: ref}
}

// ToTyped converts an untyped Ref to a TypedActorRef[T].
func ToTyped[T any](ref actor.Ref) TypedActorRef[T] {
	return NewTypedActorRef[T](ref)
}

// ToUntyped converts a TypedActorRef[T] to an untyped Ref.
func ToUntyped[T any](ref TypedActorRef[T]) actor.Ref {
	return ref.Untyped()
}

// Tell sends a message of type T to the actor.
func (r TypedActorRef[T]) Tell(msg T) {
	if r.ref != nil {
		r.ref.Tell(msg)
	}
}

// Path returns the full actor-path URI for this reference.
func (r TypedActorRef[T]) Path() string {
	if r.ref == nil {
		return ""
	}
	return r.ref.Path()
}

// String implements fmt.Stringer.
func (r TypedActorRef[T]) String() string {
	return r.Path()
}

// Untyped returns the underlying untyped Ref.
func (r TypedActorRef[T]) Untyped() actor.Ref {
	return r.ref
}

// Spawn creates a new typed actor as a child of the given context.
func Spawn[T any](ctx actor.ActorContext, behavior Behavior[T], name string, props ...actor.Props) (TypedActorRef[T], error) {
	p := actor.Props{
		New: func() actor.Actor { return NewTypedActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := ctx.ActorOf(p, name)
	if err != nil {
		return TypedActorRef[T]{}, err
	}
	return NewTypedActorRef[T](ref), nil
}

// SpawnChild creates a new typed actor as a child of the given typed context.
func SpawnChild[T any, U any](ctx TypedContext[T], behavior Behavior[U], name string, props ...actor.Props) (TypedActorRef[U], error) {
	return Spawn(ctx.System(), behavior, name, props...)
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

	replyCh := make(chan R, 1)
	responder := &typedAskResponder[R]{replyCh: replyCh}
	replyTo := NewTypedActorRef[R](responder)

	msg := msgFactory(replyTo)
	target.Tell(msg)

	var zero R
	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type typedAskResponder[R any] struct {
	replyCh chan R
}

func (r *typedAskResponder[R]) Tell(msg any, sender ...actor.Ref) {
	if m, ok := msg.(R); ok {
		select {
		case r.replyCh <- m:
		default:
		}
	}
}

func (r *typedAskResponder[R]) Path() string {
	return "/temp/typed-ask"
}

// ── Router Factories ─────────────────────────────────────────────────────

// BroadcastGroup returns props for a GroupRouter using BroadcastRoutingLogic.
func BroadcastGroup(routees []actor.Ref) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewGroupRouter(&actor.BroadcastRoutingLogic{}, routees)
		},
	}
}

// BroadcastPool returns props for a PoolRouter using BroadcastRoutingLogic.
func BroadcastPool(nrOfInstances int, props actor.Props) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewPoolRouter(&actor.BroadcastRoutingLogic{}, nrOfInstances, props)
		},
	}
}

// ScatterGatherPool returns props for a ScatterGatherPool router.
func ScatterGatherPool(nrOfInstances int, props actor.Props, within time.Duration) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewPoolRouter(&actor.ScatterGatherRoutingLogic{Within: within}, nrOfInstances, props)
		},
	}
}

// TailChoppingGroup returns props for a GroupRouter using TailChopping logic.
func TailChoppingGroup(routees []actor.Ref, within time.Duration) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewGroupRouter(&actor.TailChoppingRoutingLogic{Within: within}, routees)
		},
	}
}

// TailChoppingPool returns props for a PoolRouter using TailChopping logic.
func TailChoppingPool(nrOfInstances int, props actor.Props, within time.Duration) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewPoolRouter(&actor.TailChoppingRoutingLogic{Within: within}, nrOfInstances, props)
		},
	}
}

// ConsistentHashingGroup returns props for a GroupRouter using ConsistentHashRoutingLogic.
func ConsistentHashingGroup(routees []actor.Ref, virtualNodes int) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewGroupRouter(&actor.ConsistentHashRoutingLogic{VirtualNodesFactor: virtualNodes}, routees)
		},
	}
}

// ConsistentHashingPool returns props for a PoolRouter using ConsistentHashRoutingLogic.
func ConsistentHashingPool(nrOfInstances int, props actor.Props, virtualNodes int) actor.Props {
	return actor.Props{
		New: func() actor.Actor {
			return actor.NewPoolRouter(&actor.ConsistentHashRoutingLogic{VirtualNodesFactor: virtualNodes}, nrOfInstances, props)
		},
	}
}

// ── Supervisor Factories ──────────────────────────────────────────────────

// WithBackoff creates a supervisor behavior that wraps an actor and manages
// its lifecycle with exponential backoff. It returns a Behavior[any] because
// it must handle both the user message type M and system signals (Terminated).
func WithBackoff[M any](options BackoffOptions, childProps actor.Props) Behavior[any] {
	return NewBackoffSupervisor[M](options, childProps)
}
