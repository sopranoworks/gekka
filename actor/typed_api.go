/*
 * typed_api.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"time"
)

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
// It wraps an untyped Ref and provides a type-safe Tell method.
type TypedActorRef[T any] struct {
	ref Ref
}

// NewTypedActorRef creates a new TypedActorRef wrapping the given untyped Ref.
func NewTypedActorRef[T any](ref Ref) TypedActorRef[T] {
	return TypedActorRef[T]{ref: ref}
}

// ToTyped converts an untyped Ref to a TypedActorRef[T].
func ToTyped[T any](ref Ref) TypedActorRef[T] {
	return NewTypedActorRef[T](ref)
}

// ToUntyped converts a TypedActorRef[T] to an untyped Ref.
func ToUntyped[T any](ref TypedActorRef[T]) Ref {
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
func (r TypedActorRef[T]) Untyped() Ref {
	return r.ref
}

// SpawnTyped creates a new typed actor as a child of the given context.
func SpawnTyped[T any](ctx ActorContext, behavior Behavior[T], name string, props ...Props) (TypedActorRef[T], error) {
	p := Props{
		New: func() Actor { return NewTypedActor(behavior) },
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
func SpawnChild[T any, U any](ctx TypedContext[T], behavior Behavior[U], name string, props ...Props) (TypedActorRef[U], error) {
	return SpawnTyped(ctx.System(), behavior, name, props...)
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

func (r *typedAskResponder[R]) Tell(msg any, sender ...Ref) {
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
