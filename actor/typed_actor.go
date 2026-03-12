/*
 * typed_actor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"log/slog"
	"reflect"
	"sync"
)

// Behavior is the definition of how an actor reacts to a message.
// It is a function that takes a TypedContext and a message of type T,
// and returns the next Behavior for the actor.
//
// Returning Same[T]() (or nil) indicates that the actor should keep its
// current behavior for the next message.
// Returning Stopped[T]() indicates that the actor should stop.
type Behavior[T any] func(ctx TypedContext[T], msg T) Behavior[T]

// TypedContext provides access to the actor's contextual information and
// allows for interaction with the actor system in a type-safe manner.
type TypedContext[T any] interface {
	// Self returns the type-safe actor reference for this actor.
	Self() TypedActorRef[T]

	// System returns the untyped actor context, providing access to
	// system-level operations like spawning children or watching other actors.
	System() ActorContext

	// Log returns the structured logger for this actor.
	Log() *slog.Logger

	// Watch registers this actor to receive a TerminatedMessage when target stops.
	Watch(target Ref)

	// Unwatch removes a previously established watch.
	Unwatch(target Ref)

	// Stop gracefully terminates the target actor.
	Stop(target Ref)
}

// typedContext is the internal implementation of TypedContext[T].
type typedContext[T any] struct {
	actor *typedActor[T]
}

func (c *typedContext[T]) Self() TypedActorRef[T] {
	return NewTypedActorRef[T](c.actor.Self())
}

func (c *typedContext[T]) System() ActorContext {
	return c.actor.System()
}

func (c *typedContext[T]) Log() *slog.Logger {
	return c.actor.Log().logger()
}

func (c *typedContext[T]) Watch(target Ref) {
	c.actor.System().Watch(c.actor.Self(), target)
}

func (c *typedContext[T]) Unwatch(target Ref) {
	if sys, ok := c.actor.System().(interface {
		Unwatch(watcher Ref, target Ref)
	}); ok {
		sys.Unwatch(c.actor.Self(), target)
	}
}

func (c *typedContext[T]) Stop(target Ref) {
	if sys, ok := c.actor.System().(interface {
		Stop(target Ref)
	}); ok {
		sys.Stop(target)
	}
}

// typedActor is the internal bridge between the untyped actor system and typed behaviors.
type typedActor[T any] struct {
	BaseActor
	behavior Behavior[T]
	ctx      *typedContext[T]
}

// newTypedActor creates a new typedActor instance with the given initial behavior.
func newTypedActor[T any](behavior Behavior[T]) *typedActor[T] {
	a := &typedActor[T]{
		BaseActor: NewBaseActor(),
		behavior:  behavior,
	}
	a.ctx = &typedContext[T]{actor: a}
	return a
}

// NewTypedActor creates a new Actor that handles messages of type T using the given behavior.
func NewTypedActor[T any](behavior Behavior[T]) Actor {
	return newTypedActor(behavior)
}

// Receive implements the Actor interface.
func (a *typedActor[T]) Receive(msg any) {
	if m, ok := msg.(T); ok {
		next := a.behavior(a.ctx, m)
		if next != nil {
			if isStopped(next) {
				a.ctx.Stop(a.Self())
			} else {
				a.behavior = next
			}
		}
	} else if _, ok := msg.(TerminatedMessage); ok {
		// Lifecycle signals are handled here.
		// In this phase, we don't pass them to the behavior yet as Behavior[T]
		// only accepts T.
	}
}

// isStopped returns true if the behavior is the Stopped sentinel.
func isStopped[T any](b Behavior[T]) bool {
	if b == nil {
		return false
	}
	// In Go, generic function pointers are not stable.
	// We use reflect to compare the pointer with a stable sentinel obtained via Stopped[T]().
	return reflect.ValueOf(b).Pointer() == reflect.ValueOf(Stopped[T]()).Pointer()
}

var stoppedBehaviors sync.Map

// Stopped returns a sentinel behavior indicating that the actor should stop.
func Stopped[T any]() Behavior[T] {
	var zero T
	typeName := reflect.TypeOf(zero).String()
	if b, ok := stoppedBehaviors.Load(typeName); ok {
		return b.(Behavior[T])
	}
	b := Behavior[T](stopped[T])
	stoppedBehaviors.Store(typeName, b)
	return b
}

func stopped[T any](ctx TypedContext[T], msg T) Behavior[T] {
	return Stopped[T]()
}

// Same returns a sentinel behavior indicating that the actor should keep its current behavior.
// In this implementation, Same is represented by a nil Behavior.
func Same[T any]() Behavior[T] {
	return nil
}

// Setup is a behavior decorator that allows for initialization of the actor.
// The factory function is called once when the actor starts, before it
// processes its first message.
func Setup[T any](factory func(TypedContext[T]) Behavior[T]) Behavior[T] {
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		// In a function-based Behavior, Setup is evaluated on the first message.
		// The factory produces the initial behavior, which then processes the message.
		behavior := factory(ctx)
		if behavior == nil {
			return nil // Same
		}
		return behavior(ctx, msg)
	}
}
