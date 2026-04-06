/*
 * monitor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"github.com/sopranoworks/gekka/actor"
)

// Monitor wraps an inner Behavior[T] so that it watches the given typed actor
// reference for termination. When the watched actor stops, the onTerminated
// function is called to produce a message of type T which is then delivered to
// the inner behavior.
//
// This mirrors Pekko's Behaviors.monitor which allows a typed actor to observe
// another typed actor's lifecycle without having to handle raw system signals.
//
// Usage:
//
//	type MyMsg struct { ... }
//	type TargetMsg struct { ... }
//
//	inner := func(ctx TypedContext[MyMsg], msg MyMsg) Behavior[MyMsg] { ... }
//	monitored := Monitor[MyMsg](targetRef.Untyped(), func() MyMsg {
//	    return MyMsg{Kind: "target-stopped"}
//	}, inner)
func Monitor[T any](target actor.Ref, onTerminated func() T, inner Behavior[T]) Behavior[T] {
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		// On first message delivery, register the watch and install the
		// monitoring wrapper around the inner behavior's owning actor.
		ta := extractTypedActor[T](ctx)
		if ta != nil {
			ctx.Watch(target)
			installTerminatedHook(ta, target, onTerminated)
		}

		// Delegate the first message to inner.
		next := inner(ctx, msg)
		if next == nil {
			next = inner
		}
		if isStopped(next) {
			return Stopped[T]()
		}

		// Return a plain forwarding behavior — the terminated hook is
		// already installed on the actor and will persist for its lifetime.
		return next
	}
}

// installTerminatedHook patches the TypedActor's Receive method to intercept
// TerminatedMessage signals for the watched target and convert them into typed
// messages via onTerminated.
func installTerminatedHook[T any](ta *TypedActor[T], target actor.Ref, onTerminated func() T) {
	targetPath := target.Path()
	ta.addTerminatedHook(targetPath, func() {
		msg := onTerminated()
		ta.Self().Tell(msg)
	})
}

// extractTypedActor recovers the underlying *TypedActor[T] from a TypedContext.
func extractTypedActor[T any](ctx TypedContext[T]) *TypedActor[T] {
	if tc, ok := ctx.(*typedContext[T]); ok {
		return tc.actor
	}
	return nil
}
