/*
 * transform_messages.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

// TransformMessages creates a [Behavior] that accepts Outer messages, applies
// transform to convert them to Inner messages, and delegates to the inner
// behavior.  When transform returns (_, false), the message is treated as
// unhandled and the actor continues with its current behavior.
//
// Because Go generics do not allow a Behavior[Outer] to hold a
// TypedContext[Inner], TransformMessages operates on Behavior[any] for the
// outer type, accepting any message and applying the transform function:
//
//	inner := func(ctx TypedContext[any], msg any) Behavior[any] {
//	    n := msg.(int)
//	    // handle int
//	    return Same[any]()
//	}
//	outer := TransformMessages(inner, func(msg any) (any, bool) {
//	    s, ok := msg.(string)
//	    if !ok { return nil, false }
//	    n, err := strconv.Atoi(s)
//	    return n, err == nil
//	})
func TransformMessages(behavior Behavior[any], transform func(any) (any, bool)) Behavior[any] {
	return transformBehavior(behavior, transform)
}

func transformBehavior(inner Behavior[any], transform func(any) (any, bool)) Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		innerMsg, ok := transform(msg)
		if !ok {
			return nil // Unhandled — keep current behavior
		}

		next := inner(ctx, innerMsg)
		if next == nil {
			return nil // Same
		}
		if isStopped(next) {
			return Stopped[any]()
		}
		return transformBehavior(next, transform)
	}
}
