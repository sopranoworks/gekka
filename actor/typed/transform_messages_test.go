/*
 * transform_messages_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"strconv"
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

func TestTransformMessages_StringToInt(t *testing.T) {
	var received []int

	inner := func(ctx TypedContext[any], msg any) Behavior[any] {
		n := msg.(int)
		received = append(received, n)
		return Same[any]()
	}

	outer := TransformMessages(inner, func(msg any) (any, bool) {
		s, ok := msg.(string)
		if !ok {
			return nil, false
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, false
		}
		return n, true
	})

	ctx := &typedContext[any]{actor: &TypedActor[any]{
		BaseActor: actor.NewBaseActor(),
	}}

	// Valid string → int conversion
	next := outer(ctx, "42")
	if isStopped(next) {
		t.Fatal("should not be stopped")
	}

	// Use the returned behavior (or the original if nil/Same)
	b := outer
	if next != nil {
		b = next
	}
	b(ctx, "7")

	if len(received) != 2 || received[0] != 42 || received[1] != 7 {
		t.Errorf("got %v, want [42, 7]", received)
	}
}

func TestTransformMessages_Unhandled(t *testing.T) {
	inner := func(ctx TypedContext[any], msg any) Behavior[any] {
		t.Error("inner should not be called for non-numeric")
		return Same[any]()
	}

	outer := TransformMessages(inner, func(msg any) (any, bool) {
		s, ok := msg.(string)
		if !ok {
			return nil, false
		}
		_, err := strconv.Atoi(s)
		return nil, err == nil
	})

	ctx := &typedContext[any]{actor: &TypedActor[any]{
		BaseActor: actor.NewBaseActor(),
	}}

	// Non-numeric string should be unhandled
	next := outer(ctx, "not-a-number")
	if next != nil {
		t.Error("expected nil (unhandled)")
	}
}
