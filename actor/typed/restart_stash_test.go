/*
 * restart_stash_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"testing"

	"github.com/sopranoworks/gekka/actor"
)

// TestRestartStashCapacity_DefaultsFallback ensures that when no override is
// installed the typed-actor restart stash matches actor.DefaultStashCapacity.
func TestRestartStashCapacity_DefaultsFallback(t *testing.T) {
	defer SetDefaultRestartStashCapacity(0)

	// Sanity: a non-positive value resets to the actor.DefaultStashCapacity.
	SetDefaultRestartStashCapacity(0)
	if got, want := GetDefaultRestartStashCapacity(), actor.DefaultStashCapacity; got != want {
		t.Errorf("default GetDefaultRestartStashCapacity = %d, want %d", got, want)
	}
	SetDefaultRestartStashCapacity(-5)
	if got, want := GetDefaultRestartStashCapacity(), actor.DefaultStashCapacity; got != want {
		t.Errorf("negative override = %d, want %d", got, want)
	}
}

// TestRestartStashCapacity_AppliedOnPreStart proves the runtime effect of the
// HOCON-fed override: PreStart sizes the per-actor StashBuffer such that
// exactly the configured capacity of messages can be stashed before
// IsFull/overflow rejection kicks in.
func TestRestartStashCapacity_AppliedOnPreStart(t *testing.T) {
	defer SetDefaultRestartStashCapacity(0)

	const capacity = 4
	SetDefaultRestartStashCapacity(capacity)

	behavior := func(ctx TypedContext[string], msg string) Behavior[string] { return Same[string]() }
	a := NewTypedActorInternal(behavior)
	a.SetSystem(&typedMockContext{})
	a.SetSelf(&typedMockRef{path: "/user/restart-stash"})
	a.PreStart()

	if a.stash == nil {
		t.Fatal("PreStart did not initialise the stash buffer")
	}

	for i := 0; i < capacity; i++ {
		if err := a.stash.Stash("msg"); err != nil {
			t.Fatalf("stash %d returned err = %v, want nil", i, err)
		}
	}

	if !a.stash.IsFull() {
		t.Fatalf("expected stash to be full after %d messages, size = %d", capacity, a.stash.Size())
	}

	if err := a.stash.Stash("overflow"); err == nil {
		t.Errorf("stash beyond capacity must error; got nil")
	}
}

// TestRestartStashCapacity_AppliedOnPreStartGeneric mirrors the typed-T case
// for genericTypedActor (used by NewTypedActorGeneric / reflective spawn).
func TestRestartStashCapacity_AppliedOnPreStartGeneric(t *testing.T) {
	defer SetDefaultRestartStashCapacity(0)

	const capacity = 3
	SetDefaultRestartStashCapacity(capacity)

	behavior := func(ctx TypedContext[string], msg string) Behavior[string] { return Same[string]() }
	a := NewTypedActorGeneric(behavior).(*genericTypedActor)
	a.SetSelf(&typedMockRef{path: "/user/restart-stash-generic"})
	a.PreStart()

	if a.stash == nil {
		t.Fatal("PreStart did not initialise the generic stash buffer")
	}

	for i := 0; i < capacity; i++ {
		if err := a.stash.Stash("msg"); err != nil {
			t.Fatalf("stash %d returned err = %v, want nil", i, err)
		}
	}

	if !a.stash.IsFull() {
		t.Fatalf("expected stash to be full after %d messages, size = %d", capacity, a.stash.Size())
	}

	if err := a.stash.Stash("overflow"); err == nil {
		t.Errorf("stash beyond capacity must error; got nil")
	}
}
