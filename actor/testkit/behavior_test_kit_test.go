/*
 * behavior_test_kit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit

import (
	"testing"

	"github.com/sopranoworks/gekka/actor/typed"
)

func TestBehaviorTestKit_Greeter(t *testing.T) {
	var greetings []string
	greeter := func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
		greetings = append(greetings, "Hello, "+msg+"!")
		return typed.Same[string]()
	}

	btk := NewBehaviorTestKit(t, greeter)
	btk.Run("World")
	btk.Run("Go")

	if len(greetings) != 2 {
		t.Fatalf("expected 2 greetings, got %d", len(greetings))
	}
	if greetings[0] != "Hello, World!" {
		t.Errorf("got %q, want %q", greetings[0], "Hello, World!")
	}
	if greetings[1] != "Hello, Go!" {
		t.Errorf("got %q, want %q", greetings[1], "Hello, Go!")
	}
}

func TestBehaviorTestKit_BehaviorTransition(t *testing.T) {
	initial := func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
		if msg == "switch" {
			return func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
				if msg == "stop" {
					return typed.Stopped[string]()
				}
				return typed.Same[string]()
			}
		}
		return typed.Same[string]()
	}

	btk := NewBehaviorTestKit(t, initial)

	btk.Run("hello")
	if btk.IsStopped() {
		t.Error("should not be stopped")
	}

	btk.Run("switch")
	if btk.IsStopped() {
		t.Error("should not be stopped after switch")
	}

	btk.Run("stop")
	if !btk.IsStopped() {
		t.Error("should be stopped")
	}
}

func TestBehaviorTestKit_SpawnEffect(t *testing.T) {
	behavior := func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
		if msg == "spawn" {
			ctx.Spawn(nil, "child-actor")
		}
		return typed.Same[string]()
	}

	btk := NewBehaviorTestKit(t, behavior)
	btk.Run("spawn")

	effects := btk.Effects()
	if len(effects) != 1 {
		t.Fatalf("expected 1 effect, got %d", len(effects))
	}
	if effects[0].Kind != "spawn" || effects[0].Name != "child-actor" {
		t.Errorf("unexpected effect: %+v", effects[0])
	}
}

func TestBehaviorTestKit_SelfMessage(t *testing.T) {
	behavior := func(ctx typed.TypedContext[string], msg string) typed.Behavior[string] {
		if msg == "trigger" {
			ctx.Self().Tell("self-msg")
		}
		return typed.Same[string]()
	}

	btk := NewBehaviorTestKit(t, behavior)
	btk.Run("trigger")

	inbox := btk.SelfInbox()
	if len(inbox) != 1 || inbox[0] != "self-msg" {
		t.Errorf("self inbox = %v, want [self-msg]", inbox)
	}
}
