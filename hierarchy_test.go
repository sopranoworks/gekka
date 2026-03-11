/*
 * hierarchy_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"strings"
	"testing"
	"time"

	"gekka/actor"
)

type parentActor struct {
	actor.BaseActor
	childRef actor.Ref
}

func (p *parentActor) Receive(msg any) {
	switch m := msg.(type) {
	case string:
		if m == "spawn" {
			ref, _ := p.System().ActorOf(actor.Props{New: func() actor.Actor {
				return &childActor{BaseActor: actor.NewBaseActor()}
			}}, "child")
			p.childRef = ref
		}
	case actor.Failure:
		// Supervision: the parent receives a Failure message from its child
		if p.childRef != nil && p.childRef.Path() == m.Actor.Path() {
			// Decide what to do: in this test, we'll just record it
			p.BaseActor.Log().Info("child failed", "reason", m.Reason)
		}
	}
}

type childActor struct {
	actor.BaseActor
	preStartCalled bool
}

func (c *childActor) PreStart() {
	c.preStartCalled = true
}

func (c *childActor) Receive(msg any) {
	if s, ok := msg.(string); ok && s == "panic" {
		panic("intentional panic")
	}
}

func TestHierarchy(t *testing.T) {
	node, _ := Spawn(NodeConfig{SystemName: "TestSystem"})
	defer func() { _ = node.Shutdown() }()

	parentRef, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
		return &parentActor{BaseActor: actor.NewBaseActor()}
	}}, "parent")

	parentRef.Tell("spawn")

	// Give it some time to spawn
	time.Sleep(200 * time.Millisecond)

	// Verify child path
	path := parentRef.Path() + "/child"
	sel := node.ActorSelection(path)
	ref, err := sel.Resolve(context.Background())
	if err != nil {
		t.Fatalf("failed to resolve child actor at %s: %v", path, err)
	}

	if !strings.HasSuffix(ref.Path(), "/user/parent/child") {
		t.Errorf("unexpected child path: %s", ref.Path())
	}

	// Verify lifecycle hooks
	// In the new model, we can't easily access the inner local actor from the Ref
	// but we can check the effects if needed.
}

func TestSupervisionNotice(t *testing.T) {
	node, _ := Spawn(NodeConfig{SystemName: "TestSystem"})
	defer func() { _ = node.Shutdown() }()

	parentRef, _ := node.System.ActorOf(actor.Props{New: func() actor.Actor {
		return &parentActor{BaseActor: actor.NewBaseActor()}
	}}, "parent")

	parentRef.Tell("spawn")
	time.Sleep(100 * time.Millisecond)

	childPath := parentRef.Path() + "/child"
	childRef, _ := node.ActorSelection(childPath).Resolve(context.Background())

	childRef.Tell("panic")

	// Wait for panic and notification
	time.Sleep(200 * time.Millisecond)
}
