/*
 * gekka_node_watch_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"testing"
	"time"

	"gekka/gekka/actor"
)

// watcherTestingActor simply records any Terminated messages it receives.
type watcherTestingActor struct {
	actor.BaseActor
	terminated chan Terminated
}

func (w *watcherTestingActor) Receive(msg any) {
	if t, ok := msg.(Terminated); ok {
		w.terminated <- t
	}
}

func TestLocalDeathWatch(t *testing.T) {
	node := newTestNode(t, "Sys", "127.0.0.1", 0)

	// Create Target
	targetRef, err := node.System.ActorOf(Props{
		New: func() actor.Actor { return &echoActor{BaseActor: actor.NewBaseActor()} },
	}, "target")
	if err != nil {
		t.Fatalf("ActorOf target failed: %v", err)
	}

	// Create Watcher
	watcherActor := &watcherTestingActor{
		BaseActor:  actor.NewBaseActor(),
		terminated: make(chan Terminated, 5),
	}
	watcherRef, err := node.System.ActorOf(Props{
		New: func() actor.Actor { return watcherActor },
	}, "watcher")
	if err != nil {
		t.Fatalf("ActorOf watcher failed: %v", err)
	}

	// Watch the target
	node.System.Watch(watcherRef, targetRef)

	// Stop the target
	node.System.Stop(targetRef)

	// Expect Terminated message
	select {
	case term := <-watcherActor.terminated:
		if term.Actor.Path() != targetRef.Path() {
			t.Errorf("Expected Terminated path %s, got %s", targetRef.Path(), term.Actor.Path())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for Terminated message from local target")
	}
}

func TestRemoteDeathWatch(t *testing.T) {
	node1 := newTestNode(t, "ClusterSys", "127.0.0.1", 0)
	defer node1.Shutdown()

	node2 := newTestNode(t, "ClusterSys", "127.0.0.1", 0) // dynamic port
	defer node2.Shutdown()

	// Spawn target on node2
	targetRef, err := node2.System.ActorOf(Props{
		New: func() actor.Actor { return &echoActor{BaseActor: actor.NewBaseActor()} },
	}, "remoteTarget")
	if err != nil {
		t.Fatalf("ActorOf target failed: %v", err)
	}

	// Resolve targetRef from node1's perspective
	remoteSelection := node1.ActorSelection(targetRef.Path())
	resolvedTarget, err := remoteSelection.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve remote target failed: %v", err)
	}

	// Create Watcher on node1
	watcherActor := &watcherTestingActor{
		BaseActor:  actor.NewBaseActor(),
		terminated: make(chan Terminated, 5),
	}
	watcherRef, err := node1.System.ActorOf(Props{
		New: func() actor.Actor { return watcherActor },
	}, "watcher")
	if err != nil {
		t.Fatalf("ActorOf watcher failed: %v", err)
	}

	// Watch the remote target
	node1.System.Watch(watcherRef, resolvedTarget)

	// Simulate node2 failure explicitly by synthesising the cluster event logic
	// directly onto node1.
	node2Addr := node2.SelfAddress()
	node1.triggerRemoteNodeDeath(MemberAddress{
		Protocol: node2Addr.Protocol,
		System:   node2Addr.System,
		Host:     node2Addr.Host,
		Port:     uint32(node2Addr.Port),
	})

	// Expect Terminated message
	select {
	case term := <-watcherActor.terminated:
		if term.Actor.Path() != targetRef.Path() {
			t.Errorf("Expected Terminated path %s, got %s", targetRef.Path(), term.Actor.Path())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for Terminated message from remote target")
	}
}
