/*
 * actor_system.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"

	"gekka/gekka/actor"
)

// Props is a factory specification for creating an actor.
//
// The New field is a constructor function that returns a fresh Actor instance.
// Dependencies (node references, configuration, etc.) should be captured by the
// closure rather than embedded in Props, keeping Props itself data-free:
//
//	ref, err := node.System.ActorOf(gekka.Props{
//	    New: func() actor.Actor {
//	        return &MyActor{
//	            BaseActor: actor.NewBaseActor(),
//	            node:      node,
//	            config:    cfg,
//	        }
//	    },
//	}, "my-actor")
type Props struct {
	// New is called once to create the actor instance. It must not be nil.
	// The returned actor must embed actor.BaseActor (or otherwise implement
	// the actor.Actor interface including a valid Mailbox channel).
	New func() actor.Actor
}

// ActorSystem manages the lifecycle of actors on this node: creation,
// goroutine start, path registration, and ActorRef construction.
//
// Obtain the ActorSystem via node.System:
//
//	ref, err := node.System.ActorOf(props, "worker")
//	ref.Tell([]byte("start"))
type ActorSystem interface {
	// ActorOf creates a new actor using props, registers it at /user/<name>,
	// starts its receive goroutine, and returns a location-transparent ActorRef.
	//
	// name should be a simple identifier without slashes, e.g. "echo" or
	// "worker-1".  The actor's full path becomes /user/<name>.
	//
	// An error is returned if Props.New is nil.
	ActorOf(props Props, name string) (ActorRef, error)
}

// nodeActorSystem implements ActorSystem on top of GekkaNode.
type nodeActorSystem struct {
	node *GekkaNode
}

// ActorOf implements ActorSystem.
func (s *nodeActorSystem) ActorOf(props Props, name string) (ActorRef, error) {
	if props.New == nil {
		return ActorRef{}, fmt.Errorf("actorOf %q: Props.New must not be nil", name)
	}
	a := props.New()
	path := "/user/" + name
	return s.node.SpawnActor(path, a), nil
}
