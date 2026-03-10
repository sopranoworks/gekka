/*
 * actor_system.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"gekka/gekka/actor"
)

// Props is a factory specification for creating an actor.
// It is an alias for actor.Props so that both packages refer to the same type.
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
type Props = actor.Props

// ActorSystem manages the lifecycle of actors on this node: creation,
// goroutine start, path registration, and ActorRef construction.
//
// Obtain the ActorSystem via node.System:
//
//	ref, err := node.System.ActorOf(props, "worker")
//	ref.Tell([]byte("start"))
//
// From inside actor code use BaseActor.System() which returns the equivalent
// actor.ActorContext interface (actor.Ref instead of ActorRef, avoids import
// cycle).
type ActorSystem interface {
	// ActorOf creates a new actor using props, registers it at /user/<name>,
	// starts its receive goroutine, and returns a location-transparent ActorRef.
	//
	// name should be a simple identifier without slashes, e.g. "echo" or
	// "worker-1".  The actor's full path becomes /user/<name>.
	//
	// Returns an error when:
	//   - Props.New is nil
	//   - name contains '/'
	//   - an actor is already registered at /user/<name>
	ActorOf(props Props, name string) (ActorRef, error)

	// Context returns the root context of the node that owns this system.
	// It is cancelled when the node shuts down.
	//
	// Use it as the parent context for background goroutines so they stop
	// automatically when the node does:
	//
	//	go doWork(node.System.Context())
	Context() context.Context

	// Watch monitors the target actor. If the target actor stops (or its node
	// leaves the cluster), the watcher receives a Terminated message containing
	// the target's ActorRef.
	Watch(watcher ActorRef, target ActorRef)

	// Unwatch removes a previously established watch.
	Unwatch(watcher ActorRef, target ActorRef)

	// Stop gracefully terminates a local actor by closing its mailbox.
	// Has no effect on remote actors.
	Stop(target ActorRef)

	// RemoteActorOf returns an ActorRef for an actor at the given remote address.
	// No validation is performed: the reference is created immediately and
	// messages are delivered lazily via Artery.
	RemoteActorOf(address actor.Address, path string) ActorRef
}

// autoNameCounter is a global counter used to generate unique actor names
// when ActorOf is called with an empty name.
var autoNameCounter atomic.Uint64

// nodeActorSystem implements ActorSystem on top of GekkaNode.
type nodeActorSystem struct {
	node *GekkaNode
}

// ActorOf implements ActorSystem.
//
// Name validation and path rules:
//   - name must not contain '/' (use a simple identifier such as "echo" or "worker-1")
//   - if name is empty a unique name of the form "$<n>" is generated automatically
//   - the actor is registered at /user/<name>; if an actor is already registered
//     there an error is returned (duplicate detection)
func (s *nodeActorSystem) ActorOf(props Props, name string) (ActorRef, error) {
	return s.ActorOfHierarchical(props, name, "/user")
}

// ActorOfHierarchical creates a new actor as a child of parentPath.
func (s *nodeActorSystem) ActorOfHierarchical(props Props, name string, parentPath string) (ActorRef, error) {
	// Generate a unique name when none is supplied.
	if name == "" {
		n := autoNameCounter.Add(1)
		name = fmt.Sprintf("$%d", n)
	}

	// Reject names that contain '/' — they would silently create nested paths.
	if strings.ContainsRune(name, '/') {
		return ActorRef{}, fmt.Errorf("actorOf: name %q must not contain '/'", name)
	}

	if parentPath == "" {
		parentPath = "/user"
	}
	path := parentPath + "/" + name

	// Check for duplicates before constructing the actor.
	s.node.actorsMu.RLock()
	_, exists := s.node.actors[path]
	s.node.actorsMu.RUnlock()
	if exists {
		return ActorRef{}, fmt.Errorf("actorOf: actor already registered at %q", path)
	}

	// Deployment interception: auto-provision a router when the path has a
	// matching deployment entry. GroupRouters do not need props.New (they route
	// to pre-existing actors); PoolRouters do need it (to create workers).
	if d, ok := s.node.lookupDeployment(path); ok && d.Router != "" {
		if isGroupRouter(d.Router) {
			group, err := DeploymentToGroupRouter(s.node.cm, d)
			if err != nil {
				return ActorRef{}, fmt.Errorf("actorOf: deployment config for %q: %w", path, err)
			}
			return s.node.SpawnActor(path, group, Props{}), nil
		}
		// Pool router — worker factory is required.
		if props.New == nil {
			return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil for pool router deployment at %q", path)
		}
		pool, err := DeploymentToPoolRouter(s.node.cm, d, props)
		if err != nil {
			return ActorRef{}, fmt.Errorf("actorOf: deployment config for %q: %w", path, err)
		}
		return s.node.SpawnActor(path, pool, Props{}), nil
	}

	// Plain actor — Props.New is required.
	if props.New == nil {
		return ActorRef{}, fmt.Errorf("actorOf: Props.New must not be nil")
	}
	a := props.New()
	return s.node.SpawnActor(path, a, props), nil
}

// Context implements ActorSystem.
func (s *nodeActorSystem) Context() context.Context {
	return s.node.ctx
}

// asActorContext returns a bridge that satisfies actor.ActorContext, allowing
// this ActorSystem to be injected into BaseActor without an import cycle.
// The bridge adapts the richer ActorRef return type down to actor.Ref.
func (s *nodeActorSystem) asActorContext(parentPath string) actor.ActorContext {
	return &actorContextBridge{sys: s, parentPath: parentPath}
}

// actorContextBridge adapts nodeActorSystem to the actor.ActorContext interface.
// It is injected into every BaseActor by SpawnActor so actors can call
// a.System().ActorOf(...) and a.System().Context() without importing gekka.
type actorContextBridge struct {
	sys        *nodeActorSystem
	parentPath string // full path of the parent actor, e.g. "/user/parent"
}

func (b *actorContextBridge) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	return b.sys.ActorOfHierarchical(props, name, b.parentPath)
}

func (b *actorContextBridge) Context() context.Context {
	return b.sys.node.ctx
}

// Watch implements actor.ActorContext. It registers watcher to receive a
// Terminated message when target stops. Both arguments must be ActorRef values
// (which they always are when actors are spawned via SpawnActor or ActorOf).
func (b *actorContextBridge) Watch(watcher actor.Ref, target actor.Ref) {
	w, wOK := watcher.(ActorRef)
	tgt, tOK := target.(ActorRef)
	if wOK && tOK {
		// Both sides are full ActorRefs — delegate to the rich ActorSystem.Watch.
		b.sys.Watch(w, tgt)
		return
	}
	// Fallback: register the watcher directly on the local target actor.
	if tOK && tgt.local != nil {
		tgt.local.AddWatcher(watcher)
	}
}

// Resolve implements actor.ActorContext. It looks up the actor at path and
// returns its Ref, allowing GroupRouter.PreStart to resolve routee paths
// without importing the gekka package.
func (b *actorContextBridge) Resolve(path string) (actor.Ref, error) {
	ref, err := b.sys.node.ActorSelection(path).Resolve(context.Background())
	if err != nil {
		return nil, err
	}
	return ref, nil
}

// Watch implements ActorSystem.
func (s *nodeActorSystem) Watch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.AddWatcher(watcher)
	} else {
		// Target is remote
		s.node.watchRemote(watcher, target)
	}
}

// Unwatch implements ActorSystem.
func (s *nodeActorSystem) Unwatch(watcher ActorRef, target ActorRef) {
	if target.local != nil {
		target.local.RemoveWatcher(watcher)
	} else {
		// Target is remote
		s.node.unwatchRemote(watcher, target)
	}
}

// Stop implements ActorSystem.
func (s *nodeActorSystem) Stop(target ActorRef) {
	if target.local != nil {
		close(target.local.Mailbox())
	}
}

// RemoteActorOf implements ActorSystem.
func (s *nodeActorSystem) RemoteActorOf(address actor.Address, path string) ActorRef {
	fullPath := address.String()
	if !strings.HasPrefix(path, "/") {
		fullPath += "/"
	}
	fullPath += path
	return ActorRef{fullPath: fullPath, node: s.node}
}
