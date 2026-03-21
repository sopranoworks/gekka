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
	"reflect"
	"sync/atomic"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/stream"
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

	// Spawn creates a new typed actor with the given behavior and name.
	// behavior must be a typed.Behavior[T].
	Spawn(behavior any, name string) (ActorRef, error)

	// SpawnAnonymous creates a new typed actor with an automatically generated name.
	SpawnAnonymous(behavior any) (ActorRef, error)

	// SystemActorOf creates a new actor under the /system guardian.
	SystemActorOf(behavior any, name string) (ActorRef, error)

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

	// Terminate stops the actor system and all its actors.
	Terminate()

	// WhenTerminated returns a channel that is closed when the system has fully shut down.
	WhenTerminated() <-chan struct{}

	// Ask sends msg to the actor at dst and blocks until a reply is received
	// or ctx is cancelled.
	Ask(ctx context.Context, dst interface{}, msg any) (*IncomingMessage, error)

	// Send delivers msg to the actor at dst without waiting for a reply.
	Send(ctx context.Context, dst interface{}, msg any) error

	// RegisterType binds a manifest string to a reflect.Type for serialization.
	RegisterType(manifest string, typ reflect.Type)

	// GetTypeByManifest returns the reflect.Type registered for manifest.
	GetTypeByManifest(manifest string) (reflect.Type, bool)

	// ActorSelection returns a handle to one or more actors, local or remote,
	// identified by path.
	ActorSelection(path string) ActorSelection

	// Scheduler returns the system-level task scheduler.
	// Scheduled tasks run in their own goroutines and are automatically
	// cancelled when the ActorSystem is terminated.
	Scheduler() Scheduler

	// Materializer returns the default stream Materializer for this system.
	// Use it to run stream graphs created with the stream package:
	//
	//	graph.Run(sys.Materializer())
	Materializer() stream.Materializer

	// Receptionist returns the reference to the cluster-aware receptionist.
	Receptionist() typed.TypedActorRef[any]
}

// internalSystem is an unexported interface used by ActorRef and ActorSelection
// to perform operations on the underlying node (Cluster or localActorSystem).
type internalSystem interface {
	ActorSystem
	SendWithSender(ctx context.Context, path string, senderPath string, msg any) error
	SelfAddress() actor.Address
	SerializationRegistry() *core.SerializationRegistry
	GetLocalActor(path string) (actor.Actor, bool)
	SelfPathURI(path string) string
	LookupDeployment(path string) (core.DeploymentConfig, bool)
	SpawnActor(path string, a actor.Actor, props actor.Props) actor.Ref // Refined to actor.Ref
	SubscribeToReceptionist(keyID string, subscriber typed.TypedActorRef[any], callback func([]string))
}

func (b *actorContextBridge) SubscribeToReceptionist(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) {
	if s, ok := b.sys.(internalSystem); ok {
		s.SubscribeToReceptionist(keyID, subscriber, callback)
	}
}

// autoNameCounter is a global counter used to generate unique actor names
// when ActorOf is called with an empty name.
var autoNameCounter atomic.Uint64

// asActorContext returns a bridge that satisfies actor.ActorContext, allowing
// an ActorSystem to be injected into BaseActor without an import cycle.
// The bridge adapts the richer ActorRef return type down to actor.Ref.
func asActorContext(sys ActorSystem, parentPath string) actor.ActorContext {
	return &actorContextBridge{sys: sys, parentPath: parentPath}
}

// actorContextBridge adapts ActorSystem to the actor.ActorContext interface.
// It is injected into every BaseActor by SpawnActor so actors can call
// a.System().ActorOf(...) and a.System().Context() without importing gekka.
type actorContextBridge struct {
	sys        ActorSystem
	parentPath string // full path of the parent actor, e.g. "/user/parent"
}

func (b *actorContextBridge) ActorOf(props actor.Props, name string) (actor.Ref, error) {
	// Need a way to call ActorOfHierarchical if it's not in the interface.
	// Or we can just cast to a private interface if needed, but for now
	// we'll assume ActorOf is enough or we'll add it to the internal interface.
	type hierarchicalActorOf interface {
		ActorOfHierarchical(props Props, name string, parentPath string) (ActorRef, error)
	}
	if h, ok := b.sys.(hierarchicalActorOf); ok {
		return h.ActorOfHierarchical(props, name, b.parentPath)
	}
	return b.sys.ActorOf(props, name)
}

func (b *actorContextBridge) Spawn(behavior any, name string) (actor.Ref, error) {
	return b.sys.Spawn(behavior, name)
}

func (b *actorContextBridge) SpawnAnonymous(behavior any) (actor.Ref, error) {
	return b.sys.SpawnAnonymous(behavior)
}

func (b *actorContextBridge) SystemActorOf(behavior any, name string) (actor.Ref, error) {
	return b.sys.SystemActorOf(behavior, name)
}

func (b *actorContextBridge) Context() context.Context {
	return b.sys.Context()
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
func (b *actorContextBridge) Stop(ref actor.Ref) {
	type stopper interface {
		Stop(target ActorRef)
	}
	if s, ok := b.sys.(stopper); ok {
		if ar, ok := ref.(ActorRef); ok {
			s.Stop(ar)
		}
	}
}

func (b *actorContextBridge) Resolve(path string) (actor.Ref, error) {
	type resolver interface {
		ActorSelection(path string) ActorSelection
	}
	if r, ok := b.sys.(resolver); ok {
		ref, err := r.ActorSelection(path).Resolve(context.Background())
		if err != nil {
			return nil, err
		}
		return ref, nil
	}
	return nil, fmt.Errorf("system does not support ActorSelection")
}
