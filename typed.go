/*
 * typed.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/delivery"
	"github.com/sopranoworks/gekka/actor/typed/pubsub"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	styped "github.com/sopranoworks/gekka/cluster/sharding/typed"
	ctyped "github.com/sopranoworks/gekka/cluster/typed"
	"github.com/sopranoworks/gekka/internal/core"
	"github.com/sopranoworks/gekka/persistence"
	ptyped "github.com/sopranoworks/gekka/persistence/typed"
	pstate "github.com/sopranoworks/gekka/persistence/typed/state"
)

var askId uint64

// TypedActorRef is a type-safe reference to an actor that accepts messages of type T.
type TypedActorRef[T any] = typed.TypedActorRef[T]

// Behavior is a function that defines how a typed actor handles messages.
type Behavior[T any] = typed.Behavior[T]

// TypedContext provides the execution context for a typed actor.
type TypedContext[T any] = typed.TypedContext[T]

// Same returns a behavior that stays in the current state.
func Same[T any]() Behavior[T] {
	return typed.Same[T]()
}

// Stopped returns a behavior that stops the actor.
func Stopped[T any]() Behavior[T] {
	return typed.Stopped[T]()
}

// Setup creates a behavior that runs a factory function before the first message.
func Setup[T any](factory func(TypedContext[T]) Behavior[T]) Behavior[T] {
	return typed.Setup[T](factory)
}

// EventSourcedBehavior defines a behavior for a persistent actor.
type EventSourcedBehavior[Command any, Event any, State any] = ptyped.EventSourcedBehavior[Command, Event, State]

// Effect represents the result of processing a command in persistence.
type Effect[Event any, State any] = ptyped.Effect[Event, State]

// Persist creates an effect that persists one or more events.
func Persist[Event any, State any](events ...Event) Effect[Event, State] {
	return ptyped.Persist[Event, State](events...)
}

// PersistThen creates an effect that persists events and then runs a callback.
func PersistThen[Event any, State any](then func(State), events ...Event) Effect[Event, State] {
	return ptyped.PersistThen[Event, State](then, events...)
}

// None represents an effect that does nothing.
func None[Event any, State any]() Effect[Event, State] {
	return ptyped.None[Event, State]()
}

// DurableStateBehavior defines a behavior for a state-persistent actor.
type DurableStateBehavior[Command any, State any] = pstate.DurableStateBehavior[Command, State]

// EntityTypeKey is an alias for styped.EntityTypeKey[M].
type EntityTypeKey[M any] = styped.EntityTypeKey[M]

// EntityRef is an alias for styped.EntityRef[M].
type EntityRef[M any] = styped.EntityRef[M]

// ClusterSingleton is an alias for ctyped.Singleton[M].
type ClusterSingleton[M any] = ctyped.Singleton[M]

// TypedSingletonProxy is an alias for ctyped.TypedSingletonProxy[M].
type TypedSingletonProxy[M any] = ctyped.TypedSingletonProxy[M]

// Topic is an alias for pubsub.Topic[M].
type Topic[M any] = pubsub.Topic[M]

// TopicPublish is an alias for pubsub.Publish[M].
type TopicPublish[M any] = pubsub.Publish[M]

// TopicSubscribe is an alias for pubsub.Subscribe[M].
type TopicSubscribe[M any] = pubsub.Subscribe[M]

// ReceptionistGroup is an alias for receptionist.ReceptionistGroup[T].
type ReceptionistGroup[T any] = receptionist.ReceptionistGroup[T]

// ─── Reliable Delivery ───────────────────────────────────────────────────

// ProducerControllerSendMessage is an alias for delivery.SendMessage.
type ProducerControllerSendMessage = delivery.SendMessage

// ConsumerControllerDelivery is an alias for delivery.Delivery.
type ConsumerControllerDelivery = delivery.Delivery

// ConsumerControllerConfirmed is an alias for delivery.Confirmed.
type ConsumerControllerConfirmed = delivery.Confirmed

// NewProducerController creates a behavior for a producer controller.
func NewProducerController(producerID string) typed.Behavior[any] {
	return delivery.NewProducerController(producerID)
}

// NewConsumerController creates a behavior for a consumer controller.
func NewConsumerController(consumerActor actor.Ref, windowSize int) typed.Behavior[any] {
	return delivery.NewConsumerController(consumerActor, windowSize)
}

// RouterBehavior returns a behavior that drives a classic RouterActor.
func RouterBehavior(r *actor.RouterActor) Behavior[any] {
	return typed.RouterBehavior(r)
}

// DurableState creates a behavior for a state-persistent actor.
func DurableState[C any, S any](persistenceID string, emptyState S, stateStore persistence.DurableStateStore) *DurableStateBehavior[C, S] {
	return &DurableStateBehavior[C, S]{
		PersistenceID: persistenceID,
		EmptyState:    emptyState,
		StateStore:    stateStore,
	}
}

// NewTypedSingleton creates a new type-safe singleton manager factory.
func NewTypedSingleton[M any](cm *Cluster, behavior typed.Behavior[M], role string) *ClusterSingleton[M] {
	return ctyped.NewTypedSingleton(cm.cm, behavior, role)
}

// NewTypedSingletonProxy creates a new type-safe proxy for a cluster singleton.
func NewTypedSingletonProxy[M any](cm *Cluster, managerPath, role string) *TypedSingletonProxy[M] {
	return ctyped.NewTypedSingletonProxy[M](cm.cm, cm.router, managerPath, role)
}

// NewEntityTypeKey creates a new EntityTypeKey.
func NewEntityTypeKey[M any](name string) EntityTypeKey[M] {
	return styped.NewEntityTypeKey[M](name)
}

// ─── Spawner API ──────────────────────────────────────────────────────────

// Spawner is the common interface for spawning actors from either an
// ActorSystem or an ActorContext (TypedContext).
type Spawner interface {
	Spawn(behavior any, name string) (ActorRef, error)
	SpawnAnonymous(behavior any) (ActorRef, error)
	SystemActorOf(behavior any, name string) (ActorRef, error)
}

// NewActorSystemWithBehavior creates a new local actor system and spawns a
// root actor with the given behavior.
func NewActorSystemWithBehavior[T any](behavior Behavior[T], name string, config ...*hocon.Config) (ActorSystem, error) {
	sys, err := NewActorSystem(name, config...)
	if err != nil {
		return nil, err
	}
	_, err = Spawn(sys, behavior, "root")
	return sys, err
}

// actorOfProvider is satisfied by ActorSystem and Cluster.
// It allows the type-safe Spawn wrappers to use ActorOf with a proper
// typed.NewTypedActor[T] factory, bypassing the untyped NewTypedActorGeneric path.
type actorOfProvider interface {
	ActorOf(props Props, name string) (ActorRef, error)
}

// actorOfHierarchicalProvider is satisfied by localActorSystem and Cluster.
type actorOfHierarchicalProvider interface {
	ActorOfHierarchical(props Props, name string, parentPath string) (ActorRef, error)
}

// Spawn creates a new typed actor with the given behavior and name,
// registering it under the /user guardian.
func Spawn[T any, S Spawner](spawner S, behavior Behavior[T], name string) (TypedActorRef[T], error) {
	props := Props{New: func() actor.Actor { return typed.NewTypedActor[T](behavior) }}
	if s, ok := any(spawner).(actorOfProvider); ok {
		ref, err := s.ActorOf(props, name)
		if err != nil {
			return TypedActorRef[T]{}, err
		}
		return typed.NewTypedActorRef[T](ref), nil
	}
	// Fallback: use the untyped spawner (actor.ActorContext path)
	ref, err := spawner.Spawn(behavior, name)
	if err != nil {
		return TypedActorRef[T]{}, err
	}
	return typed.NewTypedActorRef[T](ref), nil
}

// SpawnAnonymous creates a new typed actor with an automatically generated name.
func SpawnAnonymous[T any, S Spawner](spawner S, behavior Behavior[T]) (TypedActorRef[T], error) {
	props := Props{New: func() actor.Actor { return typed.NewTypedActor[T](behavior) }}
	if s, ok := any(spawner).(actorOfProvider); ok {
		ref, err := s.ActorOf(props, "")
		if err != nil {
			return TypedActorRef[T]{}, err
		}
		return typed.NewTypedActorRef[T](ref), nil
	}
	ref, err := spawner.SpawnAnonymous(behavior)
	if err != nil {
		return TypedActorRef[T]{}, err
	}
	return typed.NewTypedActorRef[T](ref), nil
}

// SystemActorOf creates a new typed actor under the /system guardian.
func SystemActorOf[T any, S Spawner](spawner S, behavior Behavior[T], name string) (TypedActorRef[T], error) {
	props := Props{New: func() actor.Actor { return typed.NewTypedActor[T](behavior) }}
	if s, ok := any(spawner).(actorOfHierarchicalProvider); ok {
		ref, err := s.ActorOfHierarchical(props, name, "/system")
		if err != nil {
			return TypedActorRef[T]{}, err
		}
		return typed.NewTypedActorRef[T](ref), nil
	}
	ref, err := spawner.SystemActorOf(behavior, name)
	if err != nil {
		return TypedActorRef[T]{}, err
	}
	return typed.NewTypedActorRef[T](ref), nil
}

// SpawnPersistent creates a new persistent typed actor.
func SpawnPersistent[Command any, Event any, State any](sys ActorSystem, behavior *EventSourcedBehavior[Command, Event, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	ref, err := ptyped.SpawnPersistent(AsActorContext(sys, ""), behavior, name, props...)
	return ref, err
}

// SpawnDurableState creates a new state-persistent typed actor.
func SpawnDurableState[Command any, State any](sys ActorSystem, behavior *DurableStateBehavior[Command, State], name string, props ...actor.Props) (TypedActorRef[Command], error) {
	p := actor.Props{
		New: func() actor.Actor { return pstate.NewDurableStateActor(behavior) },
	}
	if len(props) > 0 {
		p.SupervisorStrategy = props[0].SupervisorStrategy
	}
	ref, err := sys.ActorOf(p, name)
	if err != nil {
		return TypedActorRef[Command]{}, err
	}
	return typed.NewTypedActorRef[Command](ref), nil
}

// Ask sends a message to a typed actor and waits for a reply.
func Ask[T any, R any](ctx context.Context, target TypedActorRef[T], timeout time.Duration, msgFactory func(replyTo TypedActorRef[R]) T) (R, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var zero R
	untyped := target.Untyped()

	type systemProvider interface {
		System() ActorSystem
	}

	var sys ActorSystem
	if sp, ok := untyped.(systemProvider); ok {
		sys = sp.System()
	}

	// If we can't find the system, fall back to the old behavior (only works locally)
	if sys == nil {
		return typed.Ask(ctx, target, timeout, msgFactory)
	}

	cluster, isCluster := sys.(*Cluster)
	if !isCluster {
		return typed.Ask(ctx, target, timeout, msgFactory)
	}

	// Cluster implementation using NodeManager's pending reply mechanism
	tempPath := fmt.Sprintf("/temp/typed-ask/%d", atomic.AddUint64(&askId, 1))
	replyCh := make(chan *core.ArteryMetadata, 1)
	cluster.nm.RegisterPendingReply(tempPath, replyCh)
	defer cluster.nm.UnregisterPendingReply(tempPath)

	// Create a typed ref for the temporary path
	fullTempPath := cluster.SelfPathURI(tempPath)
	replyTo := typed.NewTypedActorRef[R](&remoteAskRef{path: fullTempPath})

	msg := msgFactory(replyTo)
	target.Tell(msg)

	select {
	case meta := <-replyCh:
		payload := meta.DeserializedMessage
		if payload == nil {
			return zero, fmt.Errorf("sharding: received raw payload in Ask, cannot deserialize")
		}

		if result, ok := payload.(R); ok {
			return result, nil
		}
		return zero, fmt.Errorf("sharding: received unexpected reply type: %T (expected %T)", payload, zero)
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type remoteAskRef struct {
	path string
}

func (r *remoteAskRef) Tell(msg any, sender ...actor.Ref) {
	// Not used locally
}
func (r *remoteAskRef) Path() string { return r.path }
