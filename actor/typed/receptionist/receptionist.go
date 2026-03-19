/*
 * receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package receptionist

import (
	"fmt"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/crdt"
)

// ServiceKey is a type-safe identifier for a service.
type ServiceKey[T any] struct {
	ID string
}

func (k ServiceKey[T]) String() string {
	var zero T
	return fmt.Sprintf("ServiceKey[%s, %T]", k.ID, zero)
}

// NewServiceKey creates a new type-safe ServiceKey.
func NewServiceKey[T any](id string) ServiceKey[T] {
	return ServiceKey[T]{ID: id}
}

// ─── Receptionist Protocol ───────────────────────────────────────────────

type Command interface {
	handle(ctx typed.TypedContext[any], replicator *crdt.Replicator, subscribers *[]subInfo)
}

type Register[T any] struct {
	Key     ServiceKey[T]
	Service typed.TypedActorRef[T]
}

func (r Register[T]) handle(ctx typed.TypedContext[any], replicator *crdt.Replicator, subscribers *[]subInfo) {
	id := r.Key.ID
	path := r.Service.Path()
	replicator.AddToSet(id, path, crdt.WriteAll)
	ctx.Log().Info("Receptionist: registered service", "key", id, "path", path)

	// Notify subscribers
	paths := replicator.ORSet(id).Elements()
	for _, sub := range *subscribers {
		if sub.keyID == id {
			sub.sendUpdate(paths)
		}
	}
}

type Find[T any] struct {
	Key     ServiceKey[T]
	ReplyTo typed.TypedActorRef[Listing[T]]
}

func (f Find[T]) handle(ctx typed.TypedContext[any], replicator *crdt.Replicator, _ *[]subInfo) {
	id := f.Key.ID
	paths := replicator.ORSet(id).Elements()
	f.ReplyTo.Tell(constructListing(ctx, f.Key, paths))
}

type Subscribe[T any] struct {
	Key        ServiceKey[T]
	Subscriber typed.TypedActorRef[Listing[T]]
}

func (s Subscribe[T]) handle(ctx typed.TypedContext[any], replicator *crdt.Replicator, subscribers *[]subInfo) {
	id := s.Key.ID
	sub := subInfo{
		keyID:      id,
		subscriber: s.Subscriber.Untyped(),
		sendUpdate: func(paths []string) {
			s.Subscriber.Tell(constructListing(ctx, s.Key, paths))
		},
	}
	*subscribers = append(*subscribers, sub)
	// Initial update
	paths := replicator.ORSet(id).Elements()
	s.Subscriber.Tell(constructListing(ctx, s.Key, paths))
}

type Listing[T any] struct {
	Key      ServiceKey[T]
	Services []typed.TypedActorRef[T]
}

func constructListing[T any](ctx typed.TypedContext[any], key ServiceKey[T], paths []string) Listing[T] {
	services := make([]typed.TypedActorRef[T], 0, len(paths))
	for _, p := range paths {
		if ref, err := ctx.System().Resolve(p); err == nil {
			services = append(services, typed.NewTypedActorRef[T](ref))
		}
	}
	return Listing[T]{
		Key:      key,
		Services: services,
	}
}

// ─── Public API ──────────────────────────────────────────────────────────

var globalReceptionist typed.TypedActorRef[any]

// Global returns the reference to the local Receptionist actor.
func Global() typed.TypedActorRef[any] {
	return globalReceptionist
}

// SetGlobal sets the global receptionist reference.
func SetGlobal(ref typed.TypedActorRef[any]) {
	globalReceptionist = ref
}

// ─── Internal/Non-Generic Protocol ───────────────────────────────────────

type subInfo struct {
	keyID      string
	subscriber actor.Ref
	sendUpdate func([]string)
}

type subscribeInternal struct {
	keyID      string
	subscriber typed.TypedActorRef[any]
	sendUpdate func([]string) // closure to send typed Listing back to subscriber
}

type replicatorChanged struct {
	key string
}

// ─── Receptionist Actor ───────────────────────────────────────────────────

func Behavior(replicator *crdt.Replicator) typed.Behavior[any] {
	var subscribers []subInfo

	return func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
		switch m := msg.(type) {
		case Command:
			m.handle(ctx, replicator, &subscribers)

		case subscribeInternal:
			sub := subInfo{
				keyID:      m.keyID,
				subscriber: m.subscriber.Untyped(),
				sendUpdate: m.sendUpdate,
			}
			subscribers = append(subscribers, sub)
			// Initial update
			paths := replicator.ORSet(m.keyID).Elements()
			m.sendUpdate(paths)

		case replicatorChanged:
			for _, sub := range subscribers {
				if sub.keyID == m.key {
					paths := replicator.ORSet(m.key).Elements()
					sub.sendUpdate(paths)
				}
			}
		}
		return typed.Same[any]()
	}
}

// BridgeInternal allows root package to call internal subscribe.
func BridgeInternal(keyID string, subscriber typed.TypedActorRef[any], callback func([]string)) any {
	return subscribeInternal{
		keyID:      keyID,
		subscriber: subscriber,
		sendUpdate: callback,
	}
}
