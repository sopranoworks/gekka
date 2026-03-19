/*
 * receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"reflect"

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

// ─── Receptionist Protocol ───────────────────────────────────────────────

type Register[T any] struct {
	Key     ServiceKey[T]
	Service typed.TypedActorRef[T]
}

type Find[T any] struct {
	Key     ServiceKey[T]
	ReplyTo typed.TypedActorRef[Listing[T]]
}

type Subscribe[T any] struct {
	Key        ServiceKey[T]
	Subscriber typed.TypedActorRef[Listing[T]]
}

type Listing[T any] struct {
	Key      ServiceKey[T]
	Services []typed.TypedActorRef[T]
}

// ─── Internal/Non-Generic Protocol ───────────────────────────────────────

type subscribe struct {
	keyID      string
	subscriber typed.TypedActorRef[any]
	sendUpdate func([]string) // closure to send typed Listing back to subscriber
}

type replicatorChanged struct {
	key string
}

// ─── Receptionist Actor ───────────────────────────────────────────────────

func receptionistBehavior(replicator *crdt.Replicator) typed.Behavior[any] {
	type subInfo struct {
		keyID      string
		subscriber actor.Ref
		sendUpdate func([]string)
	}

	var subscribers []subInfo

	return func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
		switch m := msg.(type) {
		case subscribe:
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

		default:
			val := reflect.ValueOf(msg)
			// Handle Register[T] via reflection
			if serviceField := val.FieldByName("Service"); serviceField.IsValid() {
				if keyField := val.FieldByName("Key"); keyField.IsValid() {
					id := keyField.FieldByName("ID").String()
					path := serviceField.MethodByName("Path").Call(nil)[0].String()
					replicator.AddToSet(id, path, crdt.WriteAll)
					ctx.Log().Info("Receptionist: registered service", "key", id, "path", path)
					
					// Notify subscribers
					for _, sub := range subscribers {
						if sub.keyID == id {
							paths := replicator.ORSet(id).Elements()
							sub.sendUpdate(paths)
						}
					}
				}
			}
		}
		return typed.Same[any]()
	}
}

// ─── Public API ──────────────────────────────────────────────────────────

var globalReceptionist typed.TypedActorRef[any]

// Receptionist returns the reference to the local Receptionist actor.
func Receptionist() typed.TypedActorRef[any] {
	return globalReceptionist
}

// SetReceptionist sets the global receptionist reference.
func SetReceptionist(ref typed.TypedActorRef[any]) {
	globalReceptionist = ref
}

// NewServiceKey creates a new type-safe ServiceKey.
func NewServiceKey[T any](id string) ServiceKey[T] {
	return ServiceKey[T]{ID: id}
}
