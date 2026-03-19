/*
 * receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	"github.com/sopranoworks/gekka/crdt"
)

// ServiceKey is an alias for receptionist.ServiceKey[T].
type ServiceKey[T any] = receptionist.ServiceKey[T]

// ─── Receptionist Protocol ───────────────────────────────────────────────

type Register[T any] = receptionist.Register[T]

type Find[T any] = receptionist.Find[T]

type Subscribe[T any] = receptionist.Subscribe[T]

type Listing[T any] = receptionist.Listing[T]

// ─── Public API ──────────────────────────────────────────────────────────

// Receptionist returns the reference to the local Receptionist actor.
func Receptionist() typed.TypedActorRef[any] {
	return receptionist.Global()
}

// SetReceptionist sets the global receptionist reference.
func SetReceptionist(ref typed.TypedActorRef[any]) {
	receptionist.SetGlobal(ref)
}

// NewServiceKey creates a new type-safe ServiceKey.
func NewServiceKey[T any](id string) ServiceKey[T] {
	return receptionist.NewServiceKey[T](id)
}

// ─── Internal Factory ────────────────────────────────────────────────────

func spawnReceptionist(sys ActorSystem, replicator *crdt.Replicator) (typed.TypedActorRef[any], error) {
	behavior := receptionist.Behavior(replicator)
	ref, err := Spawn(sys, behavior, "receptionist")
	if err != nil {
		return typed.TypedActorRef[any]{}, err
	}
	SetReceptionist(ref)
	return ref, nil
}
