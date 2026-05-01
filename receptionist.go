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
	"github.com/sopranoworks/gekka/cluster/ddata"
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

func spawnReceptionist(sys ActorSystem, replicator *ddata.Replicator, cfg TypedReceptionistConfig) (typed.TypedActorRef[any], error) {
	behavior := receptionist.Behavior(replicator, typedReceptionistConfigToReceptionist(cfg))
	ref, err := Spawn(sys, behavior, "receptionist")
	if err != nil {
		return typed.TypedActorRef[any]{}, err
	}
	SetReceptionist(ref)
	return ref, nil
}

// typedReceptionistConfigToReceptionist maps the HOCON-derived
// TypedReceptionistConfig (string write-consistency, raw fields) into the
// receptionist package's runtime Config (typed ddata.WriteConsistency,
// validated DistributedKeyCount).
func typedReceptionistConfigToReceptionist(cfg TypedReceptionistConfig) receptionist.Config {
	out := receptionist.DefaultConfig()
	switch cfg.WriteConsistency {
	case "majority":
		out.WriteConsistency = ddata.WriteMajority
	case "all":
		out.WriteConsistency = ddata.WriteAll
	case "local", "":
		out.WriteConsistency = ddata.WriteLocal
	default:
		out.WriteConsistency = ddata.WriteLocal
	}
	if cfg.PruningInterval > 0 {
		out.PruningInterval = cfg.PruningInterval
	}
	if cfg.DistributedKeyCount > 0 {
		out.DistributedKeyCount = cfg.DistributedKeyCount
	}
	return out
}
