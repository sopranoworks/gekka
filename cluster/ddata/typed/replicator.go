/*
 * replicator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"reflect"

	"github.com/sopranoworks/gekka/actor/typed"
	"github.com/sopranoworks/gekka/actor/typed/receptionist"
	"github.com/sopranoworks/gekka/cluster/ddata"
)

// Replicator behavior for Distributed Data.
type Replicator struct{}

// ReplicatorServiceKey is the standard key for discovering the replicator via the Receptionist.
var ReplicatorServiceKey = receptionist.NewServiceKey[any]("ddataReplicator")

// ─── Protocol ────────────────────────────────────────────────────────────

// Command is the base interface for replicator commands.
type Command interface {
	handle(replicator *ddata.Replicator, ctx typed.TypedContext[any])
}

// Update updates a CRDT entry.
type Update[L any] struct {
	Key              string
	WriteConsistency ddata.WriteConsistency
	Modify           func(L) L
}

func (u Update[L]) handle(replicator *ddata.Replicator, ctx typed.TypedContext[any]) {
	// Specialized handling based on type L
	var zero L
	switch any(zero).(type) {
	case *ddata.GCounter:
		c := replicator.GCounter(u.Key)
		u.Modify(any(c).(L))
		if u.WriteConsistency == ddata.WriteAll {
			// Trigger immediate gossip if requested (simplified)
			replicator.IncrementCounter(u.Key, 0, ddata.WriteAll)
		}
	case *ddata.ORSet:
		s := replicator.ORSet(u.Key)
		u.Modify(any(s).(L))
		if u.WriteConsistency == ddata.WriteAll {
			replicator.AddToSet(u.Key, "", ddata.WriteAll)
		}
	case *ddata.LWWMap:
		m := replicator.LWWMap(u.Key)
		u.Modify(any(m).(L))
		if u.WriteConsistency == ddata.WriteAll {
			replicator.PutInMap(u.Key, "", nil, ddata.WriteAll)
		}
	default:
		ctx.Log().Error("Replicator: unsupported CRDT type", "type", reflect.TypeOf(zero))
	}
}

// Get retrieves a CRDT entry.
type Get[L any] struct {
	Key     string
	ReplyTo typed.TypedActorRef[GetResponse[L]]
}

type GetResponse[L any] struct {
	Key   string
	Data  L
	Found bool
}

func (g Get[L]) handle(replicator *ddata.Replicator, ctx typed.TypedContext[any]) {
	var zero L
	var data L
	found := false

	switch any(zero).(type) {
	case *ddata.GCounter:
		data = any(replicator.GCounter(g.Key)).(L)
		found = true
	case *ddata.ORSet:
		data = any(replicator.ORSet(g.Key)).(L)
		found = true
	case *ddata.LWWMap:
		data = any(replicator.LWWMap(g.Key)).(L)
		found = true
	}

	g.ReplyTo.Tell(GetResponse[L]{
		Key:   g.Key,
		Data:  data,
		Found: found,
	})
}

// Subscribe subscribes to changes of a CRDT entry.
type Subscribe[L any] struct {
	Key        string
	Subscriber typed.TypedActorRef[Changed[L]]
}

type Changed[L any] struct {
	Key  string
	Data L
}

func (s Subscribe[L]) handle(replicator *ddata.Replicator, ctx typed.TypedContext[any]) {
	// For now, we don't have a built-in subscription mechanism in ddata.Replicator
	// that works with actor refs. We would need to implement it in the core Replicator.
	// As a placeholder, we could poll or wait for ddata.Replicator to support callbacks.
	ctx.Log().Warn("Replicator: Subscribe not yet fully implemented in typed wrapper")
}

// ─── Behavior ─────────────────────────────────────────────────────────────

func (Replicator) Behavior(untypedReplicator *ddata.Replicator) typed.Behavior[any] {
	return func(ctx typed.TypedContext[any], msg any) typed.Behavior[any] {
		if cmd, ok := msg.(Command); ok {
			cmd.handle(untypedReplicator, ctx)
		}
		return typed.Same[any]()
	}
}
