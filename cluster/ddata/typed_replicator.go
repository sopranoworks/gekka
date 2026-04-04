/*
 * typed_replicator.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package ddata

import (
	"github.com/sopranoworks/gekka/actor/typed"
)

// ReplicatorResponse wraps a CRDT read result for delivery to a typed actor.
type ReplicatorResponse[D any] struct {
	Key   string
	Data  D
	Found bool
	Err   error
}

// TypedReplicatorAdapter bridges a Replicator and a typed actor's command protocol.
//
// Cmd is the typed actor's command union type.
// D is the expected CRDT data type (e.g. *GCounter, *ORSet, *LWWMap).
//
// All operations are non-blocking: responses are delivered asynchronously to
// the owning actor via Self().Tell(wrap(response)).
type TypedReplicatorAdapter[Cmd any, D any] struct {
	replicator *Replicator
	self       typed.TypedActorRef[Cmd]
	wrap       func(ReplicatorResponse[D]) Cmd
	subIDs     map[string]SubscriptionID
}

// NewTypedReplicatorAdapter creates an adapter that routes replicator responses
// into ctx's command protocol using wrap.
//
// Example:
//
//	type MyCmd interface{ isMyCmd() }
//	type GotCounter struct{ Resp ddata.ReplicatorResponse[*ddata.GCounter] }
//	func (GotCounter) isMyCmd() {}
//
//	adapter := ddata.NewTypedReplicatorAdapter(ctx, replicator,
//	    func(r ddata.ReplicatorResponse[*ddata.GCounter]) MyCmd {
//	        return GotCounter{Resp: r}
//	    })
func NewTypedReplicatorAdapter[Cmd any, D any](
	ctx typed.TypedContext[Cmd],
	replicator *Replicator,
	wrap func(ReplicatorResponse[D]) Cmd,
) *TypedReplicatorAdapter[Cmd, D] {
	return &TypedReplicatorAdapter[Cmd, D]{
		replicator: replicator,
		self:       ctx.Self(),
		wrap:       wrap,
		subIDs:     make(map[string]SubscriptionID),
	}
}

// AskGet reads the named CRDT asynchronously and delivers a ReplicatorResponse[D]
// to the owning actor. getter extracts the typed value from the replicator; use
// the convenience helpers (GCounterGetter, ORSetGetter, etc.) or provide a custom one.
func (a *TypedReplicatorAdapter[Cmd, D]) AskGet(
	key string,
	getter func(*Replicator, string) (D, bool),
) {
	repl, self, wrap := a.replicator, a.self, a.wrap
	go func() {
		data, found := getter(repl, key)
		self.Tell(wrap(ReplicatorResponse[D]{Key: key, Data: data, Found: found}))
	}()
}

// AskUpdate applies modify to the replicator and then delivers a ReplicatorResponse[D]
// to the owning actor. getter retrieves the updated value for the response payload.
func (a *TypedReplicatorAdapter[Cmd, D]) AskUpdate(
	key string,
	modify func(*Replicator, string),
	consistency WriteConsistency,
	getter func(*Replicator, string) (D, bool),
) {
	repl, self, wrap := a.replicator, a.self, a.wrap
	go func() {
		modify(repl, key)
		data, found := getter(repl, key)
		self.Tell(wrap(ReplicatorResponse[D]{Key: key, Data: data, Found: found}))
	}()
}

// Subscribe registers a change callback for key. When data for key changes
// via HandleIncoming, assert is called with the raw CRDT object; if it
// returns (data, true), a ReplicatorResponse[D] is delivered to the actor.
//
// Only one subscription per key is tracked per adapter; calling Subscribe
// again for the same key replaces the previous subscription.
func (a *TypedReplicatorAdapter[Cmd, D]) Subscribe(
	key string,
	assert func(any) (D, bool),
) {
	a.Unsubscribe(key) // cancel any existing subscription for this key
	self, wrap := a.self, a.wrap
	id := a.replicator.subscribeKey(key, func(k string, raw any) {
		if data, ok := assert(raw); ok {
			self.Tell(wrap(ReplicatorResponse[D]{Key: k, Data: data, Found: true}))
		}
	})
	a.subIDs[key] = id
}

// Unsubscribe removes the subscription registered for key, if any.
func (a *TypedReplicatorAdapter[Cmd, D]) Unsubscribe(key string) {
	if id, ok := a.subIDs[key]; ok {
		a.replicator.unsubscribeKey(key, id)
		delete(a.subIDs, key)
	}
}

// UnsubscribeAll removes every subscription registered by this adapter.
func (a *TypedReplicatorAdapter[Cmd, D]) UnsubscribeAll() {
	for key, id := range a.subIDs {
		a.replicator.unsubscribeKey(key, id)
	}
	a.subIDs = make(map[string]SubscriptionID)
}

// ── Convenience getters ──────────────────────────────────────────────────────

// GCounterGetter extracts a *GCounter from the replicator by key.
func GCounterGetter(r *Replicator, key string) (*GCounter, bool) {
	r.mu.RLock()
	c, ok := r.counters[key]
	r.mu.RUnlock()
	return c, ok
}

// ORSetGetter extracts an *ORSet from the replicator by key.
func ORSetGetter(r *Replicator, key string) (*ORSet, bool) {
	r.mu.RLock()
	s, ok := r.sets[key]
	r.mu.RUnlock()
	return s, ok
}

// LWWMapGetter extracts a *LWWMap from the replicator by key.
func LWWMapGetter(r *Replicator, key string) (*LWWMap, bool) {
	r.mu.RLock()
	m, ok := r.maps[key]
	r.mu.RUnlock()
	return m, ok
}

// PNCounterGetter extracts a *PNCounter from the replicator by key.
func PNCounterGetter(r *Replicator, key string) (*PNCounter, bool) {
	r.mu.RLock()
	c, ok := r.pnCounters[key]
	r.mu.RUnlock()
	return c, ok
}

// ORFlagGetter extracts an *ORFlag from the replicator by key.
func ORFlagGetter(r *Replicator, key string) (*ORFlag, bool) {
	r.mu.RLock()
	f, ok := r.orFlags[key]
	r.mu.RUnlock()
	return f, ok
}

// LWWRegisterGetter extracts a *LWWRegister from the replicator by key.
func LWWRegisterGetter(r *Replicator, key string) (*LWWRegister, bool) {
	r.mu.RLock()
	reg, ok := r.registers[key]
	r.mu.RUnlock()
	return reg, ok
}
