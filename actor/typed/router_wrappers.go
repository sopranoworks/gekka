/*
 * router_wrappers.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"github.com/sopranoworks/gekka/actor"
)

// ScatterGatherFirstCompleted is a typed wrapper for the scatter-gather router.
// Deprecated: use NewGroupRouter or NewPoolRouter with ScatterGatherRoutingLogic.
type ScatterGatherFirstCompleted struct {
	actor.ScatterGatherFirstCompleted
}

func (r *ScatterGatherFirstCompleted) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		BridgeClassicToTyped(r, ctx, msg)
		return Same[any]()
	}
}

// TailChoppingFirstCompleted is a typed wrapper for the tail-chopping router.
// Deprecated: use NewGroupRouter or NewPoolRouter with TailChoppingRoutingLogic.
type TailChoppingFirstCompleted struct {
	actor.TailChoppingFirstCompleted
}

func (r *TailChoppingFirstCompleted) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		BridgeClassicToTyped(r, ctx, msg)
		return Same[any]()
	}
}

// RouterBehavior returns a behavior that drives a classic RouterActor.
func RouterBehavior(r *actor.RouterActor) Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		BridgeClassicToTyped(r, ctx, msg)
		return Same[any]()
	}
}

// BridgeClassicToTyped is a helper to allow classic actors to be used as typed behaviors.
func BridgeClassicToTyped(a actor.Actor, ctx TypedContext[any], msg any) {
	// Classic actors need their System and Self injected.
	actor.InjectSystem(a, ctx.System())
	if s, ok := a.(interface{ SetSelf(actor.Ref) }); ok {
		s.SetSelf(ctx.Self().Untyped())
	}
	
	// Inject current sender
	actor.InjectSender(a, ctx.Sender())
	
	// Invoke Receive
	a.Receive(msg)
}
