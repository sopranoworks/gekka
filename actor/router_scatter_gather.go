/*
 * router_scatter_gather.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"time"
)

// ScatterGatherRoutingLogic implements the scatter-gather pattern.
type ScatterGatherRoutingLogic struct {
	Within time.Duration
}

func (l *ScatterGatherRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	return routees[0] // fallback
}

func (l *ScatterGatherRoutingLogic) Route(router *RouterActor, msg any) bool {
	if len(router.Routees) == 0 {
		return false
	}

	sys := router.System()
	aggregatorProps := Props{
		New: func() Actor {
			return &scatterGatherAggregator{
				BaseActor:      NewBaseActor(),
				originalSender: router.Sender(),
				timeout:        l.Within,
			}
		},
	}
	aggRef, err := sys.ActorOf(aggregatorProps, "")
	if err != nil {
		router.Log().Error("ScatterGather: failed to spawn aggregator", "error", err)
		return false
	}

	for _, rt := range router.Routees {
		rt.Tell(msg, aggRef)
	}
	return true
}

// ScatterGatherFirstCompleted is a router implementation that broadcasts
// a message to all routees and provides the first response received back
// to the original sender.
//
// Deprecated: use NewGroupRouter or NewPoolRouter with ScatterGatherRoutingLogic.
type ScatterGatherFirstCompleted struct {
	RouterActor
	within time.Duration
}

// NewScatterGatherFirstCompleted creates a new ScatterGatherFirstCompleted router.
func NewScatterGatherFirstCompleted(routees []Ref, within time.Duration) *ScatterGatherFirstCompleted {
	return &ScatterGatherFirstCompleted{
		RouterActor: RouterActor{
			BaseActor: NewBaseActor(),
			Logic:     &ScatterGatherRoutingLogic{Within: within},
			Routees:   routees,
		},
		within: within,
	}
}

// Behavior returns a typed actor Behavior that drives the scatter-gather router.
func (r *ScatterGatherFirstCompleted) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		r.currentSender = ctx.Sender()
		InjectSystem(r, ctx.System())
		r.Receive(msg)
		return Same[any]()
	}
}

// Receive handles the scatter-gather logic.
func (r *ScatterGatherFirstCompleted) Receive(msg any) {
	r.RouterActor.Receive(msg)
}

// scatterGatherAggregator is a temporary actor that waits for the first response.
type scatterGatherAggregator struct {
	BaseActor
	originalSender Ref
	timeout        time.Duration
	responded      bool
}

func (a *scatterGatherAggregator) PreStart() {
	// Schedule timeout
	time.AfterFunc(a.timeout, func() {
		a.Self().Tell(StatusFailure{Reason: fmt.Errorf("ScatterGather: timeout after %v", a.timeout)})
	})
}

func (a *scatterGatherAggregator) Receive(msg any) {
	if a.responded {
		return
	}

	a.responded = true
	if a.originalSender != nil {
		a.originalSender.Tell(msg, a.Self())
	}

	// Stop the aggregator after the first response or timeout.
	a.System().Stop(a.Self())
}

type StatusFailure struct {
	Reason error
}
