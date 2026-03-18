/*
 * router_tail_chopping.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"time"
)

// TailChoppingRoutingLogic implements the tail-chopping pattern.
type TailChoppingRoutingLogic struct {
	Within time.Duration
}

func (l *TailChoppingRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}
	return routees[0] // fallback
}

func (l *TailChoppingRoutingLogic) Route(router *RouterActor, msg any) bool {
	if len(router.Routees) == 0 {
		return false
	}

	sys := router.System()
	aggregatorProps := Props{
		New: func() Actor {
			return &tailChoppingAggregator{
				BaseActor:      NewBaseActor(),
				originalSender: router.Sender(),
				interval:       l.Within,
				routees:        router.RouteesSnapshot(),
				msg:            msg,
			}
		},
	}
	aggRef, err := sys.ActorOf(aggregatorProps, "")
	if err != nil {
		router.Log().Error("TailChopping: failed to spawn aggregator", "error", err)
		return false
	}

	aggRef.Tell(nextChoppingCycle{}, router.Self())
	return true
}

// TailChoppingFirstCompleted is a router implementation that sends a message
// to one routee at a time with a 'within' interval, providing the first 
// response received back to the original sender.
//
// Deprecated: use NewGroupRouter or NewPoolRouter with TailChoppingRoutingLogic.
type TailChoppingFirstCompleted struct {
	RouterActor
	within time.Duration
}

// NewTailChoppingFirstCompleted creates a new TailChoppingFirstCompleted router.
func NewTailChoppingFirstCompleted(routees []Ref, within time.Duration) *TailChoppingFirstCompleted {
	return &TailChoppingFirstCompleted{
		RouterActor: RouterActor{
			BaseActor: NewBaseActor(),
			Logic:     &TailChoppingRoutingLogic{Within: within},
			Routees:   routees,
		},
		within: within,
	}
}

// Behavior returns a typed actor Behavior that drives the tail-chopping router.
func (r *TailChoppingFirstCompleted) Behavior() Behavior[any] {
	return func(ctx TypedContext[any], msg any) Behavior[any] {
		r.currentSender = ctx.Sender()
		InjectSystem(r, ctx.System())
		r.Receive(msg)
		return Same[any]()
	}
}

// Receive handles the tail-chopping logic.
func (r *TailChoppingFirstCompleted) Receive(msg any) {
	r.RouterActor.Receive(msg)
}

type nextChoppingCycle struct{}

// tailChoppingAggregator is a temporary actor that manages the chopping cycles.
type tailChoppingAggregator struct {
	BaseActor
	originalSender Ref
	interval       time.Duration
	routees        []Ref
	msg            any
	responded      bool
	index          int
}

func (a *tailChoppingAggregator) Receive(msg any) {
	if a.responded {
		return
	}

	switch m := msg.(type) {
	case nextChoppingCycle:
		if a.index < len(a.routees) {
			target := a.routees[a.index]
			target.Tell(a.msg, a.Self())
			a.index++

			// Schedule next cycle if more routees exist
			if a.index < len(a.routees) {
				time.AfterFunc(a.interval, func() {
					a.Self().Tell(nextChoppingCycle{})
				})
			}
		}

	case StatusFailure:
		// Termination if all failed or timeout (simplified)
		a.responded = true
		if a.originalSender != nil {
			a.originalSender.Tell(m, a.Self())
		}
		a.System().Stop(a.Self())

	default:
		// First successful response
		a.responded = true
		if a.originalSender != nil {
			a.originalSender.Tell(msg, a.Self())
		}
		a.System().Stop(a.Self())
	}
}
