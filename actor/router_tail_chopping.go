/*
 * router_tail_chopping.go
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

// TailChoppingFirstCompleted is a router implementation that sends a message
// to one routee at a time with a 'within' interval, providing the first 
// response received back to the original sender.
type TailChoppingFirstCompleted struct {
	RouterActor
	within time.Duration
}

// NewTailChoppingFirstCompleted creates a new TailChoppingFirstCompleted router.
func NewTailChoppingFirstCompleted(routees []Ref, within time.Duration) *TailChoppingFirstCompleted {
	return &TailChoppingFirstCompleted{
		RouterActor: RouterActor{
			BaseActor: NewBaseActor(),
			Logic:     &RandomRoutingLogic{}, // Use random order for chopping
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
	if len(r.Routees) == 0 {
		return
	}

	// Create a temporary aggregator actor.
	sys := r.System()
	aggregatorProps := Props{
		New: func() Actor {
			return &tailChoppingAggregator{
				BaseActor:      NewBaseActor(),
				originalSender: r.currentSender,
				interval:       r.within,
				routees:        r.RouteesSnapshot(),
				msg:            msg,
			}
		},
	}
	aggRef, err := sys.ActorOf(aggregatorProps, "")
	if err != nil {
		r.Log().Error("TailChopping: failed to spawn aggregator", "error", err)
		return
	}

	// Kick off the first send.
	aggRef.Tell(nextChoppingCycle{}, r.Self())
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
		a.originalSender.Tell(m, a.Self())
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

type tailChoppingPoolActor struct {
	BaseActor
	nrOfInstances int
	props         Props
	within        time.Duration
	routees       []Ref
}

func (a *tailChoppingPoolActor) PreStart() {
	sys := a.System()
	a.routees = make([]Ref, 0, a.nrOfInstances)
	for i := 0; i < a.nrOfInstances; i++ {
		name := fmt.Sprintf("$pool-%d", i)
		ref, err := sys.ActorOf(a.props, name)
		if err == nil {
			a.routees = append(a.routees, ref)
			sys.Watch(a.Self(), ref)
		}
	}
}

func (a *tailChoppingPoolActor) Receive(msg any) {
	switch m := msg.(type) {
	case TerminatedMessage:
		for i, r := range a.routees {
			if r.Path() == m.TerminatedActor().Path() {
				a.routees = append(a.routees[:i], a.routees[i+1:]...)
				break
			}
		}
	default:
		router := NewTailChoppingFirstCompleted(a.routees, a.within)
		InjectSystem(router, a.System())
		router.SetSelf(a.Self())
		router.currentSender = a.Sender()
		router.Receive(msg)
	}
}
