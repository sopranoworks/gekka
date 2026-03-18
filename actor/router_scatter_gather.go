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

// ScatterGatherFirstCompleted is a router implementation that broadcasts
// a message to all routees and provides the first response received back
// to the original sender.
type ScatterGatherFirstCompleted struct {
	RouterActor
	within time.Duration
}

// NewScatterGatherFirstCompleted creates a new ScatterGatherFirstCompleted router.
func NewScatterGatherFirstCompleted(routees []Ref, within time.Duration) *ScatterGatherFirstCompleted {
	return &ScatterGatherFirstCompleted{
		RouterActor: RouterActor{
			BaseActor: NewBaseActor(),
			Logic:     &BroadcastRoutingLogic{},
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
	if len(r.Routees) == 0 {
		return
	}

	// Create a temporary aggregator actor to collect the first response.
	sys := r.System()
	aggregatorProps := Props{
		New: func() Actor {
			return &scatterGatherAggregator{
				BaseActor:      NewBaseActor(),
				originalSender: r.currentSender, // Use currentSender set in Behavior
				timeout:        r.within,
			}
		},
	}
	aggRef, err := sys.ActorOf(aggregatorProps, "")
	if err != nil {
		r.Log().Error("ScatterGather: failed to spawn aggregator", "error", err)
		return
	}

	// Broadcast the message to all routees, with the aggregator as the sender.
	for _, rt := range r.Routees {
		rt.Tell(msg, aggRef)
	}
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

// ── Router Factories ─────────────────────────────────────────────────────

// BroadcastGroup returns props for a GroupRouter using BroadcastRoutingLogic.
func BroadcastGroup(routees []Ref) Props {
	return Props{
		New: func() Actor {
			return NewGroupRouter(&BroadcastRoutingLogic{}, routees)
		},
	}
}

// BroadcastPool returns props for a PoolRouter using BroadcastRoutingLogic.
func BroadcastPool(nrOfInstances int, props Props) Props {
	return Props{
		New: func() Actor {
			return NewPoolRouter(&BroadcastRoutingLogic{}, nrOfInstances, props)
		},
	}
}

// ScatterGatherPool returns props for a ScatterGatherPool router.
func ScatterGatherPool(nrOfInstances int, props Props, within time.Duration) Props {
	return Props{
		New: func() Actor {
			return &scatterGatherPoolActor{
				nrOfInstances: nrOfInstances,
				props:         props,
				within:        within,
			}
		},
	}
}

type scatterGatherPoolActor struct {
	BaseActor
	nrOfInstances int
	props         Props
	within        time.Duration
	routees       []Ref
}

func (a *scatterGatherPoolActor) PreStart() {
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

func (a *scatterGatherPoolActor) Receive(msg any) {
	switch m := msg.(type) {
	case TerminatedMessage:
		// Evict routee
		for i, r := range a.routees {
			if r.Path() == m.TerminatedActor().Path() {
				a.routees = append(a.routees[:i], a.routees[i+1:]...)
				break
			}
		}
	default:
		// Delegate to scatter-gather logic
		router := NewScatterGatherFirstCompleted(a.routees, a.within)
		InjectSystem(router, a.System())
		router.SetSelf(a.Self())
		router.currentSender = a.Sender()
		router.Receive(msg)
	}
}

type StatusFailure struct {
	Reason error
}
