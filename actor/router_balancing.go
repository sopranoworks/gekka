/*
 * router_balancing.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"fmt"
	"math"
	"sync/atomic"
)

// ─── BalancingPool ─────────────────────────────────────────────────────────

// BalancingPoolRouter implements a work-stealing pool where all routee actors
// share a single mailbox (channel). The fastest workers naturally pull more
// messages, achieving load balancing without explicit routing decisions.
type BalancingPoolRouter struct {
	BaseActor
	nrOfInstances int
	props         Props
	sharedCh      chan any
	routees       []Ref
	counter       int // for naming
}

// NewBalancingPoolRouter creates a pool where all children share a mailbox.
func NewBalancingPoolRouter(nrOfInstances int, props Props) *BalancingPoolRouter {
	return &BalancingPoolRouter{
		BaseActor:     NewBaseActor(),
		nrOfInstances: nrOfInstances,
		props:         props,
		sharedCh:      make(chan any, 256),
	}
}

// PreStart spawns children that all read from the shared channel.
func (r *BalancingPoolRouter) PreStart() {
	sys := r.System()
	if sys == nil || r.nrOfInstances <= 0 || r.props.New == nil {
		return
	}
	for i := 0; i < r.nrOfInstances; i++ {
		name := fmt.Sprintf("$balanced-%d", r.counter)
		r.counter++
		// Override the actor's mailbox to use the shared channel
		p := Props{
			New: func() Actor {
				a := r.props.New()
				if ba, ok := a.(interface{ SetSharedMailbox(ch chan any) }); ok {
					ba.SetSharedMailbox(r.sharedCh)
				}
				return a
			},
			SupervisorStrategy: r.props.SupervisorStrategy,
			Mailbox:            r.props.Mailbox,
		}
		ref, err := sys.ActorOf(p, name)
		if err != nil {
			continue
		}
		r.routees = append(r.routees, ref)
		sys.Watch(r.Self(), ref)
	}
}

// Receive forwards all messages to the shared channel.
func (r *BalancingPoolRouter) Receive(msg any) {
	switch msg.(type) {
	case TerminatedMessage:
		// A routee died — remove from list
		tm := msg.(TerminatedMessage)
		for i, rt := range r.routees {
			if rt.Path() == tm.TerminatedActor().Path() {
				r.routees = append(r.routees[:i], r.routees[i+1:]...)
				break
			}
		}
	default:
		// Forward to shared channel; routees will pull from it
		select {
		case r.sharedCh <- msg:
		default:
			// Channel full — drop (or could backpressure)
		}
	}
}

// Routees returns a snapshot of the current routee list.
func (r *BalancingPoolRouter) RouteesSnapshot() []Ref {
	cp := make([]Ref, len(r.routees))
	copy(cp, r.routees)
	return cp
}

// ─── SmallestMailboxPool ──────────────────────────────────────────────────

// MailboxSizable is an optional interface that Ref implementations can
// support to expose the current number of pending messages. Used by
// SmallestMailboxRoutingLogic to route to the least-loaded routee.
type MailboxSizable interface {
	MailboxLen() int
}

// SmallestMailboxRoutingLogic routes messages to the routee with the fewest
// queued messages. If a routee's Ref implements MailboxSizable, its
// MailboxLen() is used; otherwise it is treated as having 0 pending messages.
//
// When multiple routees share the smallest size, the first one found is
// selected (deterministic but not round-robin). For tie-breaking with
// round-robin fallback, use a counter-based approach.
type SmallestMailboxRoutingLogic struct {
	fallback atomic.Uint64
}

// Select returns the routee with the smallest mailbox.
func (l *SmallestMailboxRoutingLogic) Select(_ any, routees []Ref) Ref {
	if len(routees) == 0 {
		return nil
	}

	minSize := math.MaxInt
	var minRef Ref

	for _, r := range routees {
		size := 0
		if ms, ok := r.(MailboxSizable); ok {
			size = ms.MailboxLen()
		}
		if size < minSize {
			minSize = size
			minRef = r
		}
	}

	if minRef == nil {
		// Fallback to round-robin if all refs lack MailboxSizable
		idx := l.fallback.Add(1) - 1
		return routees[idx%uint64(len(routees))]
	}

	return minRef
}
