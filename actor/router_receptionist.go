/*
 * router_receptionist.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

// ReceptionistGroup is a router that dynamically discovers its routees
// by subscribing to a ServiceKey via the Receptionist.
type ReceptionistGroup[T any] struct {
	keyID string
	logic RoutingLogic
}

// NewReceptionistGroup creates a new ReceptionistGroup router.
func NewReceptionistGroup[T any](keyID string, logic RoutingLogic) *ReceptionistGroup[T] {
	return &ReceptionistGroup[T]{
		keyID: keyID,
		logic: logic,
	}
}

// listing is an internal message for routee updates.
type listing[T any] struct {
	paths []string
}

// ─── Router Behavior ─────────────────────────────────────────────────────

func (g *ReceptionistGroup[T]) Behavior() Behavior[any] {
	var routees []Ref

	return Setup(func(ctx TypedContext[any]) Behavior[any] {
		// Attempt to subscribe to the Receptionist.
		type receptionistBridge interface {
			SubscribeToReceptionist(keyID string, subscriber TypedActorRef[any], callback func([]string))
		}

		if bridge, ok := ctx.System().(receptionistBridge); ok {
			bridge.SubscribeToReceptionist(g.keyID, ctx.Self(), func(paths []string) {
				ctx.Self().Tell(listing[T]{paths: paths})
			})
		}

		return func(ctx TypedContext[any], msg any) Behavior[any] {
			switch m := msg.(type) {
			case listing[T]:
				newRoutees := make([]Ref, 0, len(m.paths))
				for _, p := range m.paths {
					if ref, err := ctx.System().Resolve(p); err == nil {
						newRoutees = append(newRoutees, ref)
					}
				}
				routees = newRoutees
				ctx.Log().Info("ReceptionistGroup: updated routees", "count", len(routees))
				return Same[any]()

			default:
				if len(routees) == 0 {
					ctx.Log().Warn("ReceptionistGroup: no routees available, dropping message")
					return Same[any]()
				}

				target := g.logic.Select(msg, routees)
				if target != nil {
					target.Tell(msg, ctx.Sender())
				}
			}
			return Same[any]()
		}
	})
}
