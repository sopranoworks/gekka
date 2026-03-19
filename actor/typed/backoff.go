/*
 * backoff_supervisor.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"fmt"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

type backoffState struct {
	child    actor.Ref
	failures int
}

type startChild struct{}

// NewBackoffSupervisor wraps an actor and manages its lifecycle with exponential backoff.
// It handles messages of type M and forwards them to the child actor.
// If the child fails or stops, it restarts the child after a delay.
func NewBackoffSupervisor[M any](options BackoffOptions, childProps actor.Props) Behavior[any] {
	state := &backoffState{}
	var lastRestart time.Time

	return Setup(func(ctx TypedContext[any]) Behavior[any] {
		// Initial start
		ctx.Self().Tell(startChild{})

		return func(ctx TypedContext[any], msg any) Behavior[any] {
			switch m := msg.(type) {
			case startChild:
				if state.child != nil {
					return Same[any]()
				}
				ctx.Log().Debug("BackoffSupervisor: spawning child")
				child, err := ctx.System().ActorOf(childProps, "child")
				if err != nil {
					ctx.Log().Error("BackoffSupervisor: failed to start child", "error", err)
					state.failures++
					delay := options.NextDelay(state.failures)
					ctx.Timers().StartSingleTimer("restart", startChild{}, delay)
					return Same[any]()
				}
				ctx.Watch(child)
				state.child = child
				lastRestart = time.Now()
				return Same[any]()

			case actor.TerminatedMessage:
				if state.child != nil && m.TerminatedActor().Path() == state.child.Path() {
					state.child = nil
					
					// Reset failure count if child was running long enough
					if time.Since(lastRestart) > options.ResetInterval {
						state.failures = 0
					}
					
					state.failures++
					delay := options.NextDelay(state.failures)
					ctx.Log().Info("BackoffSupervisor: child terminated, scheduling restart", "failures", state.failures, "delay", delay)
					ctx.Timers().StartSingleTimer("restart", startChild{}, delay)
				}

			default:
				// Handle user message type M
				if userMsg, ok := msg.(M); ok {
					if state.child != nil {
						state.child.Tell(userMsg, ctx.Sender())
					} else {
						ctx.Log().Debug("BackoffSupervisor: child not active, dropping message")
					}
				} else {
					ctx.Log().Warn("BackoffSupervisor: received unknown message type", "type", fmt.Sprintf("%T", msg))
				}
			}

			return Same[any]()
		}
	})
}
