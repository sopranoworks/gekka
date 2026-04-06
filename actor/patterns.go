/*
 * patterns.go
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

// GracefulStop sends stopMsg to the actor and waits for it to terminate
// within timeout. If the actor does not stop in time, an error is returned.
//
// The caller should arrange for the actor to stop upon receiving stopMsg
// (e.g. by handling it in Receive and closing the mailbox, or by sending
// PoisonPill as stopMsg).
//
// This mirrors Pekko's gracefulStop(ref, timeout, stopMsg) pattern.
func GracefulStop(ref Ref, timeout time.Duration, stopMsg any) error {
	if ref == nil {
		return fmt.Errorf("gracefulStop: ref is nil")
	}

	// If the ref is backed by an Actor with a SetOnStop callback, we can
	// detect termination via a channel.
	type watchable interface {
		SetOnStop(func())
	}
	type actorProvider interface {
		Actor() Actor
	}

	done := make(chan struct{})

	// Try to install a stop notification. This works for local actors
	// whose Ref exposes the underlying Actor.
	if ap, ok := ref.(actorProvider); ok {
		if w, ok2 := ap.Actor().(watchable); ok2 {
			prev := func() {} // preserve any existing onStop
			type onStopGetter interface{ getOnStop() func() }
			if g, ok3 := ap.Actor().(onStopGetter); ok3 {
				if f := g.getOnStop(); f != nil {
					prev = f
				}
			}
			w.SetOnStop(func() {
				prev()
				close(done)
			})
		}
	}

	// Send the stop message
	ref.Tell(stopMsg)

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("gracefulStop: actor %s did not stop within %v", ref.Path(), timeout)
	}
}
