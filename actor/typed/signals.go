/*
 * signals.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"github.com/sopranoworks/gekka/actor"
)

// ─── Signal types ────────────────────────────────────────────────────────

// Signal is the base interface for typed lifecycle signals delivered to actors.
type Signal interface {
	isSignal()
}

// PostStopSignal is delivered after the actor has stopped.
type PostStopSignal struct{}

func (PostStopSignal) isSignal() {}

// PreRestartSignal is delivered before the actor restarts due to a failure.
type PreRestartSignal struct{}

func (PreRestartSignal) isSignal() {}

// TerminatedSignal is delivered when a watched actor terminates.
type TerminatedSignal struct {
	Ref actor.Ref
}

func (TerminatedSignal) isSignal() {}

// ─── StoppedWithPostStop ─────────────────────────────────────────────────

// StoppedWithPostStop returns a behavior that stops the actor and calls
// postStop during the PostStop lifecycle phase.  The callback is registered
// on the actor and executes when the actor system tears down the actor.
func StoppedWithPostStop[T any](postStop func()) Behavior[T] {
	// Return a behavior that registers the postStop hook and then becomes Stopped.
	return func(ctx TypedContext[T], _ T) Behavior[T] {
		ta := extractTypedActor[T](ctx)
		if ta != nil {
			ta.postStopCallback = postStop
		}
		return Stopped[T]()
	}
}

// ─── ReceiveSignal ───────────────────────────────────────────────────────

// SignalHandler is a function that handles typed lifecycle signals.
type SignalHandler func(signal Signal)

// ReceiveSignal creates a [Behavior] that delegates messages to msgHandler and
// lifecycle signals to signalHandler.
//
// Signals are delivered as typed actor messages via the internal signal
// delivery mechanism:
//
//	behavior := typed.ReceiveSignal(
//	    func(ctx TypedContext[MyMsg], msg MyMsg) Behavior[MyMsg] {
//	        // handle normal messages
//	        return Same[MyMsg]()
//	    },
//	    func(signal Signal) {
//	        switch s := signal.(type) {
//	        case TerminatedSignal:
//	            fmt.Println("watched actor terminated:", s.Ref.Path())
//	        case PostStopSignal:
//	            fmt.Println("actor stopped")
//	        }
//	    },
//	)
func ReceiveSignal[T any](msgHandler Behavior[T], signalHandler SignalHandler) Behavior[T] {
	return receiveSignalBehavior(msgHandler, signalHandler)
}

func receiveSignalBehavior[T any](msgHandler Behavior[T], signalHandler SignalHandler) Behavior[T] {
	return func(ctx TypedContext[T], msg T) Behavior[T] {
		// Check if msg is actually a signal wrapper
		if sw, ok := any(msg).(signalWrapper); ok {
			signalHandler(sw.signal)
			return nil // Same — keep current behavior
		}

		next := msgHandler(ctx, msg)
		if next == nil {
			return nil
		}
		if isStopped(next) {
			return Stopped[T]()
		}
		return receiveSignalBehavior(next, signalHandler)
	}
}

// signalWrapper wraps a Signal for delivery as a typed message.
type signalWrapper struct {
	signal Signal
}

// DeliverSignal sends a lifecycle signal to a typed actor.  This is intended
// for framework use — the actor's PostStop hook calls this to deliver
// PostStopSignal to ReceiveSignal-based behaviors.
func DeliverSignal[T any](ref TypedActorRef[T], sig Signal) {
	ref.Untyped().Tell(signalWrapper{signal: sig})
}
