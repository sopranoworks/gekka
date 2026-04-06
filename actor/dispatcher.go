/*
 * dispatcher.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"log/slog"
	"runtime"
)

// DispatcherType selects how an actor's receive loop is scheduled.
// Set Props.Dispatcher to override the default goroutine-per-actor dispatch.
type DispatcherType int

const (
	// DispatcherDefault is the standard goroutine-per-actor scheduler.
	// This is the zero value and requires no explicit configuration.
	DispatcherDefault DispatcherType = iota

	// DispatcherPinned assigns one dedicated OS thread to the actor's goroutine
	// via runtime.LockOSThread(). Use for CPU-bound or blocking actors that
	// must not be migrated between OS threads by the Go scheduler.
	DispatcherPinned

	// DispatcherCallingThread executes Receive synchronously on the caller's
	// goroutine instead of dispatching through a channel. Useful for
	// deterministic unit tests where goroutine scheduling introduces
	// non-determinism; every Tell call blocks until Receive returns.
	DispatcherCallingThread
)

// StartWithDispatcher starts the actor with the given dispatcher type.
// For DispatcherDefault it is identical to Start.
// For DispatcherPinned it uses runtime.LockOSThread to pin the goroutine.
// For DispatcherCallingThread it installs synchronous dispatch (no goroutine).
func StartWithDispatcher(a Actor, d DispatcherType) {
	switch d {
	case DispatcherPinned:
		startPinnedImpl(a)
	case DispatcherCallingThread:
		installCallingThreadImpl(a)
	default:
		Start(a)
	}
}

// startPinnedImpl starts the actor with runtime.LockOSThread() called at the
// beginning of the goroutine, pinning it to a dedicated OS thread for its
// entire lifetime.
func startPinnedImpl(a Actor) {
	type initer interface{ initMailbox() }
	if b, ok := any(a).(initer); ok {
		b.initMailbox()
	}

	type senderSetter interface{ setSender(Ref) }
	ss, hasSS := any(a).(senderSetter)

	go func() {
		// Pin this goroutine to its current OS thread for the actor's lifetime.
		runtime.LockOSThread()
		defer func() {
			runtime.UnlockOSThread()
			if trig, ok := any(a).(interface{ triggerStop() }); ok {
				trig.triggerStop()
			}
			a.PostStop()
		}()

		a.PreStart()

		for raw := range a.Mailbox() {
			switch m := raw.(type) {
			case resumeSignal:
				// resume: continue the loop
			case restartSignal:
				a.PreRestart(m.reason, nil)
			case Envelope:
				if hasSS {
					ss.setSender(m.Sender)
				}
				a.Receive(m.Payload)
				if hasSS {
					ss.setSender(nil)
				}
			default:
				if hasSS {
					ss.setSender(nil)
				}
				a.Receive(raw)
			}
		}
	}()
}

// installCallingThreadImpl configures the actor so that every message sent via
// Send/Tell is processed synchronously on the calling goroutine before returning.
// No new goroutine is started; PreStart is called immediately, PostStop/triggerStop
// are NOT called (no mailbox close event occurs in this mode — the actor is
// expected to be used for synchronous unit tests only).
func installCallingThreadImpl(a Actor) {
	type initer interface{ initMailbox() }
	if b, ok := any(a).(initer); ok {
		b.initMailbox()
	}

	type baseGetter interface{ baseActor() *BaseActor }
	bg, ok := any(a).(baseGetter)
	if !ok {
		// Fallback for actors that don't embed BaseActor: use default dispatch.
		Start(a)
		return
	}
	base := bg.baseActor()

	type senderSetter interface{ setSender(Ref) }
	ss, hasSS := any(a).(senderSetter)

	a.PreStart()

	base.mbSend = func(msg any) bool {
		switch m := msg.(type) {
		case Envelope:
			if hasSS {
				ss.setSender(m.Sender)
			}
			a.Receive(m.Payload)
			if hasSS {
				ss.setSender(nil)
			}
		default:
			if hasSS {
				ss.setSender(nil)
			}
			a.Receive(msg)
		}
		return true
	}
}

// ── LoggingMailbox ─────────────────────────────────────────────────────────────

// NewLoggingMailbox wraps inner with a logging decorator that calls logger.Log
// at level for every message enqueued. Pass nil for inner to wrap the default
// channel-based mailbox.
//
//	mf := actor.NewLoggingMailbox(
//	    actor.NewBoundedMailbox(100, actor.DropNewest),
//	    slog.LevelDebug,
//	    slog.Default(),
//	)
//	ref, _ := system.ActorOf(actor.Props{New: newActor, Mailbox: mf}, "worker")
func NewLoggingMailbox(inner MailboxFactory, level slog.Level, logger *slog.Logger) MailboxFactory {
	if logger == nil {
		logger = slog.Default()
	}
	return &loggingFactory{inner: inner, level: level, logger: logger}
}

type loggingFactory struct {
	inner  MailboxFactory // may be nil → wrap default channel send
	level  slog.Level
	logger *slog.Logger
}

func (f *loggingFactory) installInto(b *BaseActor) {
	// Apply inner factory first (sets up channel + mbSend).
	if f.inner != nil {
		f.inner.installInto(b)
	}

	logger := f.logger
	level := f.level

	if b.mbSend != nil {
		// Wrap whatever mbSend the inner factory installed.
		innerSend := b.mbSend
		b.mbSend = func(msg any) bool {
			logger.Log(nil, level, "mailbox enqueue", "message", msg)
			return innerSend(msg)
		}
	} else {
		// No inner factory (or inner did not set mbSend) — wrap default channel send.
		ch := b.mailbox
		b.mbSend = func(msg any) bool {
			logger.Log(nil, level, "mailbox enqueue", "message", msg)
			select {
			case ch <- msg:
				return true
			default:
				return false
			}
		}
	}
}
