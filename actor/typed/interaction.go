/*
 * interaction.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package typed

import (
	"fmt"
	"sync"
	"time"

	"github.com/sopranoworks/gekka/actor"
)

// ── P14-A: PipeToSelf ──────────────────────────────────────────────────────

// PipeToSelf runs fn asynchronously in a goroutine and sends its result
// (adapted via adapt) to the actor's own mailbox. Panics inside fn are
// recovered and converted to errors.
//
// This mirrors Pekko's context.pipeToSelf(future) { case Success(v) => ...; case Failure(e) => ... }.
func PipeToSelf[T any, R any](ctx TypedContext[T], fn func() (R, error), adapt func(R, error) T) {
	self := ctx.Self()
	go func() {
		var result R
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					if e, ok := r.(error); ok {
						err = e
					} else {
						err = fmt.Errorf("pipeToSelf panic: %v", r)
					}
				}
			}()
			result, err = fn()
		}()
		msg := adapt(result, err)
		self.Tell(msg)
	}()
}

// ── P14-B: MessageAdapter ──────────────────────────────────────────────────

// MessageAdapter creates a synthetic ActorRef[U] that adapts external protocol
// messages of type U into the actor's own message type T and forwards them.
//
// This mirrors Pekko's context.messageAdapter[U](f: U => T).
func MessageAdapter[T any, U any](ctx TypedContext[T], adapt func(U) T) TypedActorRef[U] {
	self := ctx.Self()
	return NewTypedActorRef[U](&messageAdapterRef[T, U]{
		self:  self,
		adapt: adapt,
	})
}

type messageAdapterRef[T any, U any] struct {
	self  TypedActorRef[T]
	adapt func(U) T
}

func (r *messageAdapterRef[T, U]) Tell(msg any, sender ...actor.Ref) {
	if u, ok := msg.(U); ok {
		adapted := r.adapt(u)
		r.self.Tell(adapted)
	}
}

func (r *messageAdapterRef[T, U]) Path() string {
	return r.self.Path() + "/$messageAdapter"
}

// ── P14-C: StatusReply / AskWithStatus ─────────────────────────────────────

// StatusReply wraps a success value or an error, following Pekko's StatusReply[T].
type StatusReply[T any] struct {
	Value T
	Err   error
}

// NewStatusReplySuccess creates a successful StatusReply.
func NewStatusReplySuccess[T any](value T) StatusReply[T] {
	return StatusReply[T]{Value: value}
}

// NewStatusReplyError creates an error StatusReply.
func NewStatusReplyError[T any](err error) StatusReply[T] {
	return StatusReply[T]{Err: err}
}

// AskWithStatus sends a request to the target actor and expects a StatusReply[Res]
// response. It unwraps the StatusReply, returning the value on success or the
// error on failure.
//
// createRequest receives a replyTo reference that the target should use to send
// its StatusReply[Res] back.
func AskWithStatus[Req any, Res any](ctx TypedContext[Req], target actor.Ref, createRequest func(replyTo actor.Ref) any, timeout time.Duration) (Res, error) {
	var zero Res
	replyCh := make(chan StatusReply[Res], 1)

	responder := &statusReplyResponder[Res]{replyCh: replyCh}
	msg := createRequest(responder)
	target.Tell(msg)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case reply := <-replyCh:
		if reply.Err != nil {
			return zero, reply.Err
		}
		return reply.Value, nil
	case <-timer.C:
		return zero, actor.ErrAskTimeout
	}
}

type statusReplyResponder[Res any] struct {
	replyCh chan StatusReply[Res]
}

func (r *statusReplyResponder[Res]) Tell(msg any, sender ...actor.Ref) {
	if sr, ok := msg.(StatusReply[Res]); ok {
		select {
		case r.replyCh <- sr:
		default:
		}
	}
}

func (r *statusReplyResponder[Res]) Path() string {
	return "/temp/ask-with-status"
}

// ── P14-D: WatchWith ───────────────────────────────────────────────────────

// WatchWith registers a watch on the target actor and delivers msg to the
// watching actor's mailbox when the target terminates, instead of the default
// Terminated signal.
//
// This mirrors Pekko's context.watchWith(target, msg).
func WatchWith[T any](ctx TypedContext[T], target actor.Ref, msg T) {
	ta := extractTypedActor[T](ctx)
	if ta == nil {
		return
	}
	ctx.Watch(target)
	self := ctx.Self()
	ta.addTerminatedHook(target.Path(), func() {
		self.Tell(msg)
	})
}

// ── P14-E: ReceiveTimeout (typed) ──────────────────────────────────────────

// SetReceiveTimeout configures a receive timeout on the actor. If no message
// is received within d, msg is delivered to the actor's mailbox. The timer
// resets on every message delivery.
//
// This mirrors Pekko's context.setReceiveTimeout(duration, msg).
func SetReceiveTimeout[T any](ctx TypedContext[T], d time.Duration, msg T) {
	ta := extractTypedActor[T](ctx)
	if ta == nil {
		return
	}
	ta.setReceiveTimeout(d, msg)
}

// CancelReceiveTimeout cancels a previously set receive timeout.
func CancelReceiveTimeout[T any](ctx TypedContext[T]) {
	ta := extractTypedActor[T](ctx)
	if ta == nil {
		return
	}
	ta.cancelReceiveTimeout()
}

// receiveTimeoutState holds the state for the receive timeout mechanism
// on a TypedActor.
type receiveTimeoutState[T any] struct {
	mu       sync.Mutex
	duration time.Duration
	msg      T
	timer    *time.Timer
	active   bool
}

// setReceiveTimeout installs or updates the receive timeout on the actor.
func (a *TypedActor[T]) setReceiveTimeout(d time.Duration, msg T) {
	if a.receiveTimeout == nil {
		a.receiveTimeout = &receiveTimeoutState[T]{}
	}
	rt := a.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Cancel any existing timer
	if rt.timer != nil {
		rt.timer.Stop()
	}

	rt.duration = d
	rt.msg = msg
	rt.active = true

	self := a.Self()
	rt.timer = time.AfterFunc(d, func() {
		rt.mu.Lock()
		active := rt.active
		rt.mu.Unlock()
		if active {
			self.Tell(msg)
		}
	})
}

// cancelReceiveTimeout cancels the receive timeout.
func (a *TypedActor[T]) cancelReceiveTimeout() {
	if a.receiveTimeout == nil {
		return
	}
	rt := a.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.active = false
	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}
}

// resetReceiveTimeout resets the receive timeout timer. Called after each
// message delivery.
func (a *TypedActor[T]) resetReceiveTimeout() {
	if a.receiveTimeout == nil {
		return
	}
	rt := a.receiveTimeout
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.active {
		return
	}
	if rt.timer != nil {
		rt.timer.Stop()
	}
	self := a.Self()
	msg := rt.msg
	rt.timer = time.AfterFunc(rt.duration, func() {
		rt.mu.Lock()
		active := rt.active
		rt.mu.Unlock()
		if active {
			self.Tell(msg)
		}
	})
}
