/*
 * actor_flow.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/actor/typed"
)

// Ask returns a [Flow] that, for each incoming element of type In, sends a
// typed request to ref and emits the reply Out into the stream.
//
// Parameters:
//   - ref      — the target typed actor that accepts messages of type Req.
//   - timeout  — per-element deadline; the flow fails if the actor does not
//     reply within this duration.
//   - makeMsg  — factory called for each element; it receives the element and
//     a temporary typed reply-to reference, and must return the Req message
//     to send to ref.
//
// The flow is intentionally sequential: it sends one Ask at a time and waits
// for the reply before pulling the next element from upstream.  This provides
// natural back-pressure — the upstream cannot produce faster than the actor
// can process requests.
//
// Example — double each integer via a remote actor:
//
//	type DoubleReq struct{ N int; ReplyTo typed.TypedActorRef[int] }
//
//	flow := stream.Ask(
//	    processorRef,
//	    5*time.Second,
//	    func(n int, replyTo typed.TypedActorRef[int]) DoubleReq {
//	        return DoubleReq{N: n, ReplyTo: replyTo}
//	    },
//	)
//	result, err := stream.RunWith(
//	    stream.Via(stream.FromSlice([]int{1, 2, 3}), flow),
//	    stream.Collect[int](),
//	    stream.ActorMaterializer{},
//	)
//	// result == []int{2, 4, 6}
func Ask[In, Out, Req any](
	ref typed.TypedActorRef[Req],
	timeout time.Duration,
	makeMsg func(In, typed.TypedActorRef[Out]) Req,
) Flow[In, Out, NotUsed] {
	return Flow[In, Out, NotUsed]{
		attach: func(upstream iterator[In]) (iterator[Out], NotUsed) {
			return &askIterator[In, Out, Req]{
				upstream: upstream,
				ref:      ref,
				timeout:  timeout,
				makeMsg:  makeMsg,
			}, NotUsed{}
		},
	}
}

// askIterator is the pull-based iterator implementation for [Ask].
// Each call to next() pulls one element from upstream, performs a synchronous
// typed Ask to the actor, and returns the reply.
type askIterator[In, Out, Req any] struct {
	upstream iterator[In]
	ref      typed.TypedActorRef[Req]
	timeout  time.Duration
	makeMsg  func(In, typed.TypedActorRef[Out]) Req
}

func (a *askIterator[In, Out, Req]) next() (Out, bool, error) {
	elem, ok, err := a.upstream.next()
	if !ok || err != nil {
		var zero Out
		return zero, ok, err
	}

	ctx := context.Background()
	reply, err := typed.Ask(ctx, a.ref, a.timeout, func(replyTo typed.TypedActorRef[Out]) Req {
		return a.makeMsg(elem, replyTo)
	})
	if err != nil {
		var zero Out
		return zero, false, err
	}
	return reply, true, nil
}

// FromTypedActorRef returns a [Sink] that forwards every element to ref using
// [typed.TypedActorRef.Tell].  The sink is fire-and-forget: it does not wait
// for the actor to process each message before pulling the next element.
//
// Use [Ask] instead when you need a reply or want to enforce per-element
// back-pressure through the actor.
func FromTypedActorRef[T any](ref typed.TypedActorRef[T]) Sink[T, NotUsed] {
	return Foreach(func(elem T) {
		ref.Tell(elem)
	})
}

// ActorRefWithBackpressure returns a [Sink] that sends each element to ref
// and waits for an ack message before pulling the next element from upstream.
// This provides back-pressure through the actor: if the actor is slow to
// acknowledge, the upstream is throttled.
//
// The ack channel must be provided by the caller and fed by the actor after
// processing each message.
func ActorRefWithBackpressure[T any](ref typed.TypedActorRef[T], ackCh <-chan struct{}) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			for {
				elem, ok, err := upstream.next()
				if err != nil {
					return NotUsed{}, err
				}
				if !ok {
					return NotUsed{}, nil
				}
				ref.Tell(elem)
				// Wait for ack before pulling next
				<-ackCh
			}
		},
	}
}

// ─── ActorSource ──────────────────────────────────────────────────────────

// OverflowStrategy controls the behaviour of [ActorSource] when the internal
// buffer is full and a new message arrives via Tell.
type OverflowStrategy int

const (
	// OverflowDropTail drops the incoming message (the "tail" of the queue).
	// This is the default used by [ActorSource].
	OverflowDropTail OverflowStrategy = iota

	// OverflowDropHead drops the oldest buffered message (the "head") and
	// enqueues the new one, preserving recency at the cost of oldest elements.
	OverflowDropHead

	// OverflowFail fails the stream with [ErrBufferFull] on the first overflow.
	OverflowFail

	// OverflowBackpressure blocks the producer until the consumer catches up.
	// This is meaningful for [Buffer]; in [ActorSource] Tell is fire-and-forget
	// so this strategy falls back to [OverflowDropTail] behaviour.
	OverflowBackpressure
)

// ErrBufferFull is returned by an [ActorSource] stream when the buffer is full
// and the [OverflowFail] strategy is in use.
var ErrBufferFull = errors.New("stream: actor source buffer full")

// actorSourceRef is the internal implementation of actor.Ref that backs an
// ActorSource.  Tell pushes messages into the bounded channel applying the
// chosen overflow strategy; complete() closes the channel to signal EOF.
type actorSourceRef[T any] struct {
	ch       chan T
	failCh   chan error
	strategy OverflowStrategy
	stopped  atomic.Bool
	mu       sync.Mutex // guards DropHead drain-and-enqueue sequence
}

// Tell implements actor.Ref.  The type assertion msg.(T) is always valid here
// because the only callers are TypedActorRef[T].Tell and test code.
func (r *actorSourceRef[T]) Tell(msg any, _ ...actor.Ref) {
	if r.stopped.Load() {
		return
	}
	m, ok := msg.(T)
	if !ok {
		return
	}
	switch r.strategy {
	case OverflowDropTail:
		select {
		case r.ch <- m:
		default: // buffer full: silently drop the new message
		}

	case OverflowDropHead:
		r.mu.Lock()
		defer r.mu.Unlock()
		select {
		case r.ch <- m:
		default:
			// Drain the oldest element to make room, then enqueue.
			select {
			case <-r.ch:
			default:
			}
			r.ch <- m // safe: we hold mu and just freed a slot
		}

	case OverflowFail:
		select {
		case r.ch <- m:
		default:
			select {
			case r.failCh <- ErrBufferFull:
			default: // error already pending
			}
		}
	}
}

// Path implements actor.Ref.
func (r *actorSourceRef[T]) Path() string { return "/stream/actor-source" }

// complete closes the channel, signalling EOF to the downstream iterator.
// Safe to call multiple times.
func (r *actorSourceRef[T]) complete() {
	if r.stopped.CompareAndSwap(false, true) {
		close(r.ch)
	}
}

// actorSourceIterator is the pull-based iterator for [ActorSource].
// next() blocks until an element is available, the source is completed, or
// the stream fails.
type actorSourceIterator[T any] struct {
	ref *actorSourceRef[T]
}

func (a *actorSourceIterator[T]) next() (T, bool, error) {
	// Check for a pending failure without blocking (OverflowFail fast-path).
	select {
	case err := <-a.ref.failCh:
		var zero T
		return zero, false, err
	default:
	}
	// Block on the next element or completion/failure.
	select {
	case err := <-a.ref.failCh:
		var zero T
		return zero, false, err
	case v, ok := <-a.ref.ch:
		if !ok {
			var zero T
			return zero, false, nil // channel closed → normal completion
		}
		return v, true, nil
	}
}

// ActorSource creates a [Source] that is fed by sending messages of type T to
// the returned [typed.TypedActorRef].  The third return value is a complete
// function; call it to signal end-of-stream to the downstream graph.
//
// bufferSize controls the capacity of the internal channel.  When the buffer
// is full, incoming messages are dropped (see [OverflowDropTail]).  Use
// [ActorSourceWithStrategy] for different overflow behaviours.
//
// Example:
//
//	src, ref, complete := stream.ActorSource[int](32)
//	go func() {
//	    for i := range 10 { ref.Tell(i) }
//	    complete()
//	}()
//	result, _ := stream.RunWith(src, stream.Collect[int](), stream.ActorMaterializer{})
func ActorSource[T any](bufferSize int) (Source[T, NotUsed], typed.TypedActorRef[T], func()) {
	return ActorSourceWithStrategy[T](bufferSize, OverflowDropTail)
}

// ActorSourceWithStrategy is like [ActorSource] but lets the caller specify
// how buffer overflows are handled.
func ActorSourceWithStrategy[T any](bufferSize int, strategy OverflowStrategy) (Source[T, NotUsed], typed.TypedActorRef[T], func()) {
	ref := &actorSourceRef[T]{
		ch:       make(chan T, bufferSize),
		failCh:   make(chan error, 1),
		strategy: strategy,
	}
	typedRef := typed.NewTypedActorRef[T](ref)
	src := Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			return &actorSourceIterator[T]{ref: ref}, NotUsed{}
		},
	}
	return src, typedRef, ref.complete
}
