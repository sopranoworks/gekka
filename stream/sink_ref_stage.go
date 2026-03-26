/*
 * sink_ref_stage.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"sync/atomic"

	"github.com/sopranoworks/gekka/actor"
)

// TypedSinkRef[T] is a type-safe handle to a remote streaming sink.
type TypedSinkRef[T any] struct {
	// Ref is the serializable address of the SinkRef stage actor.
	Ref SinkRef

	// Encode serializes elements of type T onto the wire.
	Encode Encoder[T]
}

// ─── ToSinkRef ────────────────────────────────────────────────────────────

// ToSinkRef materializes sink into a "SinkRef stage actor" that uses
// the Artery transport for back-pressure and data delivery.
func ToSinkRef[T any](
	ctx actor.ActorContext,
	sink Sink[T, NotUsed],
	decode Decoder[T],
	encode Encoder[T],
) (*TypedSinkRef[T], error) {
	props := actor.Props{
		New: func() actor.Actor {
			return &sinkRefActor[T]{
				BaseActor: actor.NewBaseActor(),
				sink:      sink,
				decode:    decode,
			}
		},
	}
	ref, err := ctx.ActorOf(props, "")
	if err != nil {
		return nil, err
	}

	return &TypedSinkRef[T]{
		Ref:    SinkRef{TargetPath: ref.Path()},
		Encode: encode,
	}, nil
}

type sinkRefActor[T any] struct {
	actor.BaseActor
	sink   Sink[T, NotUsed]
	decode Decoder[T]
	it     *sinkRefIterator[T]
}

func (a *sinkRefActor[T]) Receive(msg any) {
	switch m := msg.(type) {
	case *OnSubscribeHandshake:
		if a.it == nil {
			a.it = &sinkRefIterator[T]{
				ctx:      a.System(),
				self:     a.Self(),
				origin:   m.TargetRef,
				decode:   a.decode,
				buf:      make(chan T, DefaultAsyncBufSize),
				notifyCh: make(chan struct{}, 1),
				stopCh:   make(chan struct{}),
			}
			// Initial demand
			initial := int64(DefaultAsyncBufSize)
			a.it.demanded.Store(initial)
			originRef, err := a.System().Resolve(m.TargetRef)
			if err == nil {
				originRef.Tell(&CumulativeDemand{SeqNr: initial}, a.Self())
			}

			go func() {
				_, _ = a.sink.runWith(a.it)
				a.System().Stop(a.Self())
			}()
		}
	case *SequencedOnNext:
		if a.it != nil {
			elem, err := a.decode(m.Payload, m.SerializerID, m.Manifest)
			if err == nil {
				a.it.buf <- elem
			}
		}
	case *RemoteStreamCompleted:
		if a.it != nil {
			close(a.it.buf)
		}
	case *RemoteStreamFailure:
		if a.it != nil {
			// handle failure
			close(a.it.buf)
		}
	}
}

type sinkRefIterator[T any] struct {
	ctx      actor.ActorContext
	self     actor.Ref
	origin   string
	decode   Decoder[T]
	buf      chan T
	demanded atomic.Int64
	notifyCh chan struct{}
	stopCh   chan struct{}
}

func (it *sinkRefIterator[T]) next() (T, bool, error) {
	elem, ok := <-it.buf
	if !ok {
		var zero T
		return zero, false, nil
	}
	newDemand := it.demanded.Add(1)
	originRef, err := it.ctx.Resolve(it.origin)
	if err == nil {
		originRef.Tell(&CumulativeDemand{SeqNr: newDemand}, it.self)
	}
	return elem, true, nil
}

// ─── FromSinkRef ──────────────────────────────────────────────────────────

// FromSinkRef returns a [Sink][T, NotUsed] that connects to the remote SinkRef
// stage and pushes upstream elements to it using demand-driven back-pressure.
func FromSinkRef[T any](ctx actor.ActorContext, ref *TypedSinkRef[T]) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			it := &sinkRefProducerIterator[T]{
				ctx:      ctx,
				upstream: upstream,
				encode:   ref.Encode,
				target:   ref.Ref.TargetPath,
				demandCh: make(chan int64, 1),
			}

			props := actor.Props{
				New: func() actor.Actor {
					return &sinkRefProducerActor[T]{
						BaseActor: actor.NewBaseActor(),
						it:        it,
					}
				},
			}
			producerRef, err := ctx.ActorOf(props, "")
			if err != nil {
				return NotUsed{}, err
			}
			it.self = producerRef

			return it.run()
		},
	}
}

type sinkRefProducerActor[T any] struct {
	actor.BaseActor
	it *sinkRefProducerIterator[T]
}

func (a *sinkRefProducerActor[T]) PreStart() {
	targetRef, err := a.System().Resolve(a.it.target)
	if err == nil {
		targetRef.Tell(&OnSubscribeHandshake{TargetRef: a.Self().Path()}, a.Self())
	}
}

func (a *sinkRefProducerActor[T]) Receive(msg any) {
	switch m := msg.(type) {
	case *CumulativeDemand:
		select {
		case a.it.demandCh <- m.SeqNr:
		default:
			// replace pending demand with newer one
			select {
			case <-a.it.demandCh:
			default:
			}
			a.it.demandCh <- m.SeqNr
		}
	case *RemoteStreamFailure:
		// handle failure
	}
}

type sinkRefProducerIterator[T any] struct {
	ctx      actor.ActorContext
	self     actor.Ref
	upstream iterator[T]
	encode   Encoder[T]
	target   string
	demandCh chan int64
}

func (it *sinkRefProducerIterator[T]) run() (NotUsed, error) {
	defer it.ctx.Stop(it.self)

	targetRef, err := it.ctx.Resolve(it.target)
	if err != nil {
		return NotUsed{}, err
	}

	demand := int64(0)
	seqNr := int64(0)

	for {
		for seqNr >= demand {
			select {
			case d := <-it.demandCh:
				if d > demand {
					demand = d
				}
			case <-it.ctx.Context().Done():
				return NotUsed{}, it.ctx.Context().Err()
			}
		}

		elem, ok, pullErr := it.upstream.next()
		if pullErr != nil {
			targetRef.Tell(&RemoteStreamFailure{Cause: pullErr.Error()}, it.self)
			return NotUsed{}, pullErr
		}
		if !ok {
			targetRef.Tell(&RemoteStreamCompleted{SeqNr: seqNr}, it.self)
			return NotUsed{}, nil
		}

		msgBytes, serID, msgManifest, encErr := it.encode(elem)
		if encErr != nil {
			targetRef.Tell(&RemoteStreamFailure{Cause: encErr.Error()}, it.self)
			return NotUsed{}, encErr
		}

		targetRef.Tell(&SequencedOnNext{
			SeqNr:        seqNr,
			Payload:      msgBytes,
			SerializerID: serID,
			Manifest:     msgManifest,
		}, it.self)
		seqNr++
	}
}
