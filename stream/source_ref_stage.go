/*
 * source_ref_stage.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"errors"
	"sync/atomic"

	"github.com/sopranoworks/gekka/actor"
)

// TypedSourceRef[T] is a type-safe handle to a remote streaming source.
//
// It bundles the network address of the SourceRef stage actor with the codec
// needed to deserialize wire frames back into elements of type T.
type TypedSourceRef[T any] struct {
	// Ref is the serializable address of the SourceRef stage actor.
	Ref SourceRef

	// Decode deserializes wire frames into elements of type T.
	Decode Decoder[T]
}

// ─── ToSourceRef ──────────────────────────────────────────────────────────

// ToSourceRef materializes src into a "SourceRef stage actor" that uses
// the Artery transport for back-pressure and data delivery.
func ToSourceRef[T any](
	ctx actor.ActorContext,
	src Source[T, NotUsed],
	encode Encoder[T],
	decode Decoder[T],
) (*TypedSourceRef[T], error) {
	props := actor.Props{
		New: func() actor.Actor {
			return &sourceRefActor[T]{
				BaseActor: actor.NewBaseActor(),
				src:       src,
				encode:    encode,
			}
		},
	}
	ref, err := ctx.ActorOf(props, "")
	if err != nil {
		return nil, err
	}

	return &TypedSourceRef[T]{
		Ref:    SourceRef{OriginPath: ref.Path()},
		Decode: decode,
	}, nil
}

type sourceRefActor[T any] struct {
	actor.BaseActor
	src      Source[T, NotUsed]
	encode   Encoder[T]
	iter     iterator[T]
	consumer actor.Ref
	demand   int64
	seqNr    int64
}

func (a *sourceRefActor[T]) Receive(msg any) {
	switch m := msg.(type) {
	case *OnSubscribeHandshake:
		if a.iter == nil {
			a.iter, _ = a.src.factory()
			originRef, err := a.System().Resolve(m.TargetRef)
			if err == nil {
				a.consumer = originRef
				a.pushData()
			}
		}
	case *CumulativeDemand:
		if m.SeqNr > a.demand {
			a.demand = m.SeqNr
			a.pushData()
		}
	}
}

func (a *sourceRefActor[T]) pushData() {
	if a.iter == nil || a.consumer == nil {
		return
	}
	for a.seqNr < a.demand {
		elem, ok, err := a.iter.next()
		if err != nil {
			a.consumer.Tell(&RemoteStreamFailure{Cause: err.Error()}, a.Self())
			a.System().Stop(a.Self())
			return
		}
		if !ok {
			a.consumer.Tell(&RemoteStreamCompleted{SeqNr: a.seqNr}, a.Self())
			a.System().Stop(a.Self())
			return
		}

		msgBytes, serID, msgManifest, encErr := a.encode(elem)
		if encErr != nil {
			a.consumer.Tell(&RemoteStreamFailure{Cause: encErr.Error()}, a.Self())
			a.System().Stop(a.Self())
			return
		}

		a.consumer.Tell(&SequencedOnNext{
			SeqNr:        a.seqNr,
			Payload:      msgBytes,
			SerializerID: serID,
			Manifest:     msgManifest,
		}, a.Self())
		a.seqNr++
	}
}

// ─── FromSourceRef ────────────────────────────────────────────────────────

// FromSourceRef returns a [Source][T, NotUsed] that subscribes to the remote
// SourceRef stage and emits its elements locally with demand-driven back-pressure.
func FromSourceRef[T any](ctx actor.ActorContext, ref *TypedSourceRef[T]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			it := &sourceRefConsumerIterator[T]{
				ctx:      ctx,
				decode:   ref.Decode,
				buf:      make(chan T, DefaultAsyncBufSize),
				errCh:    make(chan error, 1),
				notifyCh: make(chan struct{}, 1),
				stopCh:   make(chan struct{}),
			}

			props := actor.Props{
				New: func() actor.Actor {
					return &sourceRefConsumerActor[T]{
						BaseActor: actor.NewBaseActor(),
						it:        it,
						origin:    ref.Ref.OriginPath,
					}
				},
			}
			consumerRef, err := ctx.ActorOf(props, "")
			if err != nil {
				return &errorIterator[T]{err: err}, NotUsed{}
			}
			it.self = consumerRef

			return it, NotUsed{}
		},
	}
}

type sourceRefConsumerActor[T any] struct {
	actor.BaseActor
	it     *sourceRefConsumerIterator[T]
	origin string
}

func (a *sourceRefConsumerActor[T]) PreStart() {
	// Handshake
	originRef, err := a.System().Resolve(a.origin)
	if err == nil {
		originRef.Tell(&OnSubscribeHandshake{TargetRef: a.Self().Path()}, a.Self())
		// Initial demand
		initial := int64(DefaultAsyncBufSize)
		a.it.demanded.Store(initial)
		originRef.Tell(&CumulativeDemand{SeqNr: initial}, a.Self())
	}
}

func (a *sourceRefConsumerActor[T]) Receive(msg any) {
	switch m := msg.(type) {
	case *SequencedOnNext:
		elem, err := a.it.decode(m.Payload, m.SerializerID, m.Manifest)
		if err != nil {
			select {
			case a.it.errCh <- err:
			default:
			}
			return
		}
		select {
		case a.it.buf <- elem:
		case <-a.it.stopCh:
			return
		}
	case *RemoteStreamCompleted:
		close(a.it.buf)
		a.System().Stop(a.Self())
	case *RemoteStreamFailure:
		select {
		case a.it.errCh <- errors.New(m.Cause):
		default:
		}
		a.System().Stop(a.Self())
	case *CumulativeDemand:
		// Forwarded from iterator to origin
		originRef, err := a.System().Resolve(a.origin)
		if err == nil {
			originRef.Tell(m, a.Self())
		}
	}
}

type sourceRefConsumerIterator[T any] struct {
	ctx      actor.ActorContext
	self     actor.Ref
	decode   Decoder[T]
	buf      chan T
	errCh    chan error
	notifyCh chan struct{}
	stopCh   chan struct{}
	demanded atomic.Int64
}

func (it *sourceRefConsumerIterator[T]) next() (T, bool, error) {
	select {
	case err := <-it.errCh:
		var zero T
		return zero, false, err
	case elem, ok := <-it.buf:
		if !ok {
			var zero T
			return zero, false, nil
		}
		newDemand := it.demanded.Add(1)
		it.self.Tell(&CumulativeDemand{SeqNr: newDemand})
		return elem, true, nil
	}
}
