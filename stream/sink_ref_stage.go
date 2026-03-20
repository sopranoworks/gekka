/*
 * sink_ref_stage.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"fmt"
	"net"
)

// ─── TypedSinkRef ─────────────────────────────────────────────────────────

// TypedSinkRef[T] is a type-safe handle to a remote streaming sink.
//
// It bundles the network address of the SinkRef stage actor with the codec
// needed to serialize elements of type T onto the wire.
//
// Obtain a TypedSinkRef by calling [ToSinkRef] on a local [Sink].
// Share it with a remote producer (e.g. via an actor message), then call
// [FromSinkRef] on the producing side to connect and push elements.
//
// TypedSinkRef values are safe to copy and send across goroutines.
type TypedSinkRef[T any] struct {
	// Ref is the serializable address of the SinkRef stage actor.
	// It can be converted to a plain [SinkRef] and sent over Artery.
	Ref SinkRef

	// Encode serializes elements of type T onto the wire.
	// This field stays local — it is not transmitted over the network.
	Encode Encoder[T]
}

// ─── ToSinkRef ────────────────────────────────────────────────────────────

// ToSinkRef materializes sink into a "SinkRef stage actor" — a background
// goroutine that binds a TCP listener and accepts exactly one remote producer.
// Received elements are forwarded to the local sink.
//
// The sink is driven lazily: the pipeline does not start until the remote
// producer connects and completes the OnSubscribeHandshake.
//
// Parameters:
//   - sink   — the local [Sink] that will consume the remote elements.
//   - decode — how to deserialize wire frames into T.
//   - encode — how to serialize T onto the wire; stored in the returned ref
//     for use by [FromSinkRef] on the producer side.
//
// Returns:
//   - ref  — share with the remote producer.
//   - stop — call to shut down the listener before any producer connects.
//   - err  — non-nil if the TCP listener cannot be bound.
//
// Example:
//
//	// consumer side — set up sink and share the ref
//	ref, stop, err := stream.ToSinkRef(
//	    stream.Collect[int](),
//	    intDecode, intEncode,
//	)
//	_ = stop // stop can be called to cancel before anyone connects
//	actorRef.Tell(ref) // hand off to remote producer
func ToSinkRef[T any](
	sink Sink[T, NotUsed],
	decode Decoder[T],
	encode Encoder[T],
) (*TypedSinkRef[T], func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("stream/sinkref: listen: %w", err)
	}

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port) //nolint:errcheck

	ref := &TypedSinkRef[T]{
		Ref: SinkRef{
			TargetPath: fmt.Sprintf("tcp://go@%s:%d/stream/sink", host, port),
			Host:       host,
			Port:       port,
		},
		Encode: encode,
	}

	// Stage-actor goroutine: accepts exactly one producer and drives sink.
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return // listener was stopped (stop() called) or OS error
		}
		serveSinkRefProducer(conn, sink, decode)
	}()

	return ref, func() { ln.Close() }, nil
}

// serveSinkRefProducer is the stage-actor main loop for one producer connection.
// It reuses [newTcpInIterator] — which handles the complete receive-and-demand
// protocol — and then drives the local sink with the received elements.
//
// Protocol sequence (server perspective):
//
//  1. Read OnSubscribeHandshake from the producer.
//  2. Send initial CumulativeDemand (grants DefaultAsyncBufSize slots).
//  3. Receive SequencedOnNext frames; drain demand; repeat.
//  4. Producer sends RemoteStreamCompleted → iterator closes → sink finishes.
func serveSinkRefProducer[T any](conn net.Conn, sink Sink[T, NotUsed], decode Decoder[T]) {
	defer conn.Close()

	// newTcpInIterator handles the full handshake + demand + data protocol.
	it, err := newTcpInIterator(conn, decode)
	if err != nil {
		return
	}
	// Drive the local sink with elements received from the remote producer.
	sink.runWith(it) //nolint:errcheck
}

// ─── FromSinkRef ──────────────────────────────────────────────────────────

// FromSinkRef returns a [Sink][T, NotUsed] that connects to the remote SinkRef
// stage and pushes upstream elements to it using demand-driven back-pressure.
//
// The remote stage controls throughput: the sink blocks after filling the
// remote's initial CumulativeDemand grant and waits for the stage to request
// more elements before pulling from upstream.  This prevents the producer from
// running ahead of a slow consumer.
//
// FromSinkRef is a thin wrapper around [TcpOut] — it reuses the identical
// OnSubscribeHandshake → CumulativeDemand → SequencedOnNext wire protocol.
//
// Example:
//
//	// producer side (received ref from remote consumer)
//	_, err := stream.RunWith(
//	    stream.FromSlice(elements),
//	    stream.FromSinkRef(ref),
//	    stream.ActorMaterializer{},
//	)
func FromSinkRef[T any](ref *TypedSinkRef[T]) Sink[T, NotUsed] {
	return TcpOut(ref.Ref, ref.Encode)
}
