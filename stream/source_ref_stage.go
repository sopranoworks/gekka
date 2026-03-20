/*
 * source_ref_stage.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"fmt"
	"net"
	"time"
)

// ─── TypedSourceRef ───────────────────────────────────────────────────────

// TypedSourceRef[T] is a type-safe handle to a remote streaming source.
//
// It bundles the network address of the SourceRef stage actor with the codec
// needed to deserialize wire frames back into elements of type T.
//
// Obtain a TypedSourceRef by calling [ToSourceRef] on a local [Source].
// Share it with a remote consumer (e.g. via an actor message), then call
// [FromSourceRef] on the receiving side to subscribe to the stream.
//
// TypedSourceRef values are safe to copy and send across goroutines.
type TypedSourceRef[T any] struct {
	// Ref is the serializable address of the SourceRef stage actor.
	// It can be converted to a plain [SourceRef] and sent over Artery.
	Ref SourceRef

	// Decode deserializes wire frames into elements of type T.
	// This field stays local — it is not transmitted over the network.
	Decode Decoder[T]
}

// ─── ToSourceRef ──────────────────────────────────────────────────────────

// ToSourceRef materializes src into a "SourceRef stage actor" — a background
// goroutine that binds a TCP listener and serves exactly one remote consumer.
//
// The source is materialized lazily: the factory is called only when a consumer
// connects and completes the OnSubscribeHandshake.  This means the producer
// does not allocate resources until there is a subscriber.
//
// Parameters:
//   - src    — the local [Source] to expose remotely.
//   - encode — how to serialize T values onto the wire.
//   - decode — how to deserialize wire frames on the consumer side; stored in
//     the returned TypedSourceRef and passed to [FromSourceRef].
//
// Returns:
//   - ref  — share with the consumer (it is safe to copy and send as a message).
//   - stop — call to shut down the listener if no consumer is expected to arrive.
//   - err  — non-nil if the TCP listener cannot be bound.
//
// Example:
//
//	ref, stop, err := stream.ToSourceRef(src, intEncode, intDecode)
//	if err != nil { ... }
//	// send ref to remote consumer via actor message
//	actorRef.Tell(ref)
//	// (stop is only needed if you want to cancel before any consumer arrives)
func ToSourceRef[T any](
	src Source[T, NotUsed],
	encode Encoder[T],
	decode Decoder[T],
) (*TypedSourceRef[T], func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("stream/sourceref: listen: %w", err)
	}

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port) //nolint:errcheck

	ref := &TypedSourceRef[T]{
		Ref: SourceRef{
			OriginPath: fmt.Sprintf("tcp://go@%s:%d/stream/source", host, port),
			Host:       host,
			Port:       port,
		},
		Decode: decode,
	}

	// Stage-actor goroutine: accepts exactly one consumer and serves it.
	// Once Accept returns the listener is no longer needed; the goroutine owns
	// the single connection until the stream completes.
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return // listener was stopped (stop() called) or OS error
		}
		serveSourceRefConsumer(conn, src, encode)
	}()

	return ref, func() { ln.Close() }, nil
}

// serveSourceRefConsumer is the stage-actor main loop for one consumer
// connection.  It implements the server side of the SourceRef wire protocol:
//
//  1. Read the consumer's OnSubscribeHandshake.
//  2. Wait for CumulativeDemand from the consumer.
//  3. Pull elements from the local source and send SequencedOnNext.
//  4. On source exhaustion send RemoteStreamCompleted.
//  5. On source error send RemoteStreamFailure.
func serveSourceRefConsumer[T any](conn net.Conn, src Source[T, NotUsed], encode Encoder[T]) {
	defer conn.Close()

	// ── Step 1: read the consumer's handshake ────────────────────────────
	conn.SetDeadline(time.Now().Add(30 * time.Second)) //nolint:errcheck
	manifest, payload, err := readFrame(conn)
	conn.SetDeadline(time.Time{}) //nolint:errcheck
	if err != nil {
		return
	}
	if manifest != ManifestOnSubscribeHandshake {
		return // protocol error — consumer sent wrong first frame
	}
	_, _ = decodeOnSubscribeHandshake(payload) // consumer path; informational only

	// ── Step 2: materialize the source now that a subscriber has arrived ─
	iter, _ := src.factory()
	seqNr := int64(0)
	demand := int64(0)

	for {
		// ── Wait for (more) demand before sending the next element ─────
		for seqNr >= demand {
			m, p, readErr := readFrame(conn)
			if readErr != nil {
				return // consumer disconnected or network error
			}
			if m != ManifestCumulativeDemand {
				return // unexpected frame type — protocol error
			}
			newDemand, decErr := decodeCumulativeDemand(p)
			if decErr != nil {
				return
			}
			if newDemand > demand {
				demand = newDemand
			}
		}

		// ── Pull one element from the local source ─────────────────────
		elem, ok, pullErr := iter.next()
		if pullErr != nil {
			_ = writeFrame(conn, ManifestRemoteStreamFailure,
				encodeRemoteStreamFailure(pullErr.Error()))
			return
		}
		if !ok {
			_ = writeFrame(conn, ManifestRemoteStreamCompleted,
				encodeRemoteStreamCompleted(seqNr))
			return
		}

		// ── Encode and send ────────────────────────────────────────────
		msgBytes, serID, msgManifest, encErr := encode(elem)
		if encErr != nil {
			_ = writeFrame(conn, ManifestRemoteStreamFailure,
				encodeRemoteStreamFailure(encErr.Error()))
			return
		}
		frame := encodeSequencedOnNext(seqNr, msgBytes, serID, msgManifest)
		if err = writeFrame(conn, ManifestSequencedOnNext, frame); err != nil {
			return // network error — consumer disconnected
		}
		seqNr++
	}
}

// ─── FromSourceRef ────────────────────────────────────────────────────────

// FromSourceRef returns a [Source][T, NotUsed] that subscribes to the remote
// SourceRef stage and emits its elements locally with demand-driven back-pressure.
//
// When the returned source is materialized it dials the stage actor's TCP
// address, sends an OnSubscribeHandshake, and then applies the same
// CumulativeDemand protocol as [TcpListener]: the consumer sends demand signals
// as it consumes elements, and the stage never sends more data than requested.
//
// Example:
//
//	// consumer side (received ref from a remote node)
//	result, err := stream.RunWith(
//	    stream.FromSourceRef(ref),
//	    stream.Collect[int](),
//	    stream.ActorMaterializer{},
//	)
func FromSourceRef[T any](ref *TypedSourceRef[T]) Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			conn, err := net.DialTimeout("tcp",
				fmt.Sprintf("%s:%d", ref.Ref.Host, ref.Ref.Port), 30*time.Second)
			if err != nil {
				return &errorIterator[T]{
					err: fmt.Errorf("stream/sourceref: dial %s:%d: %w",
						ref.Ref.Host, ref.Ref.Port, err),
				}, NotUsed{}
			}
			it, err := newSourceRefConsumerIterator(conn, ref.Decode)
			if err != nil {
				conn.Close()
				return &errorIterator[T]{err: err}, NotUsed{}
			}
			return it, NotUsed{}
		},
	}
}

// newSourceRefConsumerIterator initializes the consumer side of a SourceRef
// connection.  It reuses [tcpInIterator] — which implements the identical
// read-data / send-demand protocol — with consumer-side handshake ordering:
//
//  1. Send OnSubscribeHandshake (consumer → server, reversed from TcpListener).
//  2. Send initial CumulativeDemand (grants DefaultAsyncBufSize slots).
//  3. Start the shared writeLoop and readLoop goroutines.
func newSourceRefConsumerIterator[T any](conn net.Conn, decode Decoder[T]) (*tcpInIterator[T], error) {
	it := &tcpInIterator[T]{
		conn:     conn,
		decode:   decode,
		buf:      make(chan T, DefaultAsyncBufSize),
		errCh:    make(chan error, 1),
		notifyCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}

	// Announce the consumer to the stage actor.
	path := fmt.Sprintf("tcp://go@%s/stream/source-consumer", conn.LocalAddr().String())
	conn.SetDeadline(time.Now().Add(30 * time.Second)) //nolint:errcheck
	if err := writeFrame(conn, ManifestOnSubscribeHandshake,
		encodeOnSubscribeHandshake(path)); err != nil {
		return nil, fmt.Errorf("stream/sourceref: send handshake: %w", err)
	}
	conn.SetDeadline(time.Time{}) //nolint:errcheck

	// Send the initial demand grant — mirrors TcpListener's CumulativeDemand(N).
	initial := int64(DefaultAsyncBufSize)
	it.demanded.Store(initial)
	if err := writeFrame(conn, ManifestCumulativeDemand,
		encodeCumulativeDemand(initial)); err != nil {
		return nil, fmt.Errorf("stream/sourceref: send initial demand: %w", err)
	}

	go it.writeLoop()
	go it.readLoop()
	return it, nil
}
