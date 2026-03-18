/*
 * tcp.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// Encoder[T] serialises a value of type T into a wire payload.
// It returns:
//   - msgBytes     — the serialised bytes
//   - serializerID — Artery serializer identifier (4=ByteArray, 9=JSON, 2=Protobuf …)
//   - manifest     — optional type hint string (nil for ByteArray/JSON)
//   - err          — encoding error, if any
type Encoder[T any] func(T) (msgBytes []byte, serializerID int32, manifest []byte, err error)

// Decoder[T] deserialises a wire payload back into T.
type Decoder[T any] func(msgBytes []byte, serializerID int32, manifest []byte) (T, error)

// ByteArrayEncode encodes a []byte element using Artery ByteArraySerializer (ID 4).
// The bytes are sent as-is with an empty manifest.
func ByteArrayEncode(b []byte) ([]byte, int32, []byte, error) { return b, 4, nil, nil }

// ByteArrayDecode decodes a raw-byte payload — a defensive copy is returned.
func ByteArrayDecode(b []byte, _ int32, _ []byte) ([]byte, error) {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

// ─── TCP frame helpers ────────────────────────────────────────────────────
//
// StreamRef wire frame layout (used on the dedicated data TCP connection):
//
//	┌──────────────────────┬───────────┬───────────────────┐
//	│  length (4 B, LE)    │ manifest  │  protobuf payload │
//	│  = 1 + len(payload)  │  (1 byte) │  (variable)       │
//	└──────────────────────┴───────────┴───────────────────┘
//
// This matches the Pekko Artery StreamRef binary framing so that a Go
// TcpListener and a Pekko SinkRefImpl can interoperate over the same
// connection once the association is established.

func buildFrame(manifest byte, payload []byte) []byte {
	frame := make([]byte, 4+1+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(1+len(payload)))
	frame[4] = manifest
	copy(frame[5:], payload)
	return frame
}

func writeFrame(w io.Writer, manifest byte, payload []byte) error {
	_, err := w.Write(buildFrame(manifest, payload))
	return err
}

func readFrame(r io.Reader) (manifest byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	length := binary.LittleEndian.Uint32(hdr[:])
	if length == 0 {
		err = fmt.Errorf("stream/tcp: empty frame")
		return
	}
	data := make([]byte, length)
	if _, err = io.ReadFull(r, data); err != nil {
		return
	}
	manifest = data[0]
	payload = data[1:]
	return
}

// ─── TcpListener ──────────────────────────────────────────────────────────

// TcpListener[T] is a pre-bound TCP server that accepts exactly one [TcpOut]
// connection and emits the received elements as a stream [Source].
//
// Demand-driven back-pressure is enforced end-to-end: the listener sends
// CumulativeDemand signals (one per element consumed by the downstream) so
// that [TcpOut] never pushes more data than the internal buffer can hold.
//
// Protocol sequence:
//
//	TcpOut  →  listener : OnSubscribeHandshake (G)
//	listener →  TcpOut  : CumulativeDemand(N)  (B)   ← initial grant
//	TcpOut  →  listener : SequencedOnNext×N    (A)   ← data
//	listener →  TcpOut  : CumulativeDemand(N+1)(B)   ← per element consumed
//	…
//	TcpOut  →  listener : RemoteStreamCompleted (D)  ← upstream done
type TcpListener[T any] struct {
	ln     net.Listener
	decode Decoder[T]
	ref    SinkRef
}

// NewTcpListener creates a TcpListener bound to addr.  Pass "" or ":0" to let
// the OS assign a port; the actual address is available via [TcpListener.Ref]
// immediately after construction.
func NewTcpListener[T any](addr string, decode Decoder[T]) (*TcpListener[T], error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("stream/tcp: listen %q: %w", addr, err)
	}
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port) //nolint:errcheck
	ref := SinkRef{
		TargetPath: fmt.Sprintf("tcp://go@%s:%d/stream/sink", host, port),
		Host:       host,
		Port:       port,
	}
	return &TcpListener[T]{ln: ln, decode: decode, ref: ref}, nil
}

// Ref returns the [SinkRef] that should be forwarded to the remote [TcpOut].
func (tl *TcpListener[T]) Ref() SinkRef { return tl.ref }

// Close shuts down the TCP listener.  Any blocked [Source] factory call will
// return an error.
func (tl *TcpListener[T]) Close() error { return tl.ln.Close() }

// Source returns a [Source][T, NotUsed] that accepts one [TcpOut] connection
// and emits the stream elements it receives.
//
// The factory blocks until a connection arrives (or Close is called).  Wrap
// with [Source.Async] to avoid blocking the materializer goroutine:
//
//	result, err := stream.RunWith(listener.Source().Async(), stream.Collect[T](), m)
func (tl *TcpListener[T]) Source() Source[T, NotUsed] {
	return Source[T, NotUsed]{
		factory: func() (iterator[T], NotUsed) {
			conn, err := tl.ln.Accept()
			if err != nil {
				return &errorIterator[T]{err: fmt.Errorf("stream/tcp: accept: %w", err)}, NotUsed{}
			}
			it, err := newTcpInIterator(conn, tl.decode)
			if err != nil {
				conn.Close()
				return &errorIterator[T]{err: err}, NotUsed{}
			}
			return it, NotUsed{}
		},
	}
}

// ─── tcpInIterator ────────────────────────────────────────────────────────

// tcpInIterator is the iterator[T] implementation for TcpListener.
//
// Architecture:
//   - readLoop goroutine  : reads SequencedOnNext frames, pushes to buf.
//   - writeLoop goroutine : drains notifyCh and writes CumulativeDemand frames.
//   - next()              : pops from buf, atomically increments demand,
//     notifies writeLoop.
//
// Back-pressure: buf is bounded at DefaultAsyncBufSize.  readLoop blocks
// sending to buf when it is full, which stalls the remote TcpOut (it cannot
// send past the last CumulativeDemand grant).
type tcpInIterator[T any] struct {
	conn   net.Conn
	decode Decoder[T]

	buf      chan T         // bounded receive buffer
	errCh    chan error     // at most one error from readLoop
	notifyCh chan struct{}  // 1-slot channel: next() notifies writeLoop of demand change
	stopCh   chan struct{}  // closed by stop() to terminate goroutines

	demanded atomic.Int64 // monotonically-increasing cumulative demand
	// stopOnce sync.Once
}

func newTcpInIterator[T any](conn net.Conn, decode Decoder[T]) (*tcpInIterator[T], error) {
	it := &tcpInIterator[T]{
		conn:     conn,
		decode:   decode,
		buf:      make(chan T, DefaultAsyncBufSize),
		errCh:    make(chan error, 1),
		notifyCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}

	// Read the OnSubscribeHandshake before starting background goroutines.
	conn.SetDeadline(time.Now().Add(30 * time.Second)) //nolint:errcheck
	manifest, payload, err := readFrame(conn)
	conn.SetDeadline(time.Time{}) //nolint:errcheck
	if err != nil {
		return nil, fmt.Errorf("stream/tcp: read handshake: %w", err)
	}
	if manifest != ManifestOnSubscribeHandshake {
		return nil, fmt.Errorf("stream/tcp: expected OnSubscribeHandshake (G), got %q", string([]byte{manifest}))
	}
	if _, err = decodeOnSubscribeHandshake(payload); err != nil {
		return nil, fmt.Errorf("stream/tcp: decode handshake: %w", err)
	}

	// Grant the initial demand equal to the buffer capacity.
	initial := int64(DefaultAsyncBufSize)
	it.demanded.Store(initial)
	if err = writeFrame(conn, ManifestCumulativeDemand, encodeCumulativeDemand(initial)); err != nil {
		return nil, fmt.Errorf("stream/tcp: send initial demand: %w", err)
	}

	go it.writeLoop()
	go it.readLoop()
	return it, nil
}

// writeLoop drains notifyCh and sends a CumulativeDemand frame with the
// latest cumulative demand value (coalescing multiple next() calls into a
// single frame when the remote is slow to consume them).
func (it *tcpInIterator[T]) writeLoop() {
	for {
		select {
		case <-it.notifyCh:
			// Drain any additional pending notifications (coalesce).
		drain:
			for {
				select {
				case <-it.notifyCh:
				default:
					break drain
				}
			}
			demand := it.demanded.Load()
			frame := buildFrame(ManifestCumulativeDemand, encodeCumulativeDemand(demand))
			if _, err := it.conn.Write(frame); err != nil {
				return
			}
		case <-it.stopCh:
			return
		}
	}
}

// readLoop reads StreamRef control frames from the TCP connection and routes
// them to the appropriate channel.
func (it *tcpInIterator[T]) readLoop() {
	defer close(it.buf)

	received := int64(0)
	for {
		manifest, payload, err := readFrame(it.conn)
		if err != nil {
			select {
			case it.errCh <- fmt.Errorf("stream/tcp: read: %w", err):
			default:
			}
			return
		}

		switch manifest {
		case ManifestSequencedOnNext:
			seqNr, msgBytes, serID, msgManifest, err := decodeSequencedOnNext(payload)
			if err != nil {
				select {
				case it.errCh <- err:
				default:
				}
				return
			}
			if seqNr != received {
				select {
				case it.errCh <- fmt.Errorf("stream/tcp: sequence gap: expected %d got %d", received, seqNr):
				default:
				}
				return
			}
			elem, err := it.decode(msgBytes, serID, msgManifest)
			if err != nil {
				select {
				case it.errCh <- fmt.Errorf("stream/tcp: decode element %d: %w", seqNr, err):
				default:
				}
				return
			}
			received++
			select {
			case it.buf <- elem:
			case <-it.stopCh:
				return
			}

		case ManifestRemoteStreamCompleted:
			return // normal upstream completion

		case ManifestRemoteStreamFailure:
			cause := decodeRemoteStreamFailure(payload)
			select {
			case it.errCh <- fmt.Errorf("stream/tcp: remote failure: %s", cause):
			default:
			}
			return

		default:
			// Ignore unknown manifest codes for forward compatibility.
		}
	}
}

// next implements iterator[T].
func (it *tcpInIterator[T]) next() (T, bool, error) {
	elem, ok := <-it.buf
	if !ok {
		select {
		case err := <-it.errCh:
			var zero T
			return zero, false, err
		default:
			var zero T
			return zero, false, nil
		}
	}

	// Increment cumulative demand by one and notify writeLoop.
	it.demanded.Add(1)
	select {
	case it.notifyCh <- struct{}{}:
	default: // writeLoop already has a pending notification
	}

	return elem, true, nil
}

/*
func (it *tcpInIterator[T]) stop() {
	it.stopOnce.Do(func() { close(it.stopCh) })
}
*/

// ─── TcpOut ───────────────────────────────────────────────────────────────

// TcpOut[T] returns a [Sink][T, NotUsed] that connects to the [TcpListener]
// identified by ref and pushes upstream elements over TCP using the Artery
// StreamRef protocol.
//
// Back-pressure is enforced at the wire level: TcpOut blocks after sending
// the number of elements equal to the last received CumulativeDemand value,
// waiting for the remote listener to grant more capacity before pulling the
// next element from upstream.  This prevents the producer from running
// arbitrarily far ahead of a slow consumer.
//
// encode serialises each element into the wire payload (see [Encoder]).
// For raw byte streams use [ByteArrayEncode]; for JSON use a custom encoder
// with serializer ID 9.
func TcpOut[T any](ref SinkRef, encode Encoder[T]) Sink[T, NotUsed] {
	return Sink[T, NotUsed]{
		runWith: func(upstream iterator[T]) (NotUsed, error) {
			conn, err := net.DialTimeout("tcp",
				fmt.Sprintf("%s:%d", ref.Host, ref.Port), 30*time.Second)
			if err != nil {
				return NotUsed{}, fmt.Errorf("stream/tcp: dial %s:%d: %w", ref.Host, ref.Port, err)
			}
			defer conn.Close()

			// Send OnSubscribeHandshake so the remote knows our synthetic path.
			path := fmt.Sprintf("tcp://go@%s:%d/stream/out", ref.Host, ref.Port)
			if err = writeFrame(conn, ManifestOnSubscribeHandshake, encodeOnSubscribeHandshake(path)); err != nil {
				return NotUsed{}, fmt.Errorf("stream/tcp: send handshake: %w", err)
			}

			// Read the initial CumulativeDemand from the listener.
			manifest, payload, err := readFrame(conn)
			if err != nil {
				return NotUsed{}, fmt.Errorf("stream/tcp: read initial demand: %w", err)
			}
			if manifest != ManifestCumulativeDemand {
				return NotUsed{}, fmt.Errorf("stream/tcp: expected CumulativeDemand (B), got %q",
					string([]byte{manifest}))
			}
			demand, err := decodeCumulativeDemand(payload)
			if err != nil {
				return NotUsed{}, fmt.Errorf("stream/tcp: decode initial demand: %w", err)
			}

			seqNr := int64(0)
			for {
				// ── Wait for demand before sending the next element ──────
				// This is the core back-pressure loop: we block here when
				// the remote's buffer is full and no new demand has arrived.
				for seqNr >= demand {
					manifest, payload, err = readFrame(conn)
					if err != nil {
						return NotUsed{}, fmt.Errorf("stream/tcp: read demand: %w", err)
					}
					if manifest != ManifestCumulativeDemand {
						return NotUsed{}, fmt.Errorf("stream/tcp: unexpected frame while waiting for demand: %q",
							string([]byte{manifest}))
					}
					newDemand, err := decodeCumulativeDemand(payload)
					if err != nil {
						return NotUsed{}, fmt.Errorf("stream/tcp: decode demand: %w", err)
					}
					if newDemand > demand {
						demand = newDemand
					}
				}

				// ── Pull element from upstream ────────────────────────────
				elem, ok, pullErr := upstream.next()
				if pullErr != nil {
					_ = writeFrame(conn, ManifestRemoteStreamFailure,
						encodeRemoteStreamFailure(pullErr.Error()))
					return NotUsed{}, pullErr
				}
				if !ok {
					return NotUsed{}, writeFrame(conn,
						ManifestRemoteStreamCompleted, encodeRemoteStreamCompleted(seqNr))
				}

				// ── Encode and send ───────────────────────────────────────
				msgBytes, serID, msgManifest, encErr := encode(elem)
				if encErr != nil {
					_ = writeFrame(conn, ManifestRemoteStreamFailure,
						encodeRemoteStreamFailure(encErr.Error()))
					return NotUsed{}, fmt.Errorf("stream/tcp: encode element %d: %w", seqNr, encErr)
				}
				frame := encodeSequencedOnNext(seqNr, msgBytes, serID, msgManifest)
				if err = writeFrame(conn, ManifestSequencedOnNext, frame); err != nil {
					return NotUsed{}, fmt.Errorf("stream/tcp: send element %d: %w", seqNr, err)
				}
				seqNr++
			}
		},
	}
}
