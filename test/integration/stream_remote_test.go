/*
 * stream_remote_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package integration_test contains cross-node stream integration tests.
// These tests run as part of the normal unit-test suite (no build tag required)
// because they use only loopback TCP — no Scala/Pekko server is needed.
package integration_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── SourceRef tests ──────────────────────────────────────────────────────

// TestSourceRef_GoToGo_AllElementsDelivered verifies that all elements from a
// local source reach a remote consumer via the SourceRef protocol.
//
// The "remote" side calls ToSourceRef and sends the TypedSourceRef to the
// "local" side via a channel (simulating an actor Ask handoff).  The local side
// calls FromSourceRef and collects the results.
func TestSourceRef_GoToGo_AllElementsDelivered(t *testing.T) {
	const N = 150

	refCh := make(chan *stream.TypedSourceRef[int], 1)

	// ── "Remote" node: materialize the source as a SourceRef stage actor ─
	go func() {
		src := stream.FromSlice(makeInts(N))
		ref, _, err := stream.ToSourceRef(src, intEncode, intDecode)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	// ── "Local" node: receive the ref and subscribe ───────────────────────
	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	result, err := stream.RunWith(
		stream.FromSourceRef(ref),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("FromSourceRef: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestSourceRef_GoToGo_SlowConsumer_NoDrops verifies that back-pressure holds
// when the consumer is slower than the producer: all elements arrive in order
// without any being dropped.
func TestSourceRef_GoToGo_SlowConsumer_NoDrops(t *testing.T) {
	const N = 40

	refCh := make(chan *stream.TypedSourceRef[int], 1)

	go func() {
		ref, _, err := stream.ToSourceRef(
			stream.FromSlice(makeInts(N)),
			intEncode, intDecode,
		)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	var received []int
	sink := stream.Foreach(func(n int) {
		time.Sleep(2 * time.Millisecond) // slow consumer
		received = append(received, n)
	})
	if _, err := stream.FromSourceRef(ref).To(sink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(received) != N {
		t.Fatalf("got %d elements, want %d", len(received), N)
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestSourceRef_GoToGo_BackPressureLimitsInflight verifies that the SourceRef
// stage actor does not produce elements far ahead of the consumer.
//
// With a consumer buffer of DefaultAsyncBufSize the stage must block after
// at most DefaultAsyncBufSize+1 elements are in-flight (buffer + 1 staging slot).
func TestSourceRef_GoToGo_BackPressureLimitsInflight(t *testing.T) {
	const N = 500
	const bufSize = stream.DefaultAsyncBufSize

	refCh := make(chan *stream.TypedSourceRef[int], 1)

	var produced atomic.Int64 // how many elements the "source" has yielded
	go func() {
		src := stream.FromIteratorFunc(func() (int, bool, error) {
			n := int(produced.Add(1)) - 1
			if n >= N {
				return 0, false, nil
			}
			return n, true, nil
		})
		ref, _, err := stream.ToSourceRef(src, intEncode, intDecode)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	var consumed atomic.Int64
	// Consumer pauses briefly before each element.
	sink := stream.Foreach(func(n int) {
		time.Sleep(1 * time.Millisecond)
		c := consumed.Add(1)
		p := produced.Load()
		// Producer must not be more than bufSize+1 ahead of consumer.
		if p-c > int64(bufSize)+1 {
			t.Errorf("too many in-flight: produced=%d consumed=%d diff=%d",
				p, c, p-c)
		}
	})

	if _, err := stream.FromSourceRef(ref).To(sink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if consumed.Load() != N {
		t.Fatalf("consumed %d elements, want %d", consumed.Load(), N)
	}
}

// TestSourceRef_GoToGo_EmptySource verifies that an empty source materializes
// cleanly: no elements are delivered and there is no error.
func TestSourceRef_GoToGo_EmptySource(t *testing.T) {
	refCh := make(chan *stream.TypedSourceRef[int], 1)

	go func() {
		ref, _, err := stream.ToSourceRef(
			stream.FromSlice([]int{}),
			intEncode, intDecode,
		)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	result, err := stream.RunWith(
		stream.FromSourceRef(ref),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected no elements, got %v", result)
	}
}

// TestSourceRef_GoToGo_UpstreamErrorPropagates verifies that a source error on
// the producer side reaches the consumer as a stream failure.
func TestSourceRef_GoToGo_UpstreamErrorPropagates(t *testing.T) {
	sentinelErr := errors.New("source actor failure")
	refCh := make(chan *stream.TypedSourceRef[int], 1)

	go func() {
		ref, _, err := stream.ToSourceRef(
			stream.Failed[int](sentinelErr),
			intEncode, intDecode,
		)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	_, err := stream.RunWith(
		stream.FromSourceRef(ref),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error is transmitted as a string via RemoteStreamFailure, so we
	// check the message rather than errors.Is.
	if err.Error() == "" {
		t.Fatal("received empty error message")
	}
}

// TestSourceRef_GoToGo_LargePayload exercises the protocol with large binary
// payloads to catch any framing or buffering issues.
func TestSourceRef_GoToGo_LargePayload(t *testing.T) {
	const N = 20
	const chunkLen = 64 * 1024 // 64 KiB per element

	payload := make([]byte, chunkLen)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	byteEncode := func(b []byte) ([]byte, int32, []byte, error) { return b, 4, nil, nil }
	byteDecode := func(b []byte, _ int32, _ []byte) ([]byte, error) {
		cp := make([]byte, len(b))
		copy(cp, b)
		return cp, nil
	}

	chunks := make([][]byte, N)
	for i := range chunks {
		chunks[i] = payload
	}

	refCh := make(chan *stream.TypedSourceRef[[]byte], 1)

	go func() {
		ref, _, err := stream.ToSourceRef(stream.FromSlice(chunks), byteEncode, byteDecode)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	result, err := stream.RunWith(
		stream.FromSourceRef(ref),
		stream.Collect[[]byte](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d chunks, want %d", len(result), N)
	}
	for i, chunk := range result {
		if len(chunk) != chunkLen {
			t.Fatalf("chunk %d: got len %d, want %d", i, len(chunk), chunkLen)
		}
	}
}

// TestSourceRef_GoToGo_PipelinedTransformation verifies that the consumer
// can apply a Flow transformation after FromSourceRef.
func TestSourceRef_GoToGo_PipelinedTransformation(t *testing.T) {
	const N = 50

	refCh := make(chan *stream.TypedSourceRef[int], 1)

	go func() {
		ref, _, err := stream.ToSourceRef(stream.FromSlice(makeInts(N)), intEncode, intDecode)
		if err != nil {
			t.Errorf("ToSourceRef: %v", err)
			close(refCh)
			return
		}
		refCh <- ref
	}()

	ref, ok := <-refCh
	if !ok {
		t.Fatal("did not receive TypedSourceRef")
	}

	// Apply a double-map transformation on the consumer side.
	result, err := stream.RunWith(
		stream.Via(
			stream.FromSourceRef(ref),
			stream.Map(func(n int) int { return n * 2 }),
		),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i*2 {
			t.Fatalf("index %d: got %d, want %d", i, v, i*2)
		}
	}
}

// ─── SinkRef tests ────────────────────────────────────────────────────────

// makeSinkRefCapture creates a ToSinkRef stage backed by a Foreach that
// collects N elements into a slice and sends it on resultCh when done.
// This design is race-free: the send on resultCh happens-before the receive,
// and no further writes to the slice occur after the send.
func makeSinkRefCapture(t *testing.T, n int, resultCh chan<- []int) *stream.TypedSinkRef[int] {
	t.Helper()
	var received []int
	count := 0
	sinkRef, _, err := stream.ToSinkRef[int](
		stream.Foreach(func(v int) {
			received = append(received, v)
			count++
			if count == n {
				cp := make([]int, n)
				copy(cp, received)
				resultCh <- cp
			}
		}),
		intDecode, intEncode,
	)
	if err != nil {
		t.Fatalf("ToSinkRef: %v", err)
	}
	return sinkRef
}

// TestSinkRef_GoToGo_AllElementsDelivered verifies the core SinkRef round-trip:
// a remote producer pushes N integers and the local consumer receives them all
// in FIFO order without any loss.
func TestSinkRef_GoToGo_AllElementsDelivered(t *testing.T) {
	const N = 120

	resultCh := make(chan []int, 1)
	sinkRef := makeSinkRefCapture(t, N, resultCh)

	// Producer: push N integers via FromSinkRef.
	_, err := stream.RunWith(
		stream.FromSlice(makeInts(N)),
		stream.FromSinkRef(sinkRef),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	var result []int
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer timed out")
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestSinkRef_GoToGo_SlowProducer_NoDrops verifies that back-pressure holds
// when the producer pauses between elements: all N values arrive correctly.
func TestSinkRef_GoToGo_SlowProducer_NoDrops(t *testing.T) {
	const N = 30

	resultCh := make(chan []int, 1)
	sinkRef := makeSinkRefCapture(t, N, resultCh)

	// Slow producer: 2 ms delay per element.
	slowSrc := stream.Via(
		stream.FromSlice(makeInts(N)),
		stream.Map(func(n int) int {
			time.Sleep(2 * time.Millisecond)
			return n
		}),
	)
	_, err := stream.RunWith(slowSrc, stream.FromSinkRef(sinkRef), stream.ActorMaterializer{})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	var result []int
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer timed out")
	}
	if len(result) != N {
		t.Fatalf("got %d elements, want %d", len(result), N)
	}
	for i, v := range result {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestSinkRef_GoToGo_EmptySource verifies that a producer with an empty source
// closes the SinkRef stage cleanly: no elements are delivered and no error is
// returned by either side.
func TestSinkRef_GoToGo_EmptySource(t *testing.T) {
	// Use a count-based resultCh that fires after 0 elements — since the
	// producer sends zero elements, we cannot use makeSinkRefCapture (which
	// needs at least one element to close the channel).  Instead, verify by
	// checking that the producer's RunWith returns without error, which implies
	// RemoteStreamCompleted was sent and the stage exited cleanly.
	var delivered int
	sinkRef, _, err := stream.ToSinkRef[int](
		stream.Foreach(func(int) { delivered++ }),
		intDecode, intEncode,
	)
	if err != nil {
		t.Fatalf("ToSinkRef: %v", err)
	}

	_, err = stream.RunWith(
		stream.FromSlice([]int{}),
		stream.FromSinkRef(sinkRef),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("producer: unexpected error: %v", err)
	}
	// Brief pause to let the stage actor's goroutine finish its sink loop.
	time.Sleep(50 * time.Millisecond)
	if delivered != 0 {
		t.Fatalf("expected 0 elements delivered, got %d", delivered)
	}
}

// ─── JSON codec helpers (shared with stream_tcp_test.go) ──────────────────

// These are defined in stream_tcp_test.go in the same package; we re-declare
// them here only if that file does not exist in this build.  Since both files
// share package integration_test the symbols from stream_tcp_test.go are
// visible here — so we do NOT redeclare intEncode, intDecode, or makeInts.

// intEncodeJSON and intDecodeJSON are used by tests in this file that need
// JSON-encoded int streams via the SourceRef protocol.
var (
	_ = func() bool { // static proof that intEncode/intDecode/makeInts are in scope
		var _ stream.Encoder[int] = intEncode
		var _ stream.Decoder[int] = intDecode
		var _ []int = makeInts(0)
		return true
	}
)
