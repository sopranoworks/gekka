/*
 * stream_tcp_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package integration_test contains cross-node and TCP-level integration tests
// for the Gekka stream package.
//
// Go-to-Go tests (no build tag) run as part of the normal test suite.
// Go-to-Scala tests (//go:build integration) require a live Pekko/Scala node
// and are excluded by default.
package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/stream"
)

// ─── Codecs for int elements ──────────────────────────────────────────────

// intEncode serialises an int to JSON (Artery JSON serializer ID = 9).
func intEncode(n int) ([]byte, int32, []byte, error) {
	b, err := json.Marshal(n)
	return b, 9, nil, err
}

// intDecode deserialises an int from JSON.
func intDecode(b []byte, _ int32, _ []byte) (int, error) {
	var n int
	return n, json.Unmarshal(b, &n)
}

// makeInts returns a []int{0, 1, …, n-1}.
func makeInts(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

// ─── Tests ────────────────────────────────────────────────────────────────

// TestTcpStream_GoToGo_AllElementsDelivered verifies that all elements emitted
// by TcpOut are received in FIFO order by TcpListener over a loopback TCP
// connection.
func TestTcpStream_GoToGo_AllElementsDelivered(t *testing.T) {
	const N = 200

	listener, err := stream.NewTcpListener[int]("127.0.0.1:0", intDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	// Run TcpOut in a background goroutine.
	outErr := make(chan error, 1)
	go func() {
		src := stream.FromSlice(makeInts(N))
		sink := stream.TcpOut(ref, intEncode)
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	// Materialise the listener source and collect all elements.
	result, err := stream.RunWith(
		listener.Source(),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	if sendErr := <-outErr; sendErr != nil {
		t.Fatalf("TcpOut: %v", sendErr)
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

// TestTcpStream_GoToGo_SlowSink_NoDrops verifies that a slow TcpListener
// consumer receives every element without drops even when TcpOut could
// produce them much faster.
func TestTcpStream_GoToGo_SlowSink_NoDrops(t *testing.T) {
	const N = 50

	listener, err := stream.NewTcpListener[int]("127.0.0.1:0", intDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	outErr := make(chan error, 1)
	go func() {
		src := stream.FromSlice(makeInts(N))
		sink := stream.TcpOut(ref, intEncode)
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	// Slow consumer: 2 ms per element.
	var received []int
	slowSink := stream.Foreach(func(n int) {
		time.Sleep(2 * time.Millisecond)
		received = append(received, n)
	})
	if _, err := listener.Source().To(slowSink).Run(stream.ActorMaterializer{}); err != nil {
		t.Fatalf("listener: %v", err)
	}
	if sendErr := <-outErr; sendErr != nil {
		t.Fatalf("TcpOut: %v", sendErr)
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

// TestTcpStream_GoToGo_BackPressureLimitsInflight confirms that TcpOut does
// not push more data than the listener's buffer can absorb.
//
// With a listener buffer of DefaultAsyncBufSize (16) and a paused consumer,
// TcpOut must block after sending at most DefaultAsyncBufSize+1 elements.
func TestTcpStream_GoToGo_BackPressureLimitsInflight(t *testing.T) {
	const N = 200
	const bufSize = stream.DefaultAsyncBufSize

	listener, err := stream.NewTcpListener[int]("127.0.0.1:0", intDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	// Count how many elements TcpOut has successfully sent.
	var sent atomic.Int64
	i := 0
	outErr := make(chan error, 1)
	go func() {
		src := stream.FromIteratorFunc(func() (int, bool, error) {
			if i >= N {
				return 0, false, nil
			}
			v := i
			i++
			return v, true, nil
		})
		sink := stream.TcpOut(ref, func(n int) ([]byte, int32, []byte, error) {
			b, err := json.Marshal(n)
			if err == nil {
				sent.Add(1)
			}
			return b, 9, nil, err
		})
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	// Pausing gate: the consumer blocks after element 0 until we close the gate.
	gate := make(chan struct{})
	var received []int
	slowSink := stream.Foreach(func(n int) {
		received = append(received, n)
		if n == 0 {
			<-gate
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		listener.Source().To(slowSink).Run(stream.ActorMaterializer{}) //nolint:errcheck
	}()

	// Give TcpOut time to fill the buffer and stall.
	time.Sleep(80 * time.Millisecond)

	s := sent.Load()
	// Expected: bufSize elements in the network/listener buffer, plus the 1
	// element being processed by the paused sink, plus 1 staged by TcpOut.
	const maxAllowed = int64(bufSize) + 3
	if s > maxAllowed {
		close(gate)
		<-done
		t.Fatalf(
			"back-pressure not applied: TcpOut sent %d elements while consumer was paused (max allowed %d, bufSize %d)",
			s, maxAllowed, bufSize,
		)
	}

	close(gate) // unblock consumer
	<-done

	if sendErr := <-outErr; sendErr != nil {
		t.Fatalf("TcpOut: %v", sendErr)
	}
	if len(received) != N {
		t.Fatalf("got %d elements, want %d", N, len(received))
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestTcpStream_GoToGo_ErrorPropagation verifies that a TcpOut upstream
// error (failed source) is propagated to the listener as a RemoteStreamFailure
// frame and surfaced to the consumer as an error.
func TestTcpStream_GoToGo_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("injected upstream error")

	listener, err := stream.NewTcpListener[int]("127.0.0.1:0", intDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	// TcpOut backed by a failing source.
	outErr := make(chan error, 1)
	go func() {
		src := stream.Failed[int](sentinel)
		sink := stream.TcpOut(ref, intEncode)
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	_, listenErr := listener.Source().To(stream.Ignore[int]()).Run(stream.ActorMaterializer{})

	// The listener should surface an error from the remote failure frame.
	if listenErr == nil {
		t.Fatal("expected an error from the listener, got nil")
	}
	t.Logf("listener error (expected): %v", listenErr)

	// TcpOut itself should propagate the sentinel.
	if err := <-outErr; !errors.Is(err, sentinel) {
		t.Fatalf("TcpOut: want sentinel error, got %v", err)
	}
}

// TestTcpStream_GoToGo_ByteArray verifies []byte streaming using the
// ByteArrayEncode / ByteArrayDecode helpers.
func TestTcpStream_GoToGo_ByteArray(t *testing.T) {
	const N = 100

	listener, err := stream.NewTcpListener[[]byte]("127.0.0.1:0", stream.ByteArrayDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	// Source: N payloads containing their index.
	payloads := make([][]byte, N)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("msg-%04d", i))
	}

	outErr := make(chan error, 1)
	go func() {
		src := stream.FromSlice(payloads)
		sink := stream.TcpOut(ref, stream.ByteArrayEncode)
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	result, err := stream.RunWith(
		listener.Source(),
		stream.Collect[[]byte](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	if sendErr := <-outErr; sendErr != nil {
		t.Fatalf("TcpOut: %v", sendErr)
	}

	if len(result) != N {
		t.Fatalf("got %d messages, want %d", len(result), N)
	}
	for i, got := range result {
		want := fmt.Sprintf("msg-%04d", i)
		if string(got) != want {
			t.Fatalf("index %d: got %q, want %q", i, got, want)
		}
	}
}

// TestTcpStream_GoToGo_MultipleAsyncBoundaries combines TcpOut/TcpListener
// with local async boundaries to verify the full async+TCP pipeline.
func TestTcpStream_GoToGo_MultipleAsyncBoundaries(t *testing.T) {
	const N = 80

	listener, err := stream.NewTcpListener[int]("127.0.0.1:0", intDecode)
	if err != nil {
		t.Fatalf("NewTcpListener: %v", err)
	}
	defer listener.Close()

	ref := listener.Ref()

	outErr := make(chan error, 1)
	go func() {
		// Add a local async boundary before the TCP sink.
		src := stream.FromSlice(makeInts(N)).Async()
		sink := stream.TcpOut(ref, intEncode)
		_, err := src.To(sink).Run(stream.ActorMaterializer{})
		outErr <- err
	}()

	// Receive, multiply by 2, collect — all with an async boundary.
	mul2 := stream.Map(func(n int) int { return n * 2 }).Async()
	result, err := stream.RunWith(
		stream.Via(listener.Source(), mul2),
		stream.Collect[int](),
		stream.ActorMaterializer{},
	)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	if sendErr := <-outErr; sendErr != nil {
		t.Fatalf("TcpOut: %v", sendErr)
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
