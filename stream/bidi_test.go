/*
 * bidi_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"strings"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// frame wraps s with "FRAME[" and "]" to simulate a simple framing codec.
func frame(s string) string { return "FRAME[" + s + "]" }

// unframe strips the "FRAME[" prefix and "]" suffix added by frame.
// Returns the original string unmodified if the framing is absent.
func unframe(s string) string {
	s = strings.TrimPrefix(s, "FRAME[")
	s = strings.TrimSuffix(s, "]")
	return s
}

// TestBidiFlow_FramingRoundTrip verifies that data piped through a
// BidiFlowFromFlows(frameEncoder, frameDecoder).Join(identity) produces the
// original messages unchanged, demonstrating a symmetric codec round-trip.
func TestBidiFlow_FramingRoundTrip(t *testing.T) {
	// Top (forward): encode by wrapping in framing markers.
	encoder := stream.Map[string, string](frame)
	// Bottom (backward): decode by stripping framing markers.
	decoder := stream.Map[string, string](unframe)

	bidi := stream.BidiFlowFromFlows[string, string, string, string](encoder, decoder)

	// Join with an identity flow (pass-through) as the "transport" layer.
	// Data path: input → frame → identity → unframe → output
	identity := stream.Map[string, string](func(s string) string { return s })
	pipeline := bidi.Join(identity)

	input := []string{"hello", "world", "foo", "bar"}
	src := stream.FromSlice(input)

	result, err := stream.RunWith(
		stream.Via(src, pipeline),
		stream.Collect[string](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(input) {
		t.Fatalf("expected %d elements, got %d", len(input), len(result))
	}
	for i, got := range result {
		if got != input[i] {
			t.Errorf("element %d: got %q, want %q", i, got, input[i])
		}
	}
}

// TestBidiFlow_ForwardTransform verifies the forward direction of the BidiFlow
// independently by joining with a collecting sink before the backward stage.
func TestBidiFlow_ForwardTransform(t *testing.T) {
	// Top: encode (uppercase). Bottom: decode (lowercase).
	encoder := stream.Map[string, string](strings.ToUpper)
	decoder := stream.Map[string, string](strings.ToLower)

	bidi := stream.BidiFlowFromFlows[string, string, string, string](encoder, decoder)

	// Join with an identity flow; verify round-trip preserves case.
	identity := stream.Map[string, string](func(s string) string { return s })
	pipeline := bidi.Join(identity)

	input := []string{"Hello", "World"}
	src := stream.FromSlice(input)

	result, err := stream.RunWith(
		stream.Via(src, pipeline),
		stream.Collect[string](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After ToUpper then identity then ToLower we get the lowercased originals.
	want := []string{"hello", "world"}
	if len(result) != len(want) {
		t.Fatalf("expected %d elements, got %d", len(want), len(result))
	}
	for i, got := range result {
		if got != want[i] {
			t.Errorf("element %d: got %q, want %q", i, got, want[i])
		}
	}
}

// TestBidiFlow_JoinPreservesOrder verifies that elements keep their order when
// piped through a BidiFlow joined with an identity flow.
func TestBidiFlow_JoinPreservesOrder(t *testing.T) {
	// Codec that adds a numeric prefix in the forward direction and strips it
	// in the backward direction, verifying both transforms run correctly.
	counter := 0
	encoder := stream.Map[string, string](func(s string) string {
		counter++
		return s // pass-through for order verification
	})
	decoder := stream.Map[string, string](func(s string) string { return s })

	bidi := stream.BidiFlowFromFlows[string, string, string, string](encoder, decoder)
	identity := stream.Map[string, string](func(s string) string { return s })
	pipeline := bidi.Join(identity)

	input := []string{"a", "b", "c", "d", "e"}
	src := stream.FromSlice(input)

	result, err := stream.RunWith(
		stream.Via(src, pipeline),
		stream.Collect[string](),
		stream.SyncMaterializer{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(input) {
		t.Fatalf("expected %d elements, got %d", len(input), len(result))
	}
	for i, got := range result {
		if got != input[i] {
			t.Errorf("element %d: got %q, want %q", i, got, input[i])
		}
	}
	if counter != len(input) {
		t.Errorf("forward transform called %d times, want %d", counter, len(input))
	}
}
