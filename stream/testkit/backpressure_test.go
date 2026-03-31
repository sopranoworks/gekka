/*
 * backpressure_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package testkit_test

import (
	"testing"

	"github.com/sopranoworks/gekka/stream"
	"github.com/sopranoworks/gekka/stream/testkit"
	"github.com/stretchr/testify/require"
)

// TestBackpressurePropagation verifies that a Map stage correctly forwards
// demand from TestSinkProbe all the way back to TestSourceProbe: no element
// is pulled from the source until the sink explicitly requests it.
func TestBackpressurePropagation(t *testing.T) {
	srcProbe := testkit.NewTestSourceProbe[int]()
	sinkProbe := testkit.NewTestSinkProbe[int]()

	// Wire: sourceProbe → Map(×2) → sinkProbe
	graph := stream.Via(
		srcProbe.AsSource(),
		stream.Map(func(x int) int { return x * 2 }),
	).To(sinkProbe.AsSink())

	// Run graph in a background goroutine; probes synchronise via channels.
	go graph.Run(stream.SyncMaterializer{}) //nolint:errcheck

	// ── First element ──────────────────────────────────────────────────────
	// Sink requests one element; demand must propagate through Map to source.
	sinkProbe.Request(1)
	n := srcProbe.ExpectRequest()
	require.Equal(t, int64(1), n, "first demand: cumulative requested count should be 1")

	// Push 21; Map doubles it to 42; sink receives 42.
	srcProbe.SendNext(21)
	sinkProbe.ExpectNext(t, 42)

	// ── Second element ─────────────────────────────────────────────────────
	// Source must stay idle until the sink requests again.
	sinkProbe.Request(1)
	n = srcProbe.ExpectRequest()
	require.Equal(t, int64(2), n, "second demand: cumulative requested count should be 2")

	srcProbe.SendNext(10)
	sinkProbe.ExpectNext(t, 20)

	// ── End-of-stream ──────────────────────────────────────────────────────
	// Request one more pull so the source can deliver the completion signal.
	sinkProbe.Request(1)
	srcProbe.ExpectRequest()
	srcProbe.SendComplete()
	sinkProbe.ExpectComplete(t)
}

// TestBackpressure_MultipleRequests verifies that Request(n) allows exactly
// n elements to pass through before the sink blocks again.
func TestBackpressure_MultipleRequests(t *testing.T) {
	srcProbe := testkit.NewTestSourceProbe[string]()
	sinkProbe := testkit.NewTestSinkProbe[string]()

	graph := stream.Via(
		srcProbe.AsSource(),
		stream.Map(func(s string) string { return s + "!" }),
	).To(sinkProbe.AsSink())

	go graph.Run(stream.SyncMaterializer{}) //nolint:errcheck

	// Request three elements at once.
	sinkProbe.Request(3)

	for _, word := range []string{"hello", "world", "go"} {
		srcProbe.ExpectRequest()
		srcProbe.SendNext(word)
		sinkProbe.ExpectNext(t, word+"!")
	}

	// Finish cleanly.
	sinkProbe.Request(1)
	srcProbe.ExpectRequest()
	srcProbe.SendComplete()
	sinkProbe.ExpectComplete(t)
}
