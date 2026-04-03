/*
 * compression_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"bytes"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// collectBytes runs source → Foreach and concatenates all emitted slices.
func collectBytes(t *testing.T, src stream.Source[[]byte, stream.NotUsed]) []byte {
	t.Helper()
	var out []byte
	graph := src.To(stream.Foreach(func(b []byte) {
		out = append(out, b...)
	}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	return out
}

// ── Gzip round-trip ───────────────────────────────────────────────────────────

func TestCompression_Gzip_RoundTrip(t *testing.T) {
	original := []byte("hello, gzip compression world!")

	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{original}),
			stream.Compression.Gzip(stream.DefaultCompressionLevel)))

	if len(compressed) == 0 {
		t.Fatal("compressed output is empty")
	}

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Gunzip(0)))

	if !bytes.Equal(decompressed, original) {
		t.Errorf("round-trip mismatch: got %q, want %q", decompressed, original)
	}
}

func TestCompression_Gzip_MultiChunk(t *testing.T) {
	chunks := [][]byte{
		[]byte("chunk one "),
		[]byte("chunk two "),
		[]byte("chunk three"),
	}
	expected := []byte("chunk one chunk two chunk three")

	// Compress each chunk individually — each produces valid gzip output.
	// We collect all compressed bytes, then gunzip the full stream.
	var compressedAll []byte
	for _, c := range chunks {
		comp := collectBytes(t,
			stream.Via(stream.FromSlice([][]byte{c}),
				stream.Compression.Gzip(stream.DefaultCompressionLevel)))
		compressedAll = append(compressedAll, comp...)
	}

	// For multi-stream gunzip we test the single-stream variant here.
	// Re-compress everything as a single stream.
	compressed := collectBytes(t,
		stream.Via(stream.FromSlice(chunks),
			stream.Compression.Gzip(stream.DefaultCompressionLevel)))

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Gunzip(64*1024)))

	if !bytes.Equal(decompressed, expected) {
		t.Errorf("multi-chunk round-trip: got %q, want %q", decompressed, expected)
	}
}

func TestCompression_Gzip_EmptyInput(t *testing.T) {
	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{}),
			stream.Compression.Gzip(stream.DefaultCompressionLevel)))

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Gunzip(0)))

	if len(decompressed) != 0 {
		t.Errorf("expected empty decompressed output, got %d bytes", len(decompressed))
	}
}

func TestCompression_Gzip_BestSpeed(t *testing.T) {
	original := bytes.Repeat([]byte("abcdef"), 100)

	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{original}),
			stream.Compression.Gzip(1))) // BestSpeed

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Gunzip(0)))

	if !bytes.Equal(decompressed, original) {
		t.Error("BestSpeed round-trip failed")
	}
}

// ── Deflate round-trip ────────────────────────────────────────────────────────

func TestCompression_Deflate_RoundTrip(t *testing.T) {
	original := []byte("hello, deflate compression world!")

	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{original}),
			stream.Compression.Deflate(stream.DefaultCompressionLevel)))

	if len(compressed) == 0 {
		t.Fatal("deflate compressed output is empty")
	}

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Inflate(0)))

	if !bytes.Equal(decompressed, original) {
		t.Errorf("deflate round-trip mismatch: got %q, want %q", decompressed, original)
	}
}

func TestCompression_Deflate_MultiChunk(t *testing.T) {
	chunks := [][]byte{
		[]byte("alpha "),
		[]byte("beta "),
		[]byte("gamma"),
	}
	expected := []byte("alpha beta gamma")

	compressed := collectBytes(t,
		stream.Via(stream.FromSlice(chunks),
			stream.Compression.Deflate(stream.DefaultCompressionLevel)))

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Inflate(64*1024)))

	if !bytes.Equal(decompressed, expected) {
		t.Errorf("deflate multi-chunk: got %q, want %q", decompressed, expected)
	}
}

func TestCompression_Deflate_EmptyInput(t *testing.T) {
	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{}),
			stream.Compression.Deflate(stream.DefaultCompressionLevel)))

	decompressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Inflate(0)))

	if len(decompressed) != 0 {
		t.Errorf("expected empty output after deflate round-trip on empty input, got %d bytes", len(decompressed))
	}
}

// ── Gunzip chunk size ─────────────────────────────────────────────────────────

func TestCompression_Gunzip_SmallChunkSize(t *testing.T) {
	original := bytes.Repeat([]byte("x"), 1000)

	compressed := collectBytes(t,
		stream.Via(stream.FromSlice([][]byte{original}),
			stream.Compression.Gzip(stream.DefaultCompressionLevel)))

	// Use a very small chunk size — should still reconstruct the full payload.
	var chunks [][]byte
	graph := stream.Via(stream.FromSlice([][]byte{compressed}),
		stream.Compression.Gunzip(10)).
		To(stream.Foreach(func(b []byte) {
			cp := make([]byte, len(b))
			copy(cp, b)
			chunks = append(chunks, cp)
		}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}

	var full []byte
	for _, c := range chunks {
		full = append(full, c...)
	}
	if !bytes.Equal(full, original) {
		t.Errorf("small-chunk gunzip: got %d bytes, want %d", len(full), len(original))
	}
}

// ── Compose with Framing ──────────────────────────────────────────────────────

func TestCompression_Gzip_ComposeWithFraming(t *testing.T) {
	lines := [][]byte{
		[]byte("line one\n"),
		[]byte("line two\n"),
		[]byte("line three\n"),
	}

	// Compress the newline-delimited stream.
	compressed := collectBytes(t,
		stream.Via(stream.FromSlice(lines),
			stream.Compression.Gzip(stream.DefaultCompressionLevel)))

	// Decompress, then split on newline.
	var frames [][]byte
	graph := stream.Via(
		stream.Via(stream.FromSlice([][]byte{compressed}),
			stream.Compression.Gunzip(0)),
		stream.Framing.DelimiterFraming([]byte("\n"), 256, false),
	).To(stream.Foreach(func(b []byte) {
		cp := make([]byte, len(b))
		copy(cp, b)
		frames = append(frames, cp)
	}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}

	want := []string{"line one", "line two", "line three"}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d", len(frames), len(want))
	}
	for i, w := range want {
		if string(frames[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], w)
		}
	}
}
