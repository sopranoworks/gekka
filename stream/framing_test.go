/*
 * framing_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"encoding/binary"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// collect runs a graph that emits []byte frames and collects the results.
func collectFrames(t *testing.T, src stream.Source[[]byte, stream.NotUsed]) [][]byte {
	t.Helper()
	var out [][]byte
	graph := src.To(stream.Foreach(func(f []byte) {
		cp := make([]byte, len(f))
		copy(cp, f)
		out = append(out, cp)
	}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		t.Fatalf("graph error: %v", err)
	}
	return out
}

// ── DelimiterFraming ──────────────────────────────────────────────────────────

func TestFraming_Delimiter_SingleChunk(t *testing.T) {
	// "hello\nworld\n" → ["hello", "world"]
	input := [][]byte{[]byte("hello\nworld\n")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\n"), 256, false))
	frames := collectFrames(t, src)
	want := []string{"hello", "world"}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d", len(frames), len(want))
	}
	for i, w := range want {
		if string(frames[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], w)
		}
	}
}

func TestFraming_Delimiter_MultiChunk(t *testing.T) {
	// Frame spans two chunks.
	input := [][]byte{[]byte("hel"), []byte("lo\nworld\n")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\n"), 256, false))
	frames := collectFrames(t, src)
	want := []string{"hello", "world"}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d; frames=%q", len(frames), len(want), frames)
	}
	for i, w := range want {
		if string(frames[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], w)
		}
	}
}

func TestFraming_Delimiter_AllowTruncation(t *testing.T) {
	// Last "frame" has no trailing delimiter — emitted with allowTruncation.
	input := [][]byte{[]byte("hello\nworld")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\n"), 256, true))
	frames := collectFrames(t, src)
	want := []string{"hello", "world"}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d; frames=%q", len(frames), len(want), frames)
	}
	for i, w := range want {
		if string(frames[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], w)
		}
	}
}

func TestFraming_Delimiter_TruncationError(t *testing.T) {
	// Last frame has no delimiter and allowTruncation=false → error.
	input := [][]byte{[]byte("hello\nworld")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\n"), 256, false))
	var errored bool
	graph := src.To(stream.Foreach(func(_ []byte) {}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err != nil {
		errored = true
	}
	if !errored {
		t.Fatal("expected error for unterminated frame without allowTruncation")
	}
}

func TestFraming_Delimiter_ExceedsMax(t *testing.T) {
	// Frame exceeds maximumFrameLength → error.
	input := [][]byte{[]byte("hello world\n")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\n"), 5, false))
	graph := src.To(stream.Foreach(func(_ []byte) {}))
	if _, err := graph.Run(stream.SyncMaterializer{}); err == nil {
		t.Fatal("expected error when frame exceeds maximum frame length")
	}
}

func TestFraming_Delimiter_MultiByteDelimiter(t *testing.T) {
	// CRLF delimiter.
	input := [][]byte{[]byte("foo\r\nbar\r\nbaz\r\n")}
	src := stream.Via(stream.FromSlice(input), stream.Framing.DelimiterFraming([]byte("\r\n"), 256, false))
	frames := collectFrames(t, src)
	want := []string{"foo", "bar", "baz"}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d", len(frames), len(want))
	}
	for i, w := range want {
		if string(frames[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], w)
		}
	}
}

// ── LengthFieldFraming ────────────────────────────────────────────────────────

func encodeLengthPrefixed(frames [][]byte, fieldLen int, bo binary.ByteOrder) []byte {
	var buf []byte
	for _, f := range frames {
		hdr := make([]byte, fieldLen)
		switch fieldLen {
		case 1:
			hdr[0] = byte(len(f))
		case 2:
			bo.PutUint16(hdr, uint16(len(f)))
		case 4:
			bo.PutUint32(hdr, uint32(len(f)))
		}
		buf = append(buf, hdr...)
		buf = append(buf, f...)
	}
	return buf
}

func TestFraming_LengthField_BigEndian4(t *testing.T) {
	payloads := [][]byte{[]byte("hello"), []byte("world!")}
	raw := encodeLengthPrefixed(payloads, 4, binary.BigEndian)
	src := stream.Via(stream.FromSlice([][]byte{raw}), stream.Framing.LengthFieldFraming(4, 256, binary.BigEndian))
	frames := collectFrames(t, src)
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want 2", len(frames))
	}
	if string(frames[0]) != "hello" || string(frames[1]) != "world!" {
		t.Errorf("frames = %q, want [hello world!]", frames)
	}
}

func TestFraming_LengthField_LittleEndian2(t *testing.T) {
	payloads := [][]byte{[]byte("abc"), []byte("de")}
	raw := encodeLengthPrefixed(payloads, 2, binary.LittleEndian)
	// Split into small chunks to verify multi-chunk assembly.
	var chunks [][]byte
	for i := 0; i < len(raw); i += 3 {
		end := i + 3
		if end > len(raw) {
			end = len(raw)
		}
		chunks = append(chunks, raw[i:end])
	}
	src := stream.Via(stream.FromSlice(chunks), stream.Framing.LengthFieldFraming(2, 256, binary.LittleEndian))
	frames := collectFrames(t, src)
	if len(frames) != 2 {
		t.Fatalf("got %d frames, want 2", len(frames))
	}
	if string(frames[0]) != "abc" || string(frames[1]) != "de" {
		t.Errorf("frames = %q", frames)
	}
}

func TestFraming_LengthField_ExceedsMax(t *testing.T) {
	payload := []byte("hello world — too long")
	raw := encodeLengthPrefixed([][]byte{payload}, 4, binary.BigEndian)
	src := stream.Via(stream.FromSlice([][]byte{raw}), stream.Framing.LengthFieldFraming(4, 5, binary.BigEndian))
	if _, err := stream.Via(src, stream.Map[[]byte, []byte](func(b []byte) []byte { return b })).
		To(stream.Foreach(func(_ []byte) {})).
		Run(stream.SyncMaterializer{}); err == nil {
		t.Fatal("expected error when frame exceeds maximumFrameLength")
	}
}

// ── SimpleFramingProtocol ─────────────────────────────────────────────────────

func TestFraming_SimpleProtocol_RoundTrip(t *testing.T) {
	messages := [][]byte{
		[]byte("ping"),
		[]byte("pong"),
		[]byte("goodbye"),
	}

	// Encode each message with the encoder.
	var encoded [][]byte
	for _, m := range messages {
		src := stream.Via(stream.FromSlice([][]byte{m}), stream.Framing.SimpleFramingProtocolEncoder(1024))
		frames := collectFrames(t, src)
		encoded = append(encoded, frames...)
	}

	// Concatenate encoded bytes into a single stream and decode.
	var combined []byte
	for _, e := range encoded {
		combined = append(combined, e...)
	}
	src := stream.Via(stream.FromSlice([][]byte{combined}), stream.Framing.SimpleFramingProtocolDecoder(1024))
	frames := collectFrames(t, src)
	if len(frames) != len(messages) {
		t.Fatalf("decoded %d frames, want %d", len(frames), len(messages))
	}
	for i, want := range messages {
		if string(frames[i]) != string(want) {
			t.Errorf("frame[%d] = %q, want %q", i, frames[i], want)
		}
	}
}
