/*
 * file_io_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package stream_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/sopranoworks/gekka/stream"
)

// TestFileIO_RoundTrip writes bytes to a temp file with SinkToFile and reads
// them back with SourceFromFile, verifying content is preserved exactly.
func TestFileIO_RoundTrip(t *testing.T) {
	tmp, err := os.CreateTemp("", "gekka-fileio-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	// Write: slice of chunks → SinkToFile
	chunks := [][]byte{
		[]byte("hello "),
		[]byte("world"),
		[]byte("!"),
	}
	total := 0
	for _, c := range chunks {
		total += len(c)
	}

	fut, runErr := stream.RunWith(
		stream.FromSlice(chunks),
		stream.SinkToFile(path),
		stream.ActorMaterializer{},
	)
	if runErr != nil {
		t.Fatalf("RunWith SinkToFile: %v", runErr)
	}
	written, writeErr := fut.Get()
	if writeErr != nil {
		t.Fatalf("SinkToFile future: %v", writeErr)
	}
	if written != int64(total) {
		t.Fatalf("written %d bytes, want %d", written, total)
	}

	// Read back: SourceFromFile → Collect
	src, readFut := stream.SourceFromFile(path, 4) // small chunk size to test multi-chunk reads
	collected, readErr := stream.RunWith(src, stream.Collect[[]byte](), stream.ActorMaterializer{})
	if readErr != nil {
		t.Fatalf("RunWith SourceFromFile: %v", readErr)
	}
	bytesRead, futErr := readFut.Get()
	if futErr != nil {
		t.Fatalf("SourceFromFile future: %v", futErr)
	}
	if bytesRead != int64(total) {
		t.Fatalf("read %d bytes via future, want %d", bytesRead, total)
	}

	// Reassemble and compare.
	var got []byte
	for _, chunk := range collected {
		got = append(got, chunk...)
	}
	want := []byte("hello world!")
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestSourceFromFile_MissingFile verifies that SourceFromFile propagates an
// error when the file does not exist.
func TestSourceFromFile_MissingFile(t *testing.T) {
	src, fut := stream.SourceFromFile("/nonexistent/path/that/does/not/exist", 1024)
	_, err := stream.RunWith(src, stream.Collect[[]byte](), stream.ActorMaterializer{})
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	_, futErr := fut.Get()
	if futErr == nil {
		t.Fatal("expected future error for missing file, got nil")
	}
}

// TestSinkToFile_UnwritablePath verifies that SinkToFile propagates an error
// when the destination path cannot be created.
func TestSinkToFile_UnwritablePath(t *testing.T) {
	fut, err := stream.RunWith(
		stream.FromSlice([][]byte{[]byte("data")}),
		stream.SinkToFile("/nonexistent/dir/that/does/not/exist/out.bin"),
		stream.ActorMaterializer{},
	)
	if err == nil {
		t.Fatal("expected error for unwritable path, got nil")
	}
	_, futErr := fut.Get()
	if futErr == nil {
		t.Fatal("expected future error for unwritable path, got nil")
	}
}

// TestSourceFromFile_EmptyFile verifies that reading an empty file produces
// no chunks and completes with zero bytes.
func TestSourceFromFile_EmptyFile(t *testing.T) {
	tmp, err := os.CreateTemp("", "gekka-fileio-empty-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	src, fut := stream.SourceFromFile(path, 1024)
	result, runErr := stream.RunWith(src, stream.Collect[[]byte](), stream.ActorMaterializer{})
	if runErr != nil {
		t.Fatalf("unexpected error: %v", runErr)
	}
	if len(result) != 0 {
		t.Fatalf("expected no chunks, got %d", len(result))
	}
	bytesRead, futErr := fut.Get()
	if futErr != nil {
		t.Fatalf("future error: %v", futErr)
	}
	if bytesRead != 0 {
		t.Fatalf("expected 0 bytes, got %d", bytesRead)
	}
}
