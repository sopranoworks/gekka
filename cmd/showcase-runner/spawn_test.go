// cmd/showcase-runner/spawn_test.go
package main

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// TestPipeReader_ReadyDetectedUnderErrorFlood proves that the separate READY tap
// — closing a dedicated `ready` channel via sync.Once from inside the pipe reader —
// signals READY at the moment the producer scans the sentinel line, even when the
// child process is flooding ERROR lines into the same stream. Option (iii) from the
// 2026-05-18 cross-language CBOR codec gap investigation: responsibility split,
// not buffer-size bump.
func TestPipeReader_ReadyDetectedUnderErrorFlood(t *testing.T) {
	pr, pw := io.Pipe()

	out := make(chan logLine, 256)
	ready := make(chan struct{})
	var once sync.Once

	// Drain `out` in the background. This represents the main classification
	// fan-in. It runs in parallel with the READY tap; it must not be ON the
	// READY-detection path, but it must exist so the producer can keep reading.
	go func() {
		for range out {
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go pipeReader(pr, "out", "g1", out, io.Discard, "--- SHOWCASE NODE READY: g1 ---", ready, &once, &wg)

	// Flood 1000 ERROR lines, then the READY sentinel.
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := 0; i < 1000; i++ {
			fmt.Fprintf(pw, "[12:00:00.000] ERROR org.example - flood %d\n", i)
		}
		fmt.Fprintln(pw, "--- SHOWCASE NODE READY: g1 ---")
		_ = pw.Close()
	}()

	select {
	case <-ready:
		// pass
	case <-time.After(5 * time.Second):
		t.Fatal("ready chan not closed within 5s under 1000-line ERROR flood")
	}

	<-writeDone
	wg.Wait()
	close(out)
}

// TestPipeReader_ReadyOnceSemantics ensures the sync.Once guard tolerates
// multiple READY sentinels in the stream (e.g. supervised restart) without
// panicking on a double-close.
func TestPipeReader_ReadyOnceSemantics(t *testing.T) {
	pr, pw := io.Pipe()

	out := make(chan logLine, 16)
	ready := make(chan struct{})
	var once sync.Once

	go func() {
		for range out {
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go pipeReader(pr, "out", "g1", out, io.Discard, "--- SHOWCASE NODE READY: g1 ---", ready, &once, &wg)

	go func() {
		fmt.Fprintln(pw, "--- SHOWCASE NODE READY: g1 ---")
		fmt.Fprintln(pw, "--- SHOWCASE NODE READY: g1 ---")
		_ = pw.Close()
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("ready chan not closed")
	}

	wg.Wait()
	close(out)
}
