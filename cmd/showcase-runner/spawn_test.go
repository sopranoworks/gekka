// cmd/showcase-runner/spawn_test.go
package main

import (
	"context"
	"fmt"
	"io"
	"os/exec"
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

// TestChildStop_ReturnsWhenLinesUnconsumed is the regression for the teardown
// wedge. It reproduces the post-Gate-2-exit state: a still-live child that
// ignores SIGTERM and floods stdout, with NO consumer of c.lines. The cap-256
// channel fills, the pipeReader blocks on its send and never reaches pipe EOF,
// so c.done never closes. The old stop() blocked on `<-c.done` forever even
// after SIGKILL (observed: child processes stranded ~5.5h holding their ports).
// stop() must instead drain c.lines, SIGKILL after grace, and return promptly.
func TestChildStop_ReturnsWhenLinesUnconsumed(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	// `trap '' TERM` ignores SIGTERM so stop() is forced down the SIGKILL path;
	// the tight echo loop floods stdout to fill c.lines past its cap.
	cmd := exec.Command("sh", "-c", "trap '' TERM; while :; do echo flood-line-to-fill-the-channel-buffer; done")
	c, err := spawnChild(context.Background(), "wedge", cmd, t.TempDir())
	if err != nil {
		t.Fatalf("spawnChild: %v", err)
	}
	t.Cleanup(func() { _ = c.cmd.Wait() }) // reap the killed child

	// Let the child overflow c.lines (cap 256) so the pipeReader is blocked on
	// its send — the precise wedge condition. We deliberately do NOT drain it.
	time.Sleep(300 * time.Millisecond)

	returned := make(chan struct{})
	start := time.Now()
	go func() {
		c.stop(500 * time.Millisecond)
		close(returned)
	}()

	select {
	case <-returned:
		if elapsed := time.Since(start); elapsed > 8*time.Second {
			t.Fatalf("stop() took %v; expected ~grace+kill (<8s)", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("stop() did not return within 15s — teardown wedge regression")
	}

	// stop() draining c.lines must let the reader pipeline close (process dead).
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Error("c.done not closed after stop() returned — reader pipeline did not finish")
	}
}
