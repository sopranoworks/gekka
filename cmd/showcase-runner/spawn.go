// cmd/showcase-runner/spawn.go — per-child process lifecycle.
package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// child wires the per-process pipes to two independent consumers:
//
//   - `ready` (chan struct{}) is the dedicated READY tap, closed via `readyOnce`
//     the first time a pipeReader scans the node's SHOWCASE READY sentinel.
//     This signal is structurally decoupled from classification back-pressure:
//     the inline check inside pipeReader fires before any blocking channel send.
//
//   - `lines` (chan logLine) is the main classification fan-in. It keeps its
//     cap=256 buffer; the responsibility split (not a buffer bump) is what
//     keeps READY detection reliable under sustained ERROR floods.
//
// See docs/showcase-investigations/2026-05-18-cross-language-cbor-codec-gap.md
// "Path D: Fix the runner back-pressure bug" — Option (iii).
type child struct {
	name      string
	cmd       *exec.Cmd
	stdoutLF  io.ReadCloser
	stderrLF  io.ReadCloser
	lines     chan logLine
	ready     chan struct{}
	readyOnce sync.Once
	done      chan struct{}
}

type logLine struct {
	source string // node label, e.g. "s1" or "g1"
	stream string // "out" or "err"
	text   string
}

func spawnChild(ctx context.Context, name string, cmd *exec.Cmd, artifactDir string) (*child, error) {
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return nil, err
	}
	outFile, err := os.Create(filepath.Join(artifactDir, name+".log"))
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	ch := &child{
		name:     name,
		cmd:      cmd,
		stdoutLF: stdout,
		stderrLF: stderr,
		lines:    make(chan logLine, 256),
		ready:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	readyMarker := "--- SHOWCASE NODE READY: " + name + " ---"

	var wg sync.WaitGroup
	wg.Add(2)
	go pipeReader(stdout, "out", name, ch.lines, outFile, readyMarker, ch.ready, &ch.readyOnce, &wg)
	go pipeReader(stderr, "err", name, ch.lines, outFile, readyMarker, ch.ready, &ch.readyOnce, &wg)
	go func() { wg.Wait(); close(ch.lines); _ = outFile.Close(); close(ch.done) }()

	return ch, nil
}

func pipeReader(r io.ReadCloser, stream, name string, out chan<- logLine, dup io.Writer, readyMarker string, ready chan<- struct{}, readyOnce *sync.Once, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		_, _ = dup.Write([]byte(line + "\n"))
		if strings.Contains(line, readyMarker) {
			readyOnce.Do(func() { close(ready) })
		}
		out <- logLine{source: name, stream: stream, text: line}
	}
}

func (c *child) stop(grace time.Duration) {
	// Drain c.lines for the duration of teardown. After a pre-Gate-2 exit
	// (Setup or Gate 1 failure) nothing else consumes c.lines, so a still-live
	// child fills the cap-256 buffer and the pipeReaders block forever on their
	// `out <- logLine` send — they never reach pipe EOF, so c.done never closes.
	// The old `<-c.done` below then blocked indefinitely *even after SIGKILL*,
	// stranding child processes (observed: a Gate-1-failed run left the runner
	// and two gekka nodes alive for ~5.5h, holding their ports — which is the
	// leftover-incarnation source behind the smoke-2 g3 quarantine cycle).
	// Draining lets the readers reach EOF once the process dies so c.done closes.
	go func() {
		for range c.lines {
		}
	}()

	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
		return
	case <-time.After(grace):
	}

	// Grace expired: SIGKILL, then wait for the reader pipeline to close with a
	// hard cap so teardown can never wedge regardless of pipe/FD state.
	_ = c.cmd.Process.Kill()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
	}
}
