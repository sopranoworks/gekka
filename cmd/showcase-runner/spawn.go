// cmd/showcase-runner/spawn.go — per-child process lifecycle.
package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type child struct {
	name     string
	cmd      *exec.Cmd
	stdoutLF io.ReadCloser
	stderrLF io.ReadCloser
	lines    chan logLine
	done     chan struct{}
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

	ch := &child{name: name, cmd: cmd, stdoutLF: stdout, stderrLF: stderr,
		lines: make(chan logLine, 256), done: make(chan struct{})}

	var wg sync.WaitGroup
	wg.Add(2)
	go pipeReader(stdout, "out", name, ch.lines, outFile, &wg)
	go pipeReader(stderr, "err", name, ch.lines, outFile, &wg)
	go func() { wg.Wait(); close(ch.lines); _ = outFile.Close(); close(ch.done) }()

	return ch, nil
}

func pipeReader(r io.ReadCloser, stream, name string, out chan<- logLine, dup io.Writer, wg *sync.WaitGroup) {
	defer wg.Done()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		_, _ = dup.Write([]byte(line + "\n"))
		out <- logLine{source: name, stream: stream, text: line}
	}
}

func (c *child) stop(grace time.Duration) {
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(grace):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
}
