//go:build integration && multijvm

/*
 * multijvm_compat_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sopranoworks/gekka/test/jvmproc"
)

// TestMultiJvmCompat runs the Akka 2.6.x multi-JVM compatibility suite
// (sbt multi-jvm:test) under jvmproc supervision.  This is the third
// stage of the CLAUDE.md test gate and is gated behind a separate
// build tag because the sbt invocation costs ~3 minutes — too long
// to pay on every fast `go test -tags integration` iteration.
//
// Tag usage:
//
//	go test -tags integration,multijvm .       # this test
//	go test -tags integration ./...            # everything else
//
// Why route multi-JVM through jvmproc instead of running sbt directly?
// Two reasons:
//   - When the test goroutine times out or is cancelled, jvmproc's
//     Setpgid + killpg(SIGKILL) tears down the entire sbt -> JVM ->
//     fork tree.  Running sbt as a plain command leaks the forked JVMs
//     when the parent test dies, which is exactly the leak that
//     created jvmproc in the first place.
//   - Single command (`go test -tags integration,multijvm ./...`)
//     covers all three CLAUDE.md gate stages, including process-group
//     cleanup if any forked JVM crashes.
func TestMultiJvmCompat(t *testing.T) {
	if _, err := exec.LookPath("sbt"); err != nil {
		t.Skip("sbt not on PATH; skipping multi-jvm compat suite")
	}
	if testing.Short() {
		t.Skip("multi-jvm:test is a long-running suite; skipped under -short")
	}

	// 8 minutes covers the observed ~3 min steady-state plus headroom
	// for cold caches / first-run sbt resolution.  Beyond this the test
	// is genuinely wedged and should fail loudly rather than hang.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	p, err := jvmproc.Spawn(t, ctx, "sbt", []string{"multi-jvm:test"}, jvmproc.Options{
		Dir:         "test/compatibility/akka-multi-node",
		KillTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("jvmproc.Spawn(sbt multi-jvm:test): %v", err)
	}

	// Drain stdout in real time so the user sees progress and the OS
	// pipe never fills up. Mirror to test log; collect a tail to surface
	// on failure (sbt's terminal "[error] Total time" line is the most
	// useful signal but we save the last several lines for context).
	const tailLines = 60
	tail := make([]string, 0, tailLines)
	scanDone := make(chan struct{})
	var observedSuccess, observedError bool
	go func() {
		defer close(scanDone)
		sc := bufio.NewScanner(p.Stdout)
		// Default 64 KiB buffer is too small for some sbt-multi-jvm
		// log lines; bump to 1 MiB to avoid Scanner bailing mid-suite.
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintf(&testLogWriter{t: t}, "[sbt] %s\n", line)
			if strings.Contains(line, "[success] Total time:") {
				observedSuccess = true
			}
			if strings.HasPrefix(line, "[error]") {
				observedError = true
			}
			if len(tail) == tailLines {
				tail = tail[1:]
			}
			tail = append(tail, line)
		}
	}()

	exitCode, werr := p.WaitForExit(ctx)
	<-scanDone // drain remaining output

	// Surface the captured tail on failure regardless of which channel
	// flagged it (non-zero exit, [error] line, or context timeout).
	dumpTail := func() {
		if len(tail) == 0 {
			return
		}
		t.Logf("--- last %d lines of sbt output ---", len(tail))
		for _, l := range tail {
			t.Logf("[sbt] %s", l)
		}
	}

	switch {
	case werr != nil:
		dumpTail()
		t.Fatalf("multi-jvm:test wait failed: %v (exitCode=%d)", werr, exitCode)
	case exitCode != 0:
		dumpTail()
		t.Fatalf("multi-jvm:test exited %d; observedSuccess=%v observedError=%v",
			exitCode, observedSuccess, observedError)
	case observedError:
		dumpTail()
		t.Fatalf("multi-jvm:test exited 0 but stdout contained [error] lines; "+
			"observedSuccess=%v", observedSuccess)
	case !observedSuccess:
		dumpTail()
		t.Fatalf("multi-jvm:test exited 0 but no [success] line was seen; "+
			"sbt may have skipped the suite")
	}
}

// testLogWriter adapts an *testing.T into an io.Writer so we can use
// fmt.Fprintf to stream sbt output through the test logger.  Each line
// becomes a separate t.Log call so the test framework can interleave
// it with timestamps.  We deliberately don't use t.Helper() here —
// the line-level granularity is the point.
type testLogWriter struct{ t *testing.T }

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
