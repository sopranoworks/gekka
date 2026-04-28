/*
 * jvmproc_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestStopKillsGrandchild reproduces the exact leak signature this package
// exists to prevent: the immediate child forks a long-lived grandchild,
// and we must guarantee both die after Stop.  Without process-group
// isolation, naked cmd.Process.Kill() leaves the grandchild orphaned.
//
// Strategy:
//   - sh -c 'sleep 600 & echo $! ; wait'  forks `sleep`, prints its PID,
//     then waits.  The PID is the grandchild we have to kill.
//   - We capture that PID, call Stop, then prove the kernel reports
//     "no such process" for it.  syscall.Kill(pid, 0) is the standard
//     "is this PID alive?" probe.
func TestStopKillsGrandchild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := Spawn(t, ctx, "sh", []string{"-c", "sleep 600 & echo $! ; wait"}, Options{
		KillTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	scanner := bufio.NewScanner(p.Stdout)
	if !scanner.Scan() {
		t.Fatalf("did not receive grandchild pid line: %v", scanner.Err())
	}
	gPidStr := strings.TrimSpace(scanner.Text())
	gPid, err := strconv.Atoi(gPidStr)
	if err != nil {
		t.Fatalf("could not parse grandchild pid %q: %v", gPidStr, err)
	}
	t.Logf("grandchild pid = %d (parent pid = %d)", gPid, p.Pid())

	// Sanity: the grandchild should be alive right now.
	if err := syscall.Kill(gPid, 0); err != nil {
		t.Fatalf("grandchild already dead before Stop? kill(%d, 0) = %v", gPid, err)
	}

	p.Stop()
	p.Wait()

	// After Stop, the grandchild must be gone.  We give the kernel a few
	// hundred ms to reap zombies, then probe.  ESRCH = no such process.
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(gPid, 0)
		if err == syscall.ESRCH {
			return // success
		}
		if time.Now().After(deadline) {
			t.Fatalf("grandchild pid %d still alive after Stop (kill(0) returned %v)", gPid, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStopIsIdempotent locks down the Stop contract: it can be called
// multiple times safely.  Tests routinely defer p.Stop() AND register
// t.Cleanup(p.Stop) without realising — the second call must be a no-op,
// not a double-kill or panic.
func TestStopIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p, err := Spawn(t, ctx, "sh", []string{"-c", "sleep 30"}, Options{KillTimeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	p.Stop()
	p.Stop() // must not panic, must not block
	p.Wait()
}

// TestJVMFlagsLandInJavaOpts proves the JVMFlags option flows into the
// child's JAVA_OPTS env var without clobbering pre-existing values.  This
// is the contract the integration tests rely on to disable the JVM JIT
// (-Xint) while we investigate a suspected JIT-driven memory leak.
func TestJVMFlagsLandInJavaOpts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("appends to existing JAVA_OPTS", func(t *testing.T) {
		t.Setenv("JAVA_OPTS", "-Xmx512m")
		p, err := Spawn(t, ctx, "sh", []string{"-c", "echo \"$JAVA_OPTS\""}, Options{
			JVMFlags:    []string{"-Xint"},
			KillTimeout: 1 * time.Second,
		})
		if err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		scanner := bufio.NewScanner(p.Stdout)
		if !scanner.Scan() {
			t.Fatalf("no JAVA_OPTS line: %v", scanner.Err())
		}
		got := strings.TrimSpace(scanner.Text())
		want := "-Xmx512m -Xint"
		if got != want {
			t.Errorf("JAVA_OPTS = %q, want %q", got, want)
		}
	})

	t.Run("sets JAVA_OPTS when parent had none", func(t *testing.T) {
		t.Setenv("JAVA_OPTS", "")
		p, err := Spawn(t, ctx, "sh", []string{"-c", "echo \"$JAVA_OPTS\""}, Options{
			JVMFlags:    []string{"-Xint"},
			KillTimeout: 1 * time.Second,
		})
		if err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		scanner := bufio.NewScanner(p.Stdout)
		if !scanner.Scan() {
			t.Fatalf("no JAVA_OPTS line: %v", scanner.Err())
		}
		got := strings.TrimSpace(scanner.Text())
		if got != "-Xint" {
			t.Errorf("JAVA_OPTS = %q, want -Xint", got)
		}
	})
}

// TestPortRelease verifies the bind-poll loop: a child that holds a TCP
// listener on the requested port should release it by the time Stop
// returns, so the next test can bind without "address already in use".
func TestPortRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	port := pickFreePort(t)
	cmd := fmt.Sprintf(
		// nc -l holds the port for ~30s; we Stop() long before that.
		// On macOS BSD nc, -l takes the port positionally.
		"nc -l %d & echo READY ; wait", port)

	p, err := Spawn(t, ctx, "sh", []string{"-c", cmd}, Options{
		KillTimeout:        2 * time.Second,
		PortToRelease:      port,
		PortReleaseTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	scanner := bufio.NewScanner(p.Stdout)
	if !scanner.Scan() {
		t.Fatalf("did not see READY: %v", scanner.Err())
	}

	p.Stop()
	p.Wait()

	// The port should now be bindable — Stop's polling guarantees it.
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	c, err := dial(addr)
	if err == nil {
		c.Close()
		t.Fatalf("port %d still has a live listener after Stop", port)
	}
}
