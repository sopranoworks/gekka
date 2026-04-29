/*
 * atexit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"context"
	"syscall"
	"testing"
	"time"
)

// TestKillAllRegistered_ReapsGrandchild simulates what the SIGQUIT handler
// does on `go test -timeout`: drain the registry and killpg every group.
// The test arms Spawn (which registers), then calls killAllRegistered
// directly without going through Stop.  The grandchild must die.
//
// This is the unit-level proof that the registry plumbing works; the
// signal handler is a thin shim that calls this same function.
func TestKillAllRegistered_ReapsGrandchild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// `sh -c 'sleep 60 & echo $!; wait'` mirrors sbt's pattern of
	// forking a long-lived child whose pgid leadership gets inherited.
	// The PID printed is the grandchild's; we'll verify it dies.
	p, err := Spawn(t, ctx, "sh", []string{"-c", "sleep 60 & echo $!; wait"}, Options{
		KillTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Read the grandchild's PID from stdout.
	buf := make([]byte, 32)
	n, err := p.Stdout.Read(buf)
	if err != nil {
		t.Fatalf("read grandchild pid: %v", err)
	}
	gPidStr := string(buf[:n])
	var gPid int
	if _, err := fmtSscan(gPidStr, &gPid); err != nil {
		t.Fatalf("parse grandchild pid %q: %v", gPidStr, err)
	}

	// Confirm the grandchild is alive before we drain the registry.
	if err := syscall.Kill(gPid, 0); err != nil {
		t.Fatalf("grandchild %d should be alive before cleanup: %v", gPid, err)
	}

	// Bypass Stop entirely — exercise the registry-driven path the
	// signal handler uses.
	killAllRegistered()

	// Poll until the grandchild is gone (kill -0 returns ESRCH).  A
	// short loop avoids a hard sleep that would slow the suite.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(gPid, 0); err != nil {
			return // ESRCH — the kill landed
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("grandchild %d still alive 3s after killAllRegistered()", gPid)
}

// TestUnregister_RemovesFromRegistry verifies Stop drops the *Process
// from the registry so the signal handler can't double-kill an already-
// reaped group.
func TestUnregister_RemovesFromRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p, err := Spawn(t, ctx, "sh", []string{"-c", "sleep 30"}, Options{
		KillTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Spawn populates the registry.
	registryMu.Lock()
	_, present := registry[p]
	registryMu.Unlock()
	if !present {
		t.Fatal("Spawn did not add *Process to the registry")
	}

	// Stop drops the entry.
	p.Stop()
	registryMu.Lock()
	_, present = registry[p]
	registryMu.Unlock()
	if present {
		t.Fatal("Stop did not remove *Process from the registry")
	}
}

// fmtSscan is a thin wrapper to avoid pulling in the heavy fmt package
// just for parsing one int — keeps test deps minimal.
func fmtSscan(s string, out *int) (int, error) {
	v := 0
	count := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			v = v*10 + int(r-'0')
			count++
		} else if count > 0 {
			break
		}
	}
	if count == 0 {
		return 0, errEmptyPid
	}
	*out = v
	return count, nil
}

type errPid string

func (e errPid) Error() string { return string(e) }

const errEmptyPid = errPid("no digits found")
