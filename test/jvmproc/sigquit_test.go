/*
 * sigquit_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSignalHandler_KillsGrandchildOnSIGQUIT is the end-to-end proof
// that the orchestrator survives `go test -timeout`.  It re-execs the
// test binary with -test.run=TestHelperSpawnAndPrintGrandchildPid,
// reads the grandchild PID off the helper's stdout, sends SIGQUIT to
// the helper (the same signal `go test -timeout` uses), then verifies
// the grandchild has been reaped.
//
// Without atexit.go this test fails: SIGQUIT triggers Go's default
// handler, the helper exits before t.Cleanup runs, and the grandchild
// outlives the helper.  With atexit.go the registered signal handler
// killpg's the group before exit(2) and the grandchild is reaped in
// the same SIGQUIT cycle.
func TestSignalHandler_KillsGrandchildOnSIGQUIT(t *testing.T) {
	if os.Getenv("JVMPROC_HELPER_MODE") == "1" {
		// We are the helper — TestHelperSpawnAndPrintGrandchildPid runs
		// instead.  This branch should never be reached because Run
		// dispatched directly to the helper test name.
		t.Skip("running as helper child")
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperSpawnAndPrintGrandchildPid$", "-test.v")
	cmd.Env = append(os.Environ(), "JVMPROC_HELPER_MODE=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		// Belt-and-braces: in case the helper survives our SIGQUIT we
		// reap the entire group on test completion.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	})

	gPid, err := readGrandchildPid(stdout, 10*time.Second)
	if err != nil {
		t.Fatalf("read grandchild pid: %v", err)
	}
	if gPid <= 1 {
		t.Fatalf("invalid grandchild pid %d", gPid)
	}
	if err := syscall.Kill(gPid, 0); err != nil {
		t.Fatalf("grandchild %d not alive before SIGQUIT: %v", gPid, err)
	}

	// Send SIGQUIT — the same signal `go test -timeout` uses.  The
	// helper's atexit.go handler should kill the grandchild before
	// exiting.
	if err := cmd.Process.Signal(syscall.SIGQUIT); err != nil {
		t.Fatalf("send SIGQUIT: %v", err)
	}

	// Helper should exit promptly.  We don't care about its exit code
	// — only that it actually died.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("helper did not exit within 15s after SIGQUIT")
	}

	// Verify the grandchild was reaped.  Poll briefly because killpg
	// is asynchronous.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(gPid, 0); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return
			}
			t.Fatalf("kill -0 grandchild: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("grandchild %d still alive 5s after SIGQUIT — atexit handler did not run", gPid)
}

// TestHelperSpawnAndPrintGrandchildPid is the helper invoked by
// TestSignalHandler_KillsGrandchildOnSIGQUIT.  It runs only when
// JVMPROC_HELPER_MODE=1 is set; otherwise it is a no-op so a normal
// `go test ./...` does not block.
func TestHelperSpawnAndPrintGrandchildPid(t *testing.T) {
	if os.Getenv("JVMPROC_HELPER_MODE") != "1" {
		t.Skip("only runs as the helper child of TestSignalHandler_KillsGrandchildOnSIGQUIT")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Spawn a sleeper whose pid we can print.  `sleep 30 & echo $!; wait`
	// puts a real grandchild under our pgid leader and surfaces its pid.
	p, err := Spawn(t, ctx, "sh", []string{"-c", "sleep 60 & echo $!; wait"}, Options{
		KillTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	buf := make([]byte, 64)
	n, err := p.Stdout.Read(buf)
	if err != nil {
		t.Fatalf("read pid: %v", err)
	}
	// Print "GRANDCHILD_PID=<pid>" so the parent test can grep the
	// stream regardless of any preceding test framework chatter.
	pidLine := strings.TrimSpace(string(buf[:n]))
	os.Stdout.WriteString("GRANDCHILD_PID=" + pidLine + "\n")
	_ = os.Stdout.Sync()

	// Block forever — the parent test will SIGQUIT us.  The atexit
	// handler we are testing will reap the grandchild.
	select {}
}

// readGrandchildPid reads from r until it sees "GRANDCHILD_PID=<n>" or
// the deadline expires.
func readGrandchildPid(r interface{ Read([]byte) (int, error) }, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var acc strings.Builder
	buf := make([]byte, 4096)
	for time.Now().Before(deadline) {
		n, err := r.Read(buf)
		if n > 0 {
			acc.Write(buf[:n])
			if pid, ok := extractGrandchildPid(acc.String()); ok {
				return pid, nil
			}
		}
		if err != nil {
			return 0, err
		}
	}
	return 0, errors.New("timeout waiting for GRANDCHILD_PID line")
}

func extractGrandchildPid(s string) (int, bool) {
	for _, line := range strings.Split(s, "\n") {
		const tag = "GRANDCHILD_PID="
		if idx := strings.Index(line, tag); idx >= 0 {
			rest := strings.TrimSpace(line[idx+len(tag):])
			if pid, err := strconv.Atoi(rest); err == nil && pid > 1 {
				return pid, true
			}
		}
	}
	return 0, false
}
