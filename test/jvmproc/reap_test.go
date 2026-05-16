/*
 * reap_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMatchesStaleMarker verifies the substring check covers every known
// assembly JAR pattern and rejects unrelated argv strings.  Guards against
// regressions when a new assembly is added to staleJVMMarkers but the
// caller forgets to update the test.
func TestMatchesStaleMarker(t *testing.T) {
	cases := []struct {
		name string
		cmd  string
		want bool
	}{
		{
			name: "pekko assembly jar",
			cmd:  "java -cp /path/to/scala-server/target/scala-2.13/pekko-mains-assembly.jar com.example.MultiNodeCluster",
			want: true,
		},
		{
			name: "akka assembly jar",
			cmd:  "java -cp /path/to/scala-server/akka-server/target/scala-2.13/akka-mains-assembly.jar com.example.AkkaIntegrationNode",
			want: true,
		},
		{
			name: "akka 2614 assembly jar",
			cmd:  "java -cp /path/to/scala-server/akka-2614-server/target/scala-2.13/akka-2614-mains-assembly.jar com.example.AkkaStrictHoconJoinNode",
			want: true,
		},
		{
			name: "unrelated java process",
			cmd:  "java -jar /usr/local/Cellar/something/else.jar",
			want: false,
		},
		{
			name: "sbt launcher script with JAR mention does not count",
			cmd:  "/bin/sh -c echo pekko-mains-assembly.jar",
			want: true, // matchesStaleMarker alone returns true; the "java" prefix gate is in findStaleTestJVMs
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesStaleMarker(tc.cmd); got != tc.want {
				t.Errorf("matchesStaleMarker(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}
}

// TestReapStaleTestJVMs_KillsMatchingLeaker spawns a deliberate "fake leaker"
// — a tiny child process whose argv carries one of the stale markers AND
// holds a test port — and asserts that ReapStaleTestJVMs finds it, kills
// it, and frees the port.  Without this test we cannot trust the reaper
// to actually do its job in CI.
func TestReapStaleTestJVMs_KillsMatchingLeaker(t *testing.T) {
	// Avoid running concurrently with other tests in this package — the
	// reaper inspects every `java` process on the host.  The integration
	// suite's TestMain only runs once, but unit tests can run in parallel
	// (we don't, but be defensive).
	port := pickFreeTCPPortForTest(t)

	leaker := startFakeLeaker(t, "pekko-mains-assembly.jar", port)
	// Note: jvmproc.Process is not used here — startFakeLeaker manages its
	// own *exec.Cmd because we deliberately bypass the Setpgid/register
	// path to simulate a JVM that escaped the jvmproc registry (the
	// scenario the reaper exists to recover from).

	// Sanity: the leaker actually bound the port.
	if tcpPortBindable(port) {
		t.Fatalf("leaker did not bind port %d", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Restrict the polled-port set to the leaker's port so this test
	// doesn't depend on the well-known test ports being free on a
	// developer's machine.
	prev := testPortsToReleaseOnStartup
	testPortsToReleaseOnStartup = []int{port}
	defer func() { testPortsToReleaseOnStartup = prev }()

	killed, err := ReapStaleTestJVMs(ctx)
	if err != nil {
		t.Fatalf("ReapStaleTestJVMs: %v", err)
	}
	if killed < 1 {
		t.Errorf("expected at least 1 stale JVM killed, got %d", killed)
	}
	if !tcpPortBindable(port) {
		t.Errorf("port %d still held after reap", port)
	}

	// Reaper-side wait is enough; just confirm the process exited so the
	// goroutine inside startFakeLeaker doesn't outlive the test.
	select {
	case <-leaker.exited:
	case <-time.After(5 * time.Second):
		t.Errorf("fake leaker did not exit within 5 s of reap")
	}
}

// TestReapStaleTestJVMs_LeavesUnrelatedJavaAlone spawns a fake "java"
// process whose argv does NOT contain a stale marker and asserts the
// reaper does NOT touch it.  Guards against the reaper turning into
// a kill-everything-that-says-java footgun.
func TestReapStaleTestJVMs_LeavesUnrelatedJavaAlone(t *testing.T) {
	port := pickFreeTCPPortForTest(t)
	innocent := startFakeLeaker(t, "totally-unrelated-jar.jar", port)
	defer innocent.kill()

	if tcpPortBindable(port) {
		t.Fatalf("innocent process did not bind port %d", port)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prev := testPortsToReleaseOnStartup
	testPortsToReleaseOnStartup = nil // skip port wait — we WANT the port held
	defer func() { testPortsToReleaseOnStartup = prev }()

	killed, err := ReapStaleTestJVMs(ctx)
	if err != nil {
		t.Fatalf("ReapStaleTestJVMs: %v", err)
	}
	if killed != 0 {
		t.Errorf("reaper killed %d non-matching processes; want 0", killed)
	}
	if tcpPortBindable(port) {
		t.Errorf("innocent process was killed (port %d freed)", port)
	}
}

// ── Test helpers ─────────────────────────────────────────────────────────

type fakeLeaker struct {
	cmd    *exec.Cmd
	exited chan struct{}
}

func (f *fakeLeaker) kill() {
	if f.cmd != nil && f.cmd.Process != nil {
		_ = f.cmd.Process.Kill()
	}
}

// startFakeLeaker compiles and launches a tiny helper that pretends to be
// a leaked test JVM: its argv contains markerSubstr and it holds a TCP
// port until killed.  The helper is implemented in this same package
// (via the TestHelperFakeLeakerMain test entry point) so we don't need
// a separate package build.
//
// markerSubstr is appended as a dummy argv arg.  The reaper requires the
// process name to start with `java`, which we satisfy by symlinking the
// test binary to a temp file named `java` and exec'ing through that.
func startFakeLeaker(t *testing.T, markerSubstr string, port int) *fakeLeaker {
	t.Helper()
	// Symlink this test binary to a path whose basename is `java` so the
	// reaper's `strings.HasPrefix(cmd, "java ") || strings.Contains(cmd,
	// "/java ")` filter accepts it.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	dir := t.TempDir()
	javaShim := filepath.Join(dir, "java")
	if err := os.Symlink(exe, javaShim); err != nil {
		t.Fatalf("symlink java shim: %v", err)
	}

	cmd := exec.Command(javaShim,
		"-test.run=TestHelperFakeLeakerMain",
		"-test.timeout=0",
		"--",
		"-cp", "/dev/null", markerSubstr, "FakeLeakerMain",
		"--port", strconv.Itoa(port),
	)
	cmd.Env = append(os.Environ(), "GEKKA_FAKE_LEAKER_PORT="+strconv.Itoa(port))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start fake leaker: %v", err)
	}

	leaker := &fakeLeaker{cmd: cmd, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(leaker.exited)
	}()

	// Wait for the leaker to actually bind its port before returning, so
	// the caller's port-bindable check below is deterministic.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !tcpPortBindable(port) {
			t.Cleanup(leaker.kill)
			return leaker
		}
		time.Sleep(25 * time.Millisecond)
	}
	leaker.kill()
	t.Fatalf("fake leaker did not bind port %d within 5 s", port)
	return nil
}

// TestHelperFakeLeakerMain is the entry point for the fake leaker
// subprocess (see startFakeLeaker).  Activated only when the env var
// GEKKA_FAKE_LEAKER_PORT is set; otherwise it's a normal no-op test.
func TestHelperFakeLeakerMain(t *testing.T) {
	portStr := os.Getenv("GEKKA_FAKE_LEAKER_PORT")
	if portStr == "" {
		t.Skip("not the fake-leaker helper invocation")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("bad port %q: %v", portStr, err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("bind %s: %v", addr, err)
	}
	defer l.Close()
	// Block until SIGKILL.  Accept-then-discard so the kernel doesn't
	// drop the listening socket if the kernel's RST window is hit.
	for {
		conn, err := l.Accept()
		if err != nil {
			if isClosedListenerErr(err) {
				return
			}
			continue
		}
		_ = conn.Close()
	}
}

func isClosedListenerErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

// pickFreeTCPPortForTest grabs a random free port by binding to :0,
// closing, and returning the port.  Race-prone in theory but adequate
// in practice for these tests' lifetimes.
func pickFreeTCPPortForTest(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pickFreeTCPPortForTest: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}
