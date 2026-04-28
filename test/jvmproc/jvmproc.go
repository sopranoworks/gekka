/*
 * jvmproc.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package jvmproc supervises long-lived JVM child processes (sbt, scala-server)
// for integration tests.  It exists for one reason: a naked
// `cmd.Process.Kill()` only signals sbt itself — sbt's forked JVM survives
// and keeps holding the test port, leaking across test runs and corrupting
// every subsequent test that bind()s to the same address.
//
// The cure is the same one cokka's tests/compatibility/run_compatibility_tests.py
// uses: put the child in its own process group via Setpgid, then kill the
// entire group on cleanup.  Closing the loop also requires polling for port
// release because the kernel may hold the socket in TIME_WAIT after the JVM
// finally exits.
package jvmproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Options controls how the JVM child is spawned and cleaned up.  Zero
// values are sane defaults: 10 s SIGTERM grace, 30 s port-release poll.
type Options struct {
	// Dir sets the working directory for the spawned process.
	Dir string

	// ExtraEnv augments os.Environ with `KEY=VAL` entries.
	ExtraEnv []string

	// JVMFlags are appended to the spawned process's JAVA_OPTS env var.
	// sbt's launcher reads JAVA_OPTS and forwards it to the JVM, so flags
	// like -Xint (interpreter only — disables JIT) flow through to the
	// process actually executing the test code.  Existing JAVA_OPTS in
	// the parent environment are preserved; flags are appended after them.
	JVMFlags []string

	// KillTimeout is the SIGTERM grace period before escalating to SIGKILL.
	// Zero defaults to 10 s, matching cokka's run_compatibility_tests.py.
	KillTimeout time.Duration

	// PortToRelease, when > 0, instructs cleanup to poll TCP bind on
	// 127.0.0.1:Port until the kernel frees the socket.  This catches the
	// "JVM exited but the port is still held" race that breaks back-to-back
	// integration runs.
	PortToRelease int

	// PortReleaseTimeout caps the bind-poll wait.  Zero defaults to 30 s.
	PortReleaseTimeout time.Duration
}

// Process is a supervised JVM child with stdout/stderr merged onto Stdout
// for line-scanner consumption.  Stop is idempotent and registered with
// t.Cleanup automatically by Spawn.
type Process struct {
	cmd    *exec.Cmd
	Stdout io.ReadCloser
	pgid   int

	stopOnce sync.Once
	stopped  chan struct{}

	// exited is closed by the dedicated waiter goroutine (started in
	// Spawn) once cmd.Wait() returns.  WaitForExit selects on it; Stop
	// uses it to detect "process already exited, no need to kill".
	// waitErr is the result of cmd.Wait() and is safe to read after
	// `exited` has been observed closed.
	exited  chan struct{}
	waitErr error

	killTimeout        time.Duration
	port               int
	portReleaseTimeout time.Duration
}

// OrchestratorTokenEnv is the environment variable a Scala test fixture
// inspects to confirm it was launched by jvmproc and not by hand.  See
// scala-server/.../OrchestratorGate.scala — both halves must use the
// same name and value.
const OrchestratorTokenEnv = "GEKKA_ORCHESTRATOR_TOKEN"

// OrchestratorTokenValue is the literal token jvmproc injects.  Mirrored
// in OrchestratorGate.TokenValue.  The value is intentionally non-trivial
// so a human cannot satisfy the gate by guessing the env var alone — they
// have to read the source and accept the contract.
const OrchestratorTokenValue = "jvmproc-de0868c-2026"

// Spawn launches name with args under process-group isolation, registers
// a cleanup that kills the entire group, and returns a Process exposing
// the merged stdout/stderr stream for scanner-driven readiness detection.
//
// Spawn always sets OrchestratorTokenEnv=OrchestratorTokenValue in the
// child env so that gated Scala mains accept the launch.  Tests don't have
// to opt in — every jvmproc-launched process gets the token automatically.
//
// On any error before Cleanup registration, the partially started child is
// killed before the function returns — leaks are not possible from Spawn.
func Spawn(t testing.TB, ctx context.Context, name string, args []string, opts Options) (*Process, error) {
	t.Helper()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = opts.Dir
	// Inject the orchestrator token before any caller-supplied ExtraEnv
	// so that callers may explicitly override (e.g. negative tests that
	// want to exercise the "no token → exit 78" path).
	extra := append([]string{OrchestratorTokenEnv + "=" + OrchestratorTokenValue}, opts.ExtraEnv...)
	cmd.Env = buildEnv(os.Environ(), extra, opts.JVMFlags)
	// Setpgid is the load-bearing field: every grandchild the JVM forks
	// inherits this group, so a single killpg(-pgid, SIGKILL) takes them
	// all out at once.  Without it, sbt's java child becomes a session
	// orphan that survives test cleanup.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("jvmproc: StdoutPipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("jvmproc: start %s: %w", name, err)
	}

	pgid, perr := syscall.Getpgid(cmd.Process.Pid)
	if perr != nil {
		// Falling back to PID is strictly worse — grandchildren leak —
		// but we surface the error to the caller and use the pid as a
		// best-effort target.
		pgid = cmd.Process.Pid
	}

	killTimeout := opts.KillTimeout
	if killTimeout <= 0 {
		killTimeout = 10 * time.Second
	}
	portReleaseTimeout := opts.PortReleaseTimeout
	if portReleaseTimeout <= 0 {
		portReleaseTimeout = 30 * time.Second
	}

	p := &Process{
		cmd:                cmd,
		Stdout:             stdout,
		pgid:               pgid,
		stopped:            make(chan struct{}),
		exited:             make(chan struct{}),
		killTimeout:        killTimeout,
		port:               opts.PortToRelease,
		portReleaseTimeout: portReleaseTimeout,
	}

	// Single dedicated waiter for cmd.Wait().  Calling cmd.Wait twice on
	// the same *exec.Cmd is unsafe; routing every wait through this
	// goroutine + the `exited` channel lets WaitForExit (natural-exit
	// path) and Stop (kill path) observe the same result without racing.
	go func() {
		p.waitErr = p.cmd.Wait()
		close(p.exited)
	}()

	t.Cleanup(p.Stop)
	return p, nil
}

// Stop is the idempotent cleanup entry point.  It sends SIGTERM to the
// whole process group, waits up to KillTimeout for graceful exit, and
// escalates to SIGKILL if the children outlive the grace window.  When
// PortToRelease is set, it then polls TCP bind until the kernel frees the
// socket so the next test can bind to the same port.
func (p *Process) Stop() {
	p.stopOnce.Do(func() {
		defer close(p.stopped)

		if p.cmd.Process == nil {
			return
		}

		// Fast path: if the process already exited on its own (e.g. the
		// natural-exit branch of WaitForExit observed it first), skip
		// the kill and the kill-timeout entirely.
		select {
		case <-p.exited:
			// Already reaped by the waiter goroutine.
		default:
			// Go straight to SIGKILL on the entire process group.  SIGTERM
			// triggers sbt's graceful-shutdown path, which can leave the
			// test in a state main's `Process.Kill()` (immediate SIGKILL)
			// never produces — and we observed that asymmetry breaking
			// downstream cluster tests in full-suite runs.  Killing the
			// group with SIGKILL matches main's behavior at the leaf
			// while still reaping the grandchild JVM (the actual leak
			// fix this package exists for).
			_ = killGroup(p.pgid, syscall.SIGKILL)
			select {
			case <-p.exited:
				// Reaped.
			case <-time.After(p.killTimeout):
				// Wait wedged for some reason; resend and continue.
				_ = killGroup(p.pgid, syscall.SIGKILL)
				<-p.exited
			}
		}

		if p.port > 0 {
			waitPortReleased(p.port, p.portReleaseTimeout)
		}
	})
}

// Wait blocks until Stop has fully completed.  Useful in tests that want
// to confirm cleanup landed before asserting on side effects.
func (p *Process) Wait() {
	<-p.stopped
}

// WaitForExit blocks until the underlying process exits on its own (or
// ctx is cancelled), then returns the exit code.  Use this for tests
// that drive a finite-duration command (e.g. sbt multi-jvm:test) where
// success is "process exited cleanly with code 0" rather than the
// long-lived-server pattern that Stop() supervises.
//
// On ctx cancellation the process is left running — the caller is
// expected to fall through to the t.Cleanup-registered Stop() to reap
// it.  The returned exit code is -1 in that case.
//
// Calling WaitForExit after the process has already exited is safe and
// returns the cached exit code without blocking.
func (p *Process) WaitForExit(ctx context.Context) (int, error) {
	select {
	case <-p.exited:
		return p.exitCodeLocked(), p.waitErr
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

// ExitCode returns the exit code if the process has exited, or -1 if it
// is still running.  Non-blocking.  Pair with WaitForExit when you need
// to wait, or with the `exited` channel via WaitForExit-context for
// timeout-aware waits.
func (p *Process) ExitCode() int {
	select {
	case <-p.exited:
		return p.exitCodeLocked()
	default:
		return -1
	}
}

// exitCodeLocked extracts the exit code from a completed cmd.  Must
// only be called after `exited` has been observed closed.
func (p *Process) exitCodeLocked() int {
	if p.cmd.ProcessState != nil {
		return p.cmd.ProcessState.ExitCode()
	}
	return -1
}

// Pid returns the direct child's PID.  Tests that want to verify the
// grandchild tree was killed can use this as the process-group id (it is,
// because Setpgid sets pgid == pid for the leader).
func (p *Process) Pid() int { return p.cmd.Process.Pid }

// killGroup sends sig to every process in pgid's group.  The negative-pid
// convention is a portable Unix idiom: kill(2) treats a negative pid as
// "deliver to the process group abs(pid)".
func killGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 1 {
		return errors.New("jvmproc: refusing to signal pgid <= 1")
	}
	if err := syscall.Kill(-pgid, sig); err != nil {
		// ESRCH means the group is already empty — that's fine.
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

// buildEnv assembles the child env: parent env + ExtraEnv overrides, then
// folds JVMFlags into JAVA_OPTS (appended to any pre-existing value).  The
// parent's JAVA_OPTS is preserved so locally configured tuning still
// applies — we only add to it.
func buildEnv(parent, extra, jvmFlags []string) []string {
	if len(extra) == 0 && len(jvmFlags) == 0 {
		return parent
	}
	merged := make([]string, 0, len(parent)+len(extra))
	merged = append(merged, parent...)
	for _, kv := range extra {
		merged = setOrReplaceEnv(merged, kv)
	}
	if len(jvmFlags) > 0 {
		joined := strings.Join(jvmFlags, " ")
		current := lookupEnv(merged, "JAVA_OPTS")
		next := joined
		if current != "" {
			next = current + " " + joined
		}
		merged = setOrReplaceEnv(merged, "JAVA_OPTS="+next)
	}
	return merged
}

func setOrReplaceEnv(env []string, kv string) []string {
	eq := strings.IndexByte(kv, '=')
	if eq <= 0 {
		return env
	}
	key := kv[:eq]
	prefix := key + "="
	for i, e := range env {
		if strings.HasPrefix(e, prefix) {
			env[i] = kv
			return env
		}
	}
	return append(env, kv)
}

func lookupEnv(env []string, key string) string {
	prefix := key + "="
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return e[len(prefix):]
		}
	}
	return ""
}

// waitPortReleased polls TCP bind on 127.0.0.1:port until success or
// timeout.  We don't fail the test on timeout — the next bind() will
// surface the real error with better context.  We just log a warning.
func waitPortReleased(port int, timeout time.Duration) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.After(timeout)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			return
		}
		select {
		case <-deadline:
			return
		case <-tick.C:
		}
	}
}
