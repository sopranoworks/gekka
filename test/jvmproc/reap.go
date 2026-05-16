/*
 * reap.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// ReapStaleTestJVMs kills `java` processes left over from a previous test
// binary run.  Without this, integration tests inherit zombie JVMs that
// still hold the test ports (2552, 2554, ...) and every subsequent bind
// fails — the "stray test JVM, killing it" pattern that used to require
// manual intervention.
//
// We deliberately restrict the kill set to JVMs whose argv contains one
// of the assembly-JAR markers this package itself produces (see
// `assembly.go`).  An arbitrary `java` process started by the developer
// (IDE, gradle, anything else) is never touched.
//
// Reap runs as a TestMain pre-pass — see `main_integration_test.go` —
// so the first thing the suite does is guarantee a clean port surface.

package jvmproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// staleJVMMarkers is the set of substrings that, when present in a `java`
// process's argv, identify it as a leftover from this test infrastructure.
// Each entry corresponds to one of the assembly JARs produced by sbt-assembly
// (see PekkoAssembly, AkkaAssembly, Akka2614Assembly in assembly.go).
//
// Extend this list when a new assembly JAR is introduced — the reaper is
// the contract that "no Java process spawned by an earlier test binary
// survives the start of the next one."
var staleJVMMarkers = []string{
	"pekko-mains-assembly.jar",
	"akka-mains-assembly.jar",
	"akka-2614-mains-assembly.jar",
}

// testPortsToReleaseOnStartup is the set of well-known ports that
// integration tests bind to.  After SIGKILLing stale JVMs we poll each
// port until the kernel actually frees the socket, so the next test that
// calls net.Listen on the same port doesn't race the kernel's TIME_WAIT
// release.
//
// Tests that use OS-assigned ephemeral ports (allocateFreeTCPPort) are
// not in scope: those collide only if the previous test leaked a JVM
// holding a high port that the OS then handed back, which is excluded
// by the marker-based kill above.
var testPortsToReleaseOnStartup = []int{
	2550, 2551, 2552, 2553, 2554, 2555, 2556, 2557, 2558, 2559, 2560,
}

// ReapStaleTestJVMs scans for `java` processes whose argv contains any
// staleJVMMarker, SIGKILLs each one's process group, and waits for the
// well-known test ports to be bindable.  Safe to call when there is
// nothing to reap (no-op).  Returns the count of killed PIDs and any
// fatal error.
//
// Implementation notes:
//   - Discovery via `ps -axww -o pid=,command=` (BSD/macOS + Linux both
//     support these flags; -ww disables truncation of command lines so
//     the JAR path is visible).  We deliberately do not depend on
//     `pgrep -f` — its behavior diverges between macOS and Linux when
//     the command-line is long.
//   - We SIGKILL the process group (negative PID) so any forked
//     grandchildren go with the leader, mirroring jvmproc.Stop.
//   - Port-release polling has a fixed 30 s budget; if a port is still
//     held after that, we surface the situation in the error rather
//     than hanging the entire test binary.
func ReapStaleTestJVMs(ctx context.Context) (int, error) {
	pids, err := findStaleTestJVMs()
	if err != nil {
		return 0, fmt.Errorf("reap: discovery failed: %w", err)
	}

	killed := 0
	for _, pid := range pids {
		if err := killJVMGroup(pid); err != nil {
			// Don't bail on a single failure: keep killing the rest.
			// We surface the aggregate failure mode via port-release
			// polling below — if even one survivor still holds a
			// port, that's the actionable signal.
			fmt.Fprintf(io.Discard, "reap: kill pid=%d: %v\n", pid, err)
			continue
		}
		killed++
	}

	if killed > 0 {
		// Give the OS time to reap the children and free the sockets.
		// Without a small grace period the immediate port-release poll
		// below sees TIME_WAIT and bails on its 30 s budget for nothing.
		select {
		case <-ctx.Done():
			return killed, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	if err := waitForTestPortsReleased(ctx, testPortsToReleaseOnStartup, 30*time.Second); err != nil {
		return killed, fmt.Errorf("reap: ports still held after kill: %w", err)
	}

	return killed, nil
}

// findStaleTestJVMs returns the PIDs of all running `java` processes whose
// command-line matches one of staleJVMMarkers.  Empty slice + nil error
// means "nothing to clean up".
func findStaleTestJVMs() ([]int, error) {
	out, err := exec.Command("ps", "-axww", "-o", "pid=,command=").Output()
	if err != nil {
		return nil, fmt.Errorf("ps: %w", err)
	}
	var pids []int
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// "pid=,command=" emits "<pid> <command line...>" with one or more
		// spaces.  Splitting on the first space gives us the PID; the rest
		// is the full argv (re-joined by ps with single spaces).
		sp := strings.IndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		pidStr := strings.TrimSpace(line[:sp])
		cmd := strings.TrimSpace(line[sp+1:])
		// Restrict to `java` invocations to avoid touching e.g. a sbt
		// launcher script that referenced the JAR in its arguments but
		// is itself a shell or wrapper process.
		if !strings.HasPrefix(cmd, "java ") && !strings.Contains(cmd, "/java ") {
			continue
		}
		if !matchesStaleMarker(cmd) {
			continue
		}
		pid, perr := strconv.Atoi(pidStr)
		if perr != nil {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func matchesStaleMarker(cmd string) bool {
	for _, m := range staleJVMMarkers {
		if strings.Contains(cmd, m) {
			return true
		}
	}
	return false
}

// killJVMGroup SIGKILLs the process group of pid.  If the process is no
// longer a group leader (e.g. it was launched without Setpgid), falls back
// to a direct PID-level kill.
func killJVMGroup(pid int) error {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		// Fall back to PID-level kill — better than nothing.
		if kerr := syscall.Kill(pid, syscall.SIGKILL); kerr != nil && !errors.Is(kerr, syscall.ESRCH) {
			return kerr
		}
		return nil
	}
	if pgid <= 1 {
		return errors.New("reap: refusing to signal pgid <= 1")
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

// waitForTestPortsReleased polls each port for bindability and returns
// nil once every port can be bind()ed.  On timeout, returns an error
// naming the ports still held.
func waitForTestPortsReleased(ctx context.Context, ports []int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		var held []int
		for _, port := range ports {
			if !tcpPortBindable(port) {
				held = append(held, port)
			}
		}
		if len(held) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ports still held: %v", held)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func tcpPortBindable(port int) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}
