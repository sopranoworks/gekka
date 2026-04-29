/*
 * atexit.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

// Why this file exists
//
// `go test -timeout 30s` enforces its limit by sending SIGQUIT to the test
// binary, expecting the runtime's default handler to dump goroutine stacks
// and exit(2).  The default handler runs synchronously and kills the
// process before any t.Cleanup callbacks fire — including jvmproc's
// killpg-based teardown.  The result: every JVM spawned by Spawn leaks
// past timeout, holds its bound port, and breaks the next test run.
//
// The fix here is twofold.  First, every Spawn registers the resulting
// *Process in a package-level registry so the cleanup target is known
// without iterating goroutines.  Second, an init() goroutine traps
// SIGQUIT, SIGTERM, and SIGINT, killpg's every registered Process, and
// then either re-raises (for SIGTERM/SIGINT) or replicates the runtime's
// SIGQUIT default (dump-stacks + exit(2)).  The handler runs to
// completion before the process dies, so leaks survive only when the
// kernel itself force-kills us — and at that point process-group
// orphans are an OS-level problem, not jvmproc's.
//
// `Stop` removes the *Process from the registry so a normal-exit test
// run does not double-kill on shutdown.

var (
	registryMu sync.Mutex
	registry   = make(map[*Process]struct{})

	// handlerOnce gates installation of the signal-handling goroutine.
	// Tests that don't spawn anything pay no cost; the first Spawn arms
	// the handler.
	handlerOnce sync.Once
)

// register records p so the signal handler can kill its process group on
// abnormal termination.  Idempotent.
func register(p *Process) {
	if p == nil || p.pgid <= 1 {
		return
	}
	registryMu.Lock()
	registry[p] = struct{}{}
	registryMu.Unlock()
	handlerOnce.Do(installSignalHandler)
}

// unregister drops p from the registry.  Called from Stop after the
// process group has been reaped so the handler doesn't redundantly
// kill an already-dead group.
func unregister(p *Process) {
	if p == nil {
		return
	}
	registryMu.Lock()
	delete(registry, p)
	registryMu.Unlock()
}

// killAllRegistered SIGKILLs every registered process group.  Drains
// the registry — duplicate calls (handler racing with normal Stop)
// are harmless because killGroup tolerates ESRCH.
func killAllRegistered() {
	registryMu.Lock()
	procs := make([]*Process, 0, len(registry))
	for p := range registry {
		procs = append(procs, p)
	}
	registry = make(map[*Process]struct{})
	registryMu.Unlock()

	for _, p := range procs {
		_ = killGroup(p.pgid, syscall.SIGKILL)
	}
}

// installSignalHandler wires SIGQUIT/SIGTERM/SIGINT to killAllRegistered,
// preserving the runtime's user-visible behavior for each signal.
//
// SIGQUIT is the load-bearing case: `go test -timeout` sends it.  We
// replicate Go's default (dump all goroutine stacks to stderr, exit(2))
// so the timeout diagnostic the developer expects is still produced.
//
// SIGTERM and SIGINT are reset and re-raised after cleanup so exit
// status conventions (128+signal) survive.
func installSignalHandler() {
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		killAllRegistered()
		if sig == syscall.SIGQUIT {
			// Mirror runtime.signalM_quit: dump every goroutine, then
			// exit(2).  Without this the developer loses the stack
			// trace they'd otherwise see on `go test -timeout`.
			buf := make([]byte, 1<<20)
			for {
				n := runtime.Stack(buf, true)
				if n < len(buf) {
					_, _ = os.Stderr.Write(buf[:n])
					break
				}
				buf = make([]byte, 2*len(buf))
			}
			os.Exit(2)
		}
		// Restore default disposition and re-raise so the runtime's
		// usual exit semantics (128+signal) apply.
		signal.Reset(sig)
		_ = syscall.Kill(syscall.Getpid(), sig.(syscall.Signal))
	}()
}
