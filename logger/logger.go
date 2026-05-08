// Package logger is gekka's permanent slog-based logging foundation.
//
// The public API surface is intentionally `*slog.Logger` and `slog.Handler`
// from the Go standard library — there is no wrapper layer. Two LevelVars
// drive the composite handler installed at Spawn time:
//
//	MainLevelVar()   ← pekko.loglevel        (the "main" leg; future external-log-server transport plugs in here)
//	StdoutLevelVar() ← pekko.stdout-loglevel (the "stdout" leg; bootstrap-time fallback that always emits JSON to stdout)
//
// Both LevelVars accept slog.Level values returned by ParseLevel, including
// the LevelOff sentinel for the Pekko/Akka "OFF" level. See the package
// godoc on level.go and handler.go for the level mapping and the routing
// semantics respectively.
package logger

import (
	"log/slog"
	"os"
	"sync/atomic"
)

var (
	// mainLevelVar / stdoutLevelVar are the two operative LevelVars
	// referenced by the composite handler installed via Install (A2).
	// They are declared at package scope so callers can call
	// MainLevelVar()/StdoutLevelVar() and stash the pointer for later
	// runtime adjustments.
	mainLevelVar   = new(slog.LevelVar)
	stdoutLevelVar = new(slog.LevelVar)

	// defaultLogger is swapped atomically by Install (A2) and by
	// setDefaultLoggerForTest. Reading it via Default() is lock-free.
	defaultLogger atomic.Pointer[slog.Logger]
)

func init() {
	// Bootstrap-time logger: a flat JSON handler writing to stdout at
	// LevelWarn. This makes early warnings — e.g. malformed HOCON keys
	// detected during parse, before Spawn calls Install — visible.
	// Once Install runs, defaultLogger is swapped to a compositeHandler
	// that consults mainLevelVar and stdoutLevelVar.
	bootstrap := new(slog.LevelVar)
	bootstrap.Set(slog.LevelWarn)
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: bootstrap})
	defaultLogger.Store(slog.New(h))
}

// Default returns the current package-level *slog.Logger. The returned
// pointer is never nil — package init publishes a bootstrap logger before
// any caller can observe the package.
func Default() *slog.Logger {
	return defaultLogger.Load()
}

// WithSystem returns a logger derived from Default() carrying a "system"
// attribute named after the supplied subsystem. Mirrors Pekko's
// `Logger(system, "Subsystem")` convention so log records can be filtered
// by subsystem in downstream tooling.
func WithSystem(name string) *slog.Logger {
	return Default().With(slog.String("system", name))
}

// MainLevelVar returns the *slog.LevelVar that gates the composite
// handler's main leg (Pekko's `pekko.loglevel`). Set returns immediately;
// the change is visible to every concurrent emit on the next record.
//
// MainLevelVar is operative only after Install has installed the composite
// handler. Pre-Install Set calls are remembered but the bootstrap logger
// — which writes flat JSON to stdout at LevelWarn — does not consult the
// LevelVar. Once Install runs the LevelVar's current value takes effect.
func MainLevelVar() *slog.LevelVar { return mainLevelVar }

// StdoutLevelVar returns the *slog.LevelVar that gates the composite
// handler's stdout leg (Pekko's `pekko.stdout-loglevel`). Same caveats as
// MainLevelVar.
func StdoutLevelVar() *slog.LevelVar { return stdoutLevelVar }

// setDefaultLoggerForTest atomically replaces the package default logger
// for the duration of a test, returning a function that restores the
// previous default. It is intentionally unexported: A2's Install is the
// public lifecycle hook for production callers.
func setDefaultLoggerForTest(l *slog.Logger) func() {
	prev := defaultLogger.Swap(l)
	return func() {
		defaultLogger.Store(prev)
	}
}

// SetDefaultForTest atomically replaces the package default logger and
// returns a function that restores the previous default. Intended for
// tests in dependent packages that need to capture structured log output
// without triggering Install's composite-handler lifecycle. The override
// bypasses the composite handler and its LevelVars while active.
func SetDefaultForTest(l *slog.Logger) (restore func()) {
	return setDefaultLoggerForTest(l)
}
