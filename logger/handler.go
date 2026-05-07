package logger

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
)

// compositeHandler routes every slog.Record to two filtered legs:
//
//   - main   — gated by mainLevel  (Pekko's pekko.loglevel)
//   - stdout — gated by stdoutLevel (Pekko's pekko.stdout-loglevel)
//
// The main leg is held in an atomic.Pointer so Install can swap it at
// runtime without coordinating with concurrent emits — the forcing
// requirement is that `go test -race ./logger/...` reports clean while
// emits and Install run in parallel. The stdout leg is set once at
// construction and never replaced; an external-log-server transport
// plugs into the main leg, leaving the stdout leg as the bootstrap-time
// fallback.
//
// The two LevelVars are the single source of truth for level filtering;
// neither leg performs its own level gating. This lets callers flip
// runtime levels by Set() on the LevelVars and have it observed on the
// next record without rebuilding handlers.
type compositeHandler struct {
	main        atomic.Pointer[slog.Handler]
	stdout      slog.Handler
	mainLevel   *slog.LevelVar
	stdoutLevel *slog.LevelVar
}

// newCompositeHandler builds a compositeHandler. All four arguments must be
// non-nil; this is an internal constructor and the package wiring guarantees
// the invariants.
func newCompositeHandler(main, stdout slog.Handler, mainLevel, stdoutLevel *slog.LevelVar) *compositeHandler {
	h := &compositeHandler{
		stdout:      stdout,
		mainLevel:   mainLevel,
		stdoutLevel: stdoutLevel,
	}
	h.main.Store(&main)
	return h
}

// loadMain returns the currently installed main-leg handler. The composite
// constructor guarantees a non-nil main leg; Install never stores a nil
// leg either, so loadMain is safe to dereference.
func (h *compositeHandler) loadMain() slog.Handler {
	p := h.main.Load()
	return *p
}

// swapMain atomically replaces the main leg with newMain and returns the
// previous handler. Used by Install to rotate the main leg and surface
// the superseded handler so it can be Close()d if it implements io.Closer.
func (h *compositeHandler) swapMain(newMain slog.Handler) slog.Handler {
	prev := h.main.Swap(&newMain)
	return *prev
}

// Enabled returns true if at least one leg would accept a record at the given
// level. Returning false here lets slog.Logger short-circuit before allocating
// the slog.Record — important for hot paths where every leg is OFF.
func (h *compositeHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.mainLevel.Level() || level >= h.stdoutLevel.Level()
}

// Handle dispatches the record to each leg whose LevelVar admits it. Errors
// from the legs are joined so a failure on one leg does not silently mask the
// other.
func (h *compositeHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	if r.Level >= h.mainLevel.Level() {
		if err := h.loadMain().Handle(ctx, r); err != nil {
			errs = append(errs, err)
		}
	}
	if r.Level >= h.stdoutLevel.Level() {
		if err := h.stdout.Handle(ctx, r); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// WithAttrs snapshots the current main leg, derives WithAttrs on each
// leg, and returns a fresh composite holding the derived legs. Runtime
// Install swaps on the original composite do not propagate into clones
// produced by WithAttrs/WithGroup; in practice loggers are derived after
// Install runs at Spawn time, so the snapshot is the post-install leg.
func (h *compositeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return newCompositeHandler(
		h.loadMain().WithAttrs(attrs),
		h.stdout.WithAttrs(attrs),
		h.mainLevel,
		h.stdoutLevel,
	)
}

// WithGroup snapshots the current main leg and returns a fresh composite
// whose legs each open the named group.
func (h *compositeHandler) WithGroup(name string) slog.Handler {
	return newCompositeHandler(
		h.loadMain().WithGroup(name),
		h.stdout.WithGroup(name),
		h.mainLevel,
		h.stdoutLevel,
	)
}
