package logger

import (
	"context"
	"errors"
	"log/slog"
)

// compositeHandler routes every slog.Record to two filtered legs:
//
//   - main   — gated by mainLevel  (Pekko's pekko.loglevel)
//   - stdout — gated by stdoutLevel (Pekko's pekko.stdout-loglevel)
//
// Each leg is a slog.Handler that does no level filtering of its own; the
// LevelVars on this struct are the single source of truth. This lets callers
// flip levels at runtime without recreating handlers, and lets a future
// external-log-server transport replace the main leg without touching the
// stdout leg used as the bootstrap-time fallback.
type compositeHandler struct {
	main        slog.Handler
	stdout      slog.Handler
	mainLevel   *slog.LevelVar
	stdoutLevel *slog.LevelVar
}

// newCompositeHandler builds a compositeHandler. All four arguments must be
// non-nil; this is an internal constructor and the package wiring guarantees
// the invariants.
func newCompositeHandler(main, stdout slog.Handler, mainLevel, stdoutLevel *slog.LevelVar) *compositeHandler {
	return &compositeHandler{
		main:        main,
		stdout:      stdout,
		mainLevel:   mainLevel,
		stdoutLevel: stdoutLevel,
	}
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
		if err := h.main.Handle(ctx, r); err != nil {
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

// WithAttrs returns a new compositeHandler whose legs each carry the new
// attrs. The LevelVars are shared by pointer so a runtime level change still
// affects loggers derived via slog.Logger.With.
func (h *compositeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &compositeHandler{
		main:        h.main.WithAttrs(attrs),
		stdout:      h.stdout.WithAttrs(attrs),
		mainLevel:   h.mainLevel,
		stdoutLevel: h.stdoutLevel,
	}
}

// WithGroup returns a new compositeHandler whose legs are each opened in the
// named group. LevelVars are shared by pointer.
func (h *compositeHandler) WithGroup(name string) slog.Handler {
	return &compositeHandler{
		main:        h.main.WithGroup(name),
		stdout:      h.stdout.WithGroup(name),
		mainLevel:   h.mainLevel,
		stdoutLevel: h.stdoutLevel,
	}
}
