package logger

import (
	"io"
	"log/slog"
	"os"
	"sync/atomic"
)

// Options configure Install. When a leg is nil, Install substitutes a
// stdout JSON handler bound to the matching package LevelVar. The level
// fields are written to mainLevelVar and stdoutLevelVar; the change is
// visible to every concurrent emit on the next record.
type Options struct {
	Main        slog.Handler
	Stdout      slog.Handler
	MainLevel   slog.Level
	StdoutLevel slog.Level
}

// installVersion is incremented on every successful Install. Each Install's
// uninstall closure captures the version it produced; later uninstall calls
// compare against the current version to detect that they have been
// superseded by a subsequent Install and become no-ops.
var installVersion atomic.Uint64

// Install promotes the package default logger from the bootstrap stdout
// JSON handler to the two-channel composite handler that gekka uses at
// runtime. The first call constructs the composite; subsequent calls
// atomically swap the composite's main leg, closing the superseded
// handler if it implements io.Closer. The stdout leg is established at
// the first call and is not replaced thereafter — an external-log-server
// transport plugs into the main leg, leaving the stdout leg as the
// always-available fallback.
//
// Install returns an uninstall function that swaps the main leg back to
// a fresh stdout JSON handler; the composite handler stays in place.
// Only the latest Install's uninstall is live — any uninstall returned
// by a superseded Install becomes a no-op once the next Install runs,
// so callers can safely defer every uninstall.
//
// Install is safe to call concurrently with emits on the package default
// logger. The main leg storage is an atomic pointer; the LevelVars are
// slog primitives whose Set is itself atomic.
func Install(opts Options) (func(), error) {
	if opts.Main == nil {
		opts.Main = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: mainLevelVar})
	}
	if opts.Stdout == nil {
		opts.Stdout = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: stdoutLevelVar})
	}

	mainLevelVar.Set(opts.MainLevel)
	stdoutLevelVar.Set(opts.StdoutLevel)

	cur := defaultLogger.Load()
	var comp *compositeHandler
	if existing, ok := cur.Handler().(*compositeHandler); ok {
		comp = existing
		prev := comp.swapMain(opts.Main)
		closeIfCloser(prev)
	} else {
		comp = newCompositeHandler(opts.Main, opts.Stdout, mainLevelVar, stdoutLevelVar)
		defaultLogger.Store(slog.New(comp))
	}

	myVersion := installVersion.Add(1)

	return func() {
		if installVersion.Load() != myVersion {
			return
		}
		fallback := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: mainLevelVar})
		comp.swapMain(fallback)
	}, nil
}

// closeIfCloser invokes Close on h when it satisfies io.Closer. The
// returned error is intentionally swallowed: at this point h is
// unreachable from any further log emits, so a failure to close is at
// most a leak on the previously-installed handler — not something the
// caller of Install can act on.
func closeIfCloser(h slog.Handler) {
	if c, ok := h.(io.Closer); ok {
		_ = c.Close()
	}
}
