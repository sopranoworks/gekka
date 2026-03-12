/*
 * logger.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package actor

import (
	"context"
	"log/slog"
)

// ActorLogger is a structured logger that automatically includes actor context
// attributes in every log entry.
//
// Every call adds:
//   - "actor"  — the full actor-path URI (e.g. "pekko://Sys@host:port/user/echo")
//   - "system" — the actor-system name (e.g. "ClusterSystem")
//
// When called during Receive (i.e. when a sender is set), it additionally adds:
//   - "sender" — the sender's actor-path URI (omitted when there is no sender)
//
// Obtain an ActorLogger through BaseActor.Log():
//
//	func (a *MyActor) Receive(msg any) {
//	    a.Log().Info("received message", "type", fmt.Sprintf("%T", msg))
//	}
type ActorLogger struct {
	base   *slog.Logger // underlying slog.Logger with actor + system pre-set
	getRef func() Ref   // returns currentSender (called at log time for "sender")
}

// newActorLogger constructs an ActorLogger from a slog.Handler and actor
// context.  h is the handler configured on the Cluster (or the default
// slog handler when none is set).  ref is the actor's own ActorRef;
// getRef is a closure that returns the current sender (or nil) at log time.
func newActorLogger(h slog.Handler, ref Ref, getRef func() Ref) ActorLogger {
	logger := slog.New(h).With(
		"actor", ref.Path(),
		"system", systemName(ref.Path()),
	)
	return ActorLogger{base: logger, getRef: getRef}
}

// withSender returns a *slog.Logger with the sender attribute pre-set if
// a sender is currently set on the actor. This is called lazily at log time.
func (l ActorLogger) logger() *slog.Logger {
	if l.getRef == nil {
		return l.base
	}
	if s := l.getRef(); s != nil && s.Path() != "" {
		return l.base.With("sender", s.Path())
	}
	return l.base
}

// Debug logs at DEBUG level with the actor context.
func (l ActorLogger) Debug(msg string, args ...any) {
	l.logger().Debug(msg, args...)
}

// Info logs at INFO level with the actor context.
func (l ActorLogger) Info(msg string, args ...any) {
	l.logger().Info(msg, args...)
}

// Warn logs at WARN level with the actor context.
func (l ActorLogger) Warn(msg string, args ...any) {
	l.logger().Warn(msg, args...)
}

// Error logs at ERROR level with the actor context.
func (l ActorLogger) Error(msg string, args ...any) {
	l.logger().Error(msg, args...)
}

// Log emits a record at the given level with the actor context.
// Use this when the level is dynamic.
func (l ActorLogger) Log(ctx context.Context, level slog.Level, msg string, args ...any) {
	l.logger().Log(ctx, level, msg, args...)
}

// With returns a new ActorLogger with additional pre-set attributes.
// Equivalent to slog.Logger.With: attributes are attached to every
// subsequent log call on the returned logger.
//
//	log := a.Log().With("requestID", id)
//	log.Info("handling request")
func (l ActorLogger) With(args ...any) ActorLogger {
	return ActorLogger{base: l.base.With(args...), getRef: l.getRef}
}

// Enabled reports whether the logger would emit a record at the given level.
// Use this to guard expensive argument construction:
//
//	if a.Log().Enabled(ctx, slog.LevelDebug) {
//	    a.Log().Debug("payload", "data", expensiveFormat(payload))
//	}
func (l ActorLogger) Enabled(ctx context.Context, level slog.Level) bool {
	return l.logger().Enabled(ctx, level)
}

// systemName extracts the actor-system name from a full actor-path URI.
// "pekko://ClusterSystem@host:port/user/foo" → "ClusterSystem"
// Returns an empty string when the path cannot be parsed.
func systemName(path string) string {
	// Path form: <scheme>://<system>@<host>:<port>/…
	// Fast manual parse — avoids importing net/url in the hot path.
	const sep = "://"
	i := len("pekko")
	if len(path) > 5 && path[5] == ':' {
		// could be pekko or akka
	} else if len(path) > 4 && path[4] == ':' {
		i = len("akka")
	} else {
		return ""
	}
	if len(path) <= i+len(sep) {
		return ""
	}
	rest := path[i+len(sep):]
	at := -1
	for j, c := range rest {
		if c == '@' {
			at = j
			break
		}
	}
	if at < 0 {
		return ""
	}
	return rest[:at]
}
