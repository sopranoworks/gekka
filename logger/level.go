package logger

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
)

// LevelOff is the sentinel slog.Level used to disable a logging leg entirely.
// It maps the Pekko/Akka "OFF" log level into slog's open-ended Level space:
// any record level r satisfies r < LevelOff, so Handler.Enabled returns false
// for every record when a LevelVar is set to LevelOff.
var LevelOff = slog.Level(math.MaxInt32)

// ParseLevel converts a Pekko/Akka log-level string into the corresponding
// slog.Level. The accepted spellings (case-insensitive) are:
//
//	OFF              -> LevelOff
//	ERROR            -> slog.LevelError
//	WARNING / WARN   -> slog.LevelWarn   (Pekko uses "WARNING"; "WARN" is the slog-native spelling)
//	INFO             -> slog.LevelInfo
//	DEBUG            -> slog.LevelDebug
//
// Unknown spellings — including the empty string — return a non-nil error.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "off":
		return LevelOff, nil
	case "error":
		return slog.LevelError, nil
	case "warning", "warn":
		return slog.LevelWarn, nil
	case "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	default:
		return 0, fmt.Errorf("logger: unknown log level %q", s)
	}
}
