package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingHandler captures slog.Records for white-box assertions on the
// composite handler. It is always Enabled so the composite handler's own
// LevelVar gating is the sole filter under test.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingHandler) Records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"OFF", LevelOff},
		{"off", LevelOff},
		{"Off", LevelOff},
		{"ERROR", slog.LevelError},
		{"error", slog.LevelError},
		{"WARNING", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"Warning", slog.LevelWarn},
		{"WARN", slog.LevelWarn},
		{"warn", slog.LevelWarn},
		{"INFO", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"DEBUG", slog.LevelDebug},
		{"debug", slog.LevelDebug},
	}
	for _, tc := range cases {
		got, err := ParseLevel(tc.in)
		require.NoErrorf(t, err, "ParseLevel(%q)", tc.in)
		require.Equalf(t, tc.want, got, "ParseLevel(%q)", tc.in)
	}

	if _, err := ParseLevel("UNKNOWN"); err == nil {
		t.Fatalf("ParseLevel(\"UNKNOWN\") should return an error")
	}
	if _, err := ParseLevel(""); err == nil {
		t.Fatalf("ParseLevel(\"\") should return an error")
	}
}

func TestCompositeHandler_RoutesByLevel(t *testing.T) {
	main := &recordingHandler{}
	stdout := &recordingHandler{}
	mainLV := new(slog.LevelVar)
	mainLV.Set(slog.LevelInfo)
	stdoutLV := new(slog.LevelVar)
	stdoutLV.Set(LevelOff)

	logger := slog.New(newCompositeHandler(main, stdout, mainLV, stdoutLV))
	logger.Info("hello")

	mainRecs := main.Records()
	require.Len(t, mainRecs, 1)
	require.Equal(t, "hello", mainRecs[0].Message)
	require.Empty(t, stdout.Records())
}

func TestCompositeHandler_BothLegsReceive(t *testing.T) {
	main := &recordingHandler{}
	stdout := &recordingHandler{}
	mainLV := new(slog.LevelVar)
	mainLV.Set(slog.LevelDebug)
	stdoutLV := new(slog.LevelVar)
	stdoutLV.Set(slog.LevelWarn)

	logger := slog.New(newCompositeHandler(main, stdout, mainLV, stdoutLV))
	logger.Warn("warn-msg")
	logger.Info("info-msg")

	mainRecs := main.Records()
	require.Len(t, mainRecs, 2)
	require.Equal(t, "warn-msg", mainRecs[0].Message)
	require.Equal(t, "info-msg", mainRecs[1].Message)

	stdoutRecs := stdout.Records()
	require.Len(t, stdoutRecs, 1)
	require.Equal(t, "warn-msg", stdoutRecs[0].Message)
}

func TestCompositeHandler_Enabled_ShortCircuits(t *testing.T) {
	main := &recordingHandler{}
	stdout := &recordingHandler{}
	mainLV := new(slog.LevelVar)
	mainLV.Set(LevelOff)
	stdoutLV := new(slog.LevelVar)
	stdoutLV.Set(LevelOff)

	h := newCompositeHandler(main, stdout, mainLV, stdoutLV)
	ctx := context.Background()
	require.False(t, h.Enabled(ctx, slog.LevelError))
	require.False(t, h.Enabled(ctx, slog.LevelInfo))
	require.False(t, h.Enabled(ctx, slog.LevelDebug))
}

func TestLevelVar_Dynamic(t *testing.T) {
	main := &recordingHandler{}
	stdout := &recordingHandler{}
	mainLV := new(slog.LevelVar)
	mainLV.Set(slog.LevelInfo)
	stdoutLV := new(slog.LevelVar)
	stdoutLV.Set(LevelOff)

	logger := slog.New(newCompositeHandler(main, stdout, mainLV, stdoutLV))

	logger.Info("first")
	require.Len(t, main.Records(), 1)

	mainLV.Set(slog.LevelError)
	logger.Info("second")
	require.Len(t, main.Records(), 1, "Info dropped after main raised to Error")

	logger.Error("third")
	require.Len(t, main.Records(), 2)
}

func TestJSONOutputShape(t *testing.T) {
	var buf bytes.Buffer

	mainLV := new(slog.LevelVar)
	mainLV.Set(slog.LevelInfo)
	stdoutLV := new(slog.LevelVar)
	stdoutLV.Set(LevelOff)

	mainJSON := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: mainLV})
	stdoutJSON := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: stdoutLV})
	composite := newCompositeHandler(mainJSON, stdoutJSON, mainLV, stdoutLV)

	restore := setDefaultLoggerForTest(slog.New(composite))
	defer restore()

	WithSystem("gekka-test").Info("hello-world", slog.String("foo", "bar"))

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line, "expected JSON output, got empty")

	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &rec))

	require.Contains(t, rec, "time")
	require.Contains(t, rec, "level")
	require.Contains(t, rec, "msg")
	require.Equal(t, "hello-world", rec["msg"])
	require.Equal(t, "INFO", rec["level"])
	require.Equal(t, "gekka-test", rec["system"])
	require.Equal(t, "bar", rec["foo"])
}
