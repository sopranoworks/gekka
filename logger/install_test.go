package logger

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// closableRecorder is a slog.Handler that captures records and tracks
// whether Close was called. Used to assert Install closes superseded
// main handlers and to observe routing behaviour.
type closableRecorder struct {
	mu      sync.Mutex
	records []slog.Record
	closed  atomic.Bool
}

func (h *closableRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *closableRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *closableRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *closableRecorder) WithGroup(string) slog.Handler { return h }

func (h *closableRecorder) Close() error {
	h.closed.Store(true)
	return nil
}

func (h *closableRecorder) RecordCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *closableRecorder) WasClosed() bool { return h.closed.Load() }

// resetLoggerState snapshots and restores the package-level logger state
// across tests so installs in one test do not leak into the next.
func resetLoggerState(t *testing.T) {
	t.Helper()
	prevLogger := defaultLogger.Load()
	prevMain := mainLevelVar.Level()
	prevStdout := stdoutLevelVar.Level()
	prevVersion := installVersion.Load()
	t.Cleanup(func() {
		defaultLogger.Store(prevLogger)
		mainLevelVar.Set(prevMain)
		stdoutLevelVar.Set(prevStdout)
		installVersion.Store(prevVersion)
	})
}

func TestInstall_ReplacesMainLeg(t *testing.T) {
	resetLoggerState(t)

	custom := &closableRecorder{}
	uninstall, err := Install(Options{
		Main:        custom,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: LevelOff,
	})
	require.NoError(t, err)
	require.NotNil(t, uninstall)
	defer uninstall()

	Default().Info("hello-main")

	require.Equal(t, 1, custom.RecordCount(),
		"custom main handler should observe the emitted record")
}

func TestInstall_StdoutLegPreserved(t *testing.T) {
	resetLoggerState(t)

	var buf bytes.Buffer
	stdoutJSON := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: stdoutLevelVar})
	main1 := &closableRecorder{}

	uninstall1, err := Install(Options{
		Main:        main1,
		Stdout:      stdoutJSON,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: slog.LevelInfo,
	})
	require.NoError(t, err)
	defer uninstall1()

	Default().Info("first")
	require.Equal(t, 1, main1.RecordCount())
	require.Contains(t, buf.String(), "first",
		"stdout leg should receive the first emit")

	// Replace main leg via a second Install. The stdout leg established at
	// first Install must persist — only the main leg is swapped at runtime.
	main2 := &closableRecorder{}
	uninstall2, err := Install(Options{
		Main:        main2,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: slog.LevelInfo,
	})
	require.NoError(t, err)
	defer uninstall2()

	buf.Reset()
	Default().Info("second")

	require.Equal(t, 1, main2.RecordCount(),
		"second main handler should observe the post-replacement record")
	require.Contains(t, buf.String(), "second",
		"stdout leg from first Install must survive main-leg replacement")
}

func TestUninstall_RestoresDefaults(t *testing.T) {
	resetLoggerState(t)

	custom := &closableRecorder{}
	uninstall, err := Install(Options{
		Main:        custom,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: LevelOff,
	})
	require.NoError(t, err)

	Default().Info("before-uninstall")
	require.Equal(t, 1, custom.RecordCount())

	uninstall()

	Default().Info("after-uninstall")
	require.Equal(t, 1, custom.RecordCount(),
		"custom main handler must not receive any records after uninstall")

	// The composite handler stays in place — only the main leg reverts to
	// a fresh stdout JSON handler. Default() must still be a non-nil
	// *slog.Logger backed by *compositeHandler.
	require.NotNil(t, Default())
	_, ok := Default().Handler().(*compositeHandler)
	require.True(t, ok, "composite handler should remain installed after uninstall")
}

func TestInstall_Idempotent(t *testing.T) {
	resetLoggerState(t)

	main1 := &closableRecorder{}
	uninstall1, err := Install(Options{
		Main:        main1,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: LevelOff,
	})
	require.NoError(t, err)

	main2 := &closableRecorder{}
	uninstall2, err := Install(Options{
		Main:        main2,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: LevelOff,
	})
	require.NoError(t, err)
	defer uninstall2()

	require.True(t, main1.WasClosed(),
		"second Install should close superseded main handler if it implements io.Closer")

	Default().Info("after-second-install")
	require.Equal(t, 0, main1.RecordCount(),
		"first main handler should no longer receive records")
	require.Equal(t, 1, main2.RecordCount(),
		"second main handler should receive the post-replacement record")

	// The first Install's uninstall is superseded — calling it must not
	// disturb the live state put in place by the second Install.
	uninstall1()

	Default().Info("after-stale-uninstall")
	require.Equal(t, 2, main2.RecordCount(),
		"stale uninstall should be a no-op; main2 must still receive records")
}

func TestInstall_ConcurrentEmit(t *testing.T) {
	resetLoggerState(t)

	main1 := &closableRecorder{}
	uninstall1, err := Install(Options{
		Main:        main1,
		MainLevel:   slog.LevelInfo,
		StdoutLevel: LevelOff,
	})
	require.NoError(t, err)
	defer uninstall1()

	const goroutines = 100
	const emitsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < emitsPerGoroutine; j++ {
				Default().Info("concurrent-emit")
			}
		}()
	}

	main2 := &closableRecorder{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// Leave the second Install live until the test exits — uninstalling
		// here would swap the main leg back to the default JSON-to-stdout
		// handler while worker goroutines are still emitting, dropping
		// records onto stdout. resetLoggerState's cleanup tears down the
		// package state when the test returns.
		if _, err := Install(Options{
			Main:        main2,
			MainLevel:   slog.LevelInfo,
			StdoutLevel: LevelOff,
		}); err != nil {
			t.Errorf("concurrent Install failed: %v", err)
		}
	}()

	close(start)
	wg.Wait()

	total := main1.RecordCount() + main2.RecordCount()
	require.Equal(t, goroutines*emitsPerGoroutine, total,
		"every emit must land on exactly one main leg")
}
