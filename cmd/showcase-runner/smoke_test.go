// cmd/showcase-runner/smoke_test.go
//
// Runner-only smoke: drives the production spawnChild + awaitReady path
// against a real subprocess that floods stderr with sustained ERROR lines
// before emitting the SHOWCASE READY sentinel on stdout. Phase 1-2
// completion criterion from the cross-language CBOR codec gap follow-up.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const mockChildEnv = "GEKKA_SHOWCASE_RUNNER_MOCK_CHILD"

func TestMain(m *testing.M) {
	// When invoked as a mock child (via the spawnChild smoke test below),
	// flood stderr with N ERROR lines, emit the READY sentinel on stdout,
	// then sleep until killed.
	if v := os.Getenv(mockChildEnv); v != "" {
		mockChildMain(v)
		return
	}
	os.Exit(m.Run())
}

func mockChildMain(label string) {
	floodN := 5000
	if s := os.Getenv("GEKKA_SHOWCASE_RUNNER_MOCK_FLOOD"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			floodN = n
		}
	}
	for i := 0; i < floodN; i++ {
		fmt.Fprintf(os.Stderr, "[12:00:00.000] ERROR org.example.mock - flood %d\n", i)
	}
	fmt.Fprintf(os.Stdout, "--- SHOWCASE NODE READY: %s ---\n", label)
	time.Sleep(30 * time.Second)
}

// TestSpawnChild_ReadyDetectedUnderRealSubprocessFlood drives the production
// pipeReader + awaitReady code path with a real OS-level subprocess that
// emits a sustained ERROR flood on stderr before the READY sentinel on
// stdout. Proves the separate READY tap (option iii) holds end-to-end, not
// just at the unit-test boundary.
func TestSpawnChild_ReadyDetectedUnderRealSubprocessFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess smoke in -short mode")
	}

	artifactDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		mockChildEnv+"=g-mock",
		"GEKKA_SHOWCASE_RUNNER_MOCK_FLOOD=5000",
	)

	c, err := spawnChild(ctx, "g-mock", cmd, artifactDir)
	if err != nil {
		t.Fatalf("spawnChild: %v", err)
	}
	// Mirror the production gate2 contract: a classification consumer takes
	// over draining c.lines so the pipe reader never blocks. The drain runs
	// until c.lines closes (which happens after both pipeReaders exit on
	// EOF — i.e. after c.stop's SIGTERM takes effect). Without an active
	// drain, the cap=256 buffer fills and pipeReader stalls on send,
	// preventing clean SIGTERM shutdown.
	go func() {
		for range c.lines {
		}
	}()
	defer c.stop(2 * time.Second)

	start := time.Now()
	if err := awaitReady(ctx, c, "g-mock"); err != nil {
		t.Fatalf("awaitReady failed under 5000-line stderr flood: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("READY detected under 5000-line stderr flood in %s", elapsed)
}
