// cmd/showcase-runner/main.go — orchestrator for gekka_showcase_test
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	gateWindow     = 600 * time.Second // §5 Gate 2 strict window
	setupBudget    = 90 * time.Second
	gate1Budget    = 30 * time.Second
	teardownBudget = 30 * time.Second
)

func main() {
	scalaJar := flag.String("scala-jar", "test/showcase/scala/target/scala-2.13/showcase-scala-assembly.jar", "path to assembled scala showcase jar")
	gekkaBin := flag.String("gekka-bin", "bin/showcase-gekka", "path to gekka showcase binary")
	runID := flag.String("run-id", time.Now().UTC().Format("20060102T150405Z"), "run identifier for artifact dir")
	flag.Parse()

	artifactDir := ".testrun/showcase-" + *runID
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir artifacts: %v\n", err)
		os.Exit(10)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("showcase runner start", "run_id", *runID, "artifact_dir", artifactDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT/SIGTERM → cancel ctx so phases exit gracefully.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Warn("received signal — cancelling")
		cancel()
	}()

	exit := runPhases(ctx, runnerCfg{
		ScalaJar:    *scalaJar,
		GekkaBin:    *gekkaBin,
		ArtifactDir: artifactDir,
		Logger:      logger,
	})
	os.Exit(exit)
}

type runnerCfg struct {
	ScalaJar    string
	GekkaBin    string
	ArtifactDir string
	Logger      *slog.Logger
}
