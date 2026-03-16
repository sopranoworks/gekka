/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// gekka-metrics is a sidecar process that periodically scrapes the Gekka
// Cluster HTTP Management API and emits cluster-state metrics.
//
// Currently the metrics are emitted as structured log entries (a placeholder
// for a full OpenTelemetry export pipeline).  The scrape interval and target
// URL are read from a HOCON application.conf via the standard gekka config
// loader, then overridable with flags.
//
// Usage:
//
//	gekka-metrics --config application.conf
//	gekka-metrics --url http://node1:8558 --interval 30s
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/internal/management/client"
)

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (optional)")
	flagURL := flag.String("url", "", "Management API base URL (overrides config)")
	flagInterval := flag.String("interval", "", "Scrape interval, e.g. 15s (overrides config)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// ── Resolve configuration ─────────────────────────────────────────────────

	managementURL := "http://127.0.0.1:8558"
	scrapeInterval := 15 * time.Second

	if *flagConfig != "" {
		cfg, err := gekka.LoadConfig(*flagConfig)
		if err != nil {
			logger.Error("load config", "error", err)
			os.Exit(1)
		}
		if cfg.Metrics.ManagementURL != "" {
			managementURL = cfg.Metrics.ManagementURL
		}
		if cfg.Metrics.ScrapeInterval != "" {
			if d, err := time.ParseDuration(cfg.Metrics.ScrapeInterval); err == nil {
				scrapeInterval = d
			} else {
				logger.Warn("invalid scrape-interval in config, using default",
					"value", cfg.Metrics.ScrapeInterval, "default", scrapeInterval)
			}
		}
	}

	// CLI flags override config values.
	if *flagURL != "" {
		managementURL = *flagURL
	}
	if *flagInterval != "" {
		d, err := time.ParseDuration(*flagInterval)
		if err != nil {
			logger.Error("invalid --interval", "value", *flagInterval, "error", err)
			os.Exit(1)
		}
		scrapeInterval = d
	}

	logger.Info("gekka-metrics starting",
		"management_url", managementURL,
		"scrape_interval", scrapeInterval)

	// ── Run scrape loop ───────────────────────────────────────────────────────

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c := client.New(managementURL)
	ticker := time.NewTicker(scrapeInterval)
	defer ticker.Stop()

	// Run one immediate scrape so operators see output without waiting.
	scrape(logger, c)

	for {
		select {
		case <-ticker.C:
			scrape(logger, c)
		case <-ctx.Done():
			logger.Info("shutting down")
			return
		}
	}
}

// scrape fetches the current cluster state and logs key metrics.
// This is a placeholder for a full OpenTelemetry export pipeline.
func scrape(logger *slog.Logger, c *client.Client) {
	members, err := c.Members()
	if err != nil {
		logger.Error("scrape failed", "error", err)
		return
	}

	upCount := 0
	for _, m := range members {
		if m.Status == "Up" {
			upCount++
		}
	}

	logger.Info("cluster_members_up",
		"count", upCount,
		"total", len(members),
	)
}
