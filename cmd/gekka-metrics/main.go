/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// gekka-metrics is a sidecar process that periodically scrapes the Gekka
// Cluster HTTP Management API and exports cluster-state metrics via the
// OpenTelemetry Protocol (OTLP/HTTP).
//
// Configuration is loaded from a HOCON application.conf via the standard
// gekka config loader.  All settings can be overridden with CLI flags.
//
// HOCON keys consumed:
//
//	gekka.metrics.management-url               base URL of the Management API
//	gekka.metrics.scrape-interval              how often to fetch cluster state
//	gekka.telemetry.exporter.otlp.endpoint     OTLP/HTTP collector endpoint
//
// Usage:
//
//	gekka-metrics --config application.conf
//	gekka-metrics --url http://node1:8558 --interval 30s --otlp http://otel:4318
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/internal/management/client"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (optional)")
	flagURL := flag.String("url", "", "Management API base URL (overrides config)")
	flagInterval := flag.String("interval", "", "Scrape interval, e.g. 15s (overrides config)")
	flagOtlp := flag.String("otlp", "", "OTLP/HTTP collector endpoint, e.g. http://otel:4318 (overrides config)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// ── Resolve configuration ─────────────────────────────────────────────────

	managementURL := "http://127.0.0.1:8558"
	scrapeInterval := 15 * time.Second
	otlpEndpoint := ""

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
		otlpEndpoint = cfg.Telemetry.OtlpEndpoint
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
	if *flagOtlp != "" {
		otlpEndpoint = *flagOtlp
	}

	logger.Info("gekka-metrics starting",
		"management_url", managementURL,
		"scrape_interval", scrapeInterval,
		"otlp_endpoint", otlpEndpoint,
	)

	// ── OTEL SDK initialisation ───────────────────────────────────────────────

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mp, shutdown, err := initMeterProvider(ctx, otlpEndpoint)
	if err != nil {
		logger.Error("init meter provider", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(shutCtx); err != nil {
			logger.Error("meter provider shutdown", "error", err)
		}
	}()
	otel.SetMeterProvider(mp)

	// ── Metric instruments ────────────────────────────────────────────────────

	meter := mp.Meter("github.com/sopranoworks/gekka/cmd/gekka-metrics")

	// snapshot holds the latest scraped data read by the observable gauge callback.
	var snapshotMu sync.RWMutex
	var snapshot []client.MemberInfo

	// gekka.cluster.members is an observable (async) gauge that reports the
	// number of cluster members broken down by "status" and "dc" attributes.
	// The OTEL SDK calls the registered callback on every collection cycle.
	_, err = meter.Int64ObservableGauge(
		"gekka.cluster.members",
		otelmetric.WithDescription("Number of cluster members in each status/dc combination"),
		otelmetric.WithUnit("{members}"),
		otelmetric.WithInt64Callback(func(_ context.Context, obs otelmetric.Int64Observer) error {
			snapshotMu.RLock()
			members := snapshot
			snapshotMu.RUnlock()

			// Aggregate member counts by (status, dc).
			type groupKey struct{ status, dc string }
			counts := make(map[groupKey]int64)
			for _, m := range members {
				counts[groupKey{m.Status, m.DataCenter}]++
			}
			for k, n := range counts {
				obs.Observe(n,
					otelmetric.WithAttributes(
						attribute.String("status", k.status),
						attribute.String("dc", k.dc),
					),
				)
			}
			return nil
		}),
	)
	if err != nil {
		logger.Error("create gauge", "error", err)
		os.Exit(1)
	}

	// ── Scrape loop ───────────────────────────────────────────────────────────

	c := client.New(managementURL)
	ticker := time.NewTicker(scrapeInterval)
	defer ticker.Stop()

	// Initial scrape before the first tick so metrics are available immediately.
	doScrape(logger, c, &snapshotMu, &snapshot)

	for {
		select {
		case <-ticker.C:
			doScrape(logger, c, &snapshotMu, &snapshot)
		case <-ctx.Done():
			logger.Info("shutting down")
			return
		}
	}
}

// doScrape fetches cluster state, updates the snapshot used by the OTEL gauge
// callback, and logs a summary.
func doScrape(
	logger *slog.Logger,
	c *client.Client,
	mu *sync.RWMutex,
	snapshot *[]client.MemberInfo,
) {
	members, err := c.Members()
	if err != nil {
		logger.Error("scrape failed", "error", err)
		return
	}

	mu.Lock()
	*snapshot = members
	mu.Unlock()

	upCount := 0
	for _, m := range members {
		if m.Status == "Up" {
			upCount++
		}
	}
	logger.Info("cluster_members_up", "count", upCount, "total", len(members))
}

// initMeterProvider creates an sdkmetric.MeterProvider.
// When otlpEndpoint is non-empty a periodic OTLP/HTTP exporter is registered.
// When empty a ManualReader is used (metrics stay in-process; useful for
// local testing or when only the structured log output is needed).
func initMeterProvider(ctx context.Context, otlpEndpoint string) (*sdkmetric.MeterProvider, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("gekka-metrics")),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, nil, err
	}

	var readerOpt sdkmetric.Option
	if otlpEndpoint != "" {
		exp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(otlpEndpoint),
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			return nil, nil, err
		}
		readerOpt = sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp))
	} else {
		// No OTLP endpoint: use a ManualReader so the gauge callback is still
		// registered (and can be triggered in tests) without requiring a collector.
		readerOpt = sdkmetric.WithReader(sdkmetric.NewManualReader())
	}

	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), readerOpt)
	return mp, mp.Shutdown, nil
}
