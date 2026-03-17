/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// gekka-metrics joins the cluster as a dedicated monitoring node and exports
// cluster-state metrics via the OpenTelemetry Protocol (OTLP/HTTP).
//
// Unlike a sidecar that polls the Management HTTP API, gekka-metrics participates
// in the gossip protocol directly.  This gives it a live, first-class view of
// cluster membership without depending on any management endpoint.
//
// The node joins with the "metrics-exporter" role so that production workloads
// (sharding, singletons) can exclude it via role-based allocation.
//
// Configuration is loaded from a HOCON application.conf.  The --otlp flag
// overrides gekka.telemetry.exporter.otlp.endpoint from config.
//
// HOCON keys consumed:
//
//	pekko.remote.artery.canonical.hostname  this node's advertised host
//	pekko.remote.artery.canonical.port      this node's listen port
//	pekko.cluster.seed-nodes               seed nodes to join
//	gekka.telemetry.exporter.otlp.endpoint OTLP/HTTP collector endpoint
//
// Usage:
//
//	gekka-metrics --config application.conf
//	gekka-metrics --config application.conf --otlp http://otel:4318
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	gcluster "github.com/sopranoworks/gekka/cluster"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (required)")
	flagOtlp := flag.String("otlp", "", "OTLP/HTTP collector endpoint, e.g. http://otel:4318 (overrides config)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if *flagConfig == "" {
		logger.Error("--config is required")
		os.Exit(1)
	}

	// ── Load HOCON config and inject metrics-exporter role ────────────────────

	cfg, err := gekka.LoadConfig(*flagConfig)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	// Add the "metrics-exporter" role so sharding/singleton allocators can
	// exclude this node.  Preserve any roles already declared in the config.
	cfg.Roles = appendIfMissing(cfg.Roles, "metrics-exporter")

	otlpEndpoint := cfg.Telemetry.OtlpEndpoint
	if *flagOtlp != "" {
		otlpEndpoint = *flagOtlp
	}

	logger.Info("gekka-metrics starting",
		"host", cfg.Host,
		"port", cfg.Port,
		"roles", cfg.Roles,
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

	// ── Join the cluster ──────────────────────────────────────────────────────

	node, err := gekka.NewCluster(cfg)
	if err != nil {
		logger.Error("create cluster node", "error", err)
		os.Exit(1)
	}
	defer node.Shutdown()

	if err := node.JoinSeeds(); err != nil {
		logger.Error("join cluster", "error", err)
		os.Exit(1)
	}
	logger.Info("joined cluster, waiting for gossip convergence")

	// ── Register OTEL gauge ───────────────────────────────────────────────────

	meter := mp.Meter("github.com/sopranoworks/gekka/cmd/gekka-metrics")

	cm := node.ClusterManager()

	// gekka.cluster.members is an observable (async) gauge that reports the
	// number of cluster members broken down by "status" and "dc" attributes.
	// The OTEL SDK calls the registered callback on every collection cycle.
	// The callback reads gossip state directly — no HTTP round-trip required.
	_, err = meter.Int64ObservableGauge(
		"gekka.cluster.members",
		otelmetric.WithDescription("Number of cluster members in each status/dc combination"),
		otelmetric.WithUnit("{members}"),
		otelmetric.WithInt64Callback(func(_ context.Context, obs otelmetric.Int64Observer) error {
			cm.Mu.RLock()
			gossip := cm.State
			cm.Mu.RUnlock()

			if gossip == nil {
				return nil
			}

			type groupKey struct{ status, dc string }
			counts := make(map[groupKey]int64)
			for _, m := range gossip.GetMembers() {
				status := statusString(m.GetStatus().String())
				dc := gcluster.DataCenterForMember(gossip, m)
				counts[groupKey{status, dc}]++
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

	// ── Periodic status log ───────────────────────────────────────────────────

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logClusterState(logger, cm)
		case <-ctx.Done():
			logger.Info("shutting down")
			return
		}
	}
}

// logClusterState logs a brief summary of current gossip state.
func logClusterState(logger *slog.Logger, cm *gcluster.ClusterManager) {
	cm.Mu.RLock()
	gossip := cm.State
	cm.Mu.RUnlock()

	if gossip == nil {
		return
	}

	total := len(gossip.GetMembers())
	upCount := 0
	for _, m := range gossip.GetMembers() {
		if m.GetStatus().String() == "Up" {
			upCount++
		}
	}
	logger.Info("cluster_state", "up", upCount, "total", total)
}

// statusString normalises the proto enum name to a short status string.
// Proto generates names like "Up", "Joining", "WeaklyUp", etc.
func statusString(s string) string {
	return strings.TrimPrefix(s, "MemberStatus_")
}

// appendIfMissing appends role to roles only if it is not already present.
func appendIfMissing(roles []string, role string) []string {
	for _, r := range roles {
		if r == role {
			return roles
		}
	}
	return append(roles, role)
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
