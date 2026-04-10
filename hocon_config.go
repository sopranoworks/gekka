/*
 * hocon_config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"

	hocon "github.com/sopranoworks/gekka-config"
)

// LoadConfig reads a HOCON configuration file and converts it to a ClusterConfig.
//
// The following HOCON paths are recognised (replace "pekko" with "akka" for
// Lightbend Akka clusters — the protocol is auto-detected):
//
//	pekko.remote.artery.canonical.hostname  → Address.Host
//	pekko.remote.artery.canonical.port      → Address.Port
//	pekko.cluster.seed-nodes               → SeedNodes ([]actor.Address)
//
// The actor system name and protocol prefix are derived from the first
// seed-node URI (e.g. "pekko://ClusterSystem@127.0.0.1:2552").
//
// Optional fallback paths are loaded and merged with lower priority, which
// lets you layer a reference.conf under an application.conf:
//
//	cfg, err := gekka.LoadConfig("application.conf", "reference.conf")
//
// LoadConfig reads a HOCON configuration file and converts it to a ClusterConfig.
//
// The following HOCON paths are recognised (replace "pekko" with "akka" for
// Lightbend Akka clusters — the protocol is auto-detected):
//
//	pekko.remote.artery.canonical.hostname  → Address.Host
//	pekko.remote.artery.canonical.port      → Address.Port
//	pekko.cluster.seed-nodes               → SeedNodes ([]actor.Address)
//
// The actor system name and protocol prefix are derived from the first
// seed-node URI (e.g. "pekko://ClusterSystem@127.0.0.1:2552").
//
// Optional fallback paths are loaded and merged with lower priority, which
// lets you layer a reference.conf under an application.conf:
//
//	cfg, err := gekka.LoadConfig("application.conf", "reference.conf")
func LoadConfig(path string, fallbacks ...string) (ClusterConfig, error) {
	primaryData, err := os.ReadFile(path)
	if err != nil {
		return ClusterConfig{}, fmt.Errorf("gekka: read config %q: %w", path, err)
	}
	cfg, err := hocon.ParseString(string(primaryData))
	if err != nil {
		return ClusterConfig{}, fmt.Errorf("gekka: parse primary config: %w", err)
	}

	for _, fb := range fallbacks {
		data, err := os.ReadFile(fb)
		if err != nil {
			return ClusterConfig{}, fmt.Errorf("gekka: read fallback %q: %w", fb, err)
		}
		fallbackCfg, err := hocon.ParseString(string(data))
		if err != nil {
			return ClusterConfig{}, fmt.Errorf("gekka: parse fallback %q: %w", fb, err)
		}
		*cfg = cfg.WithFallback(*fallbackCfg)
	}

	return hoconToClusterConfig(cfg)
}

// NewClusterFromConfig is a convenience wrapper that calls LoadConfig then NewCluster.
// After NewClusterFromConfig, call node.JoinSeeds() to connect to the cluster.
//
//	node, err := gekka.NewClusterFromConfig("application.conf")
//	if err != nil { log.Fatal(err) }
//	defer node.Shutdown()
//	node.Join(...) // or node.JoinSeeds()
func NewClusterFromConfig(path string, fallbacks ...string) (*Cluster, error) {
	cfg, err := LoadConfig(path, fallbacks...)
	if err != nil {
		return nil, err
	}
	return NewCluster(cfg)
}

// ParseHOCONString parses an in-memory HOCON string and returns a ClusterConfig.
// Useful for embedding configuration in tests or when the config comes from
// a source other than a file (e.g. Kubernetes ConfigMap, etcd).
func ParseHOCONString(text string) (ClusterConfig, error) {
	return parseHOCONString(text)
}

func parseHOCONString(text string) (ClusterConfig, error) {
	cfg, err := hocon.ParseString(text)
	if err != nil {
		return ClusterConfig{}, fmt.Errorf("gekka: parse config: %w", err)
	}
	return hoconToClusterConfig(cfg)
}

// hoconToClusterConfig maps a parsed HOCON Config to a ClusterConfig.
func hoconToClusterConfig(cfg *hocon.Config) (ClusterConfig, error) {
	var nodeCfg ClusterConfig
	if err := cfg.Unmarshal(&nodeCfg); err != nil {
		return ClusterConfig{}, fmt.Errorf("gekka: unmarshal config: %w", err)
	}

	// Auto-detect protocol: prefer "pekko", fall back to "akka".
	proto := detectProtocol(cfg)
	prefix := proto // "pekko" or "akka"

	// Use the detected prefix for manual fallbacks if unmarshal didn't fill everything.
	if nodeCfg.Host == "" || nodeCfg.Host == "127.0.0.1" {
		if h, err := cfg.GetString(prefix + ".remote.artery.canonical.hostname"); err == nil {
			nodeCfg.Host = h
		}
	}
	if nodeCfg.Port == 0 {
		if p, err := cfg.GetInt(prefix + ".remote.artery.canonical.port"); err == nil {
			nodeCfg.Port = uint32(p)
		}
	}
	if nodeCfg.SystemName == "" {
		if s, err := cfg.GetString(prefix + ".actor.system-name"); err == nil {
			nodeCfg.SystemName = s
		}
	}

	// Provider
	if proto == "akka" {
		nodeCfg.Provider = ProviderAkka
	} else {
		nodeCfg.Provider = ProviderPekko
	}

	var seedURIs []string
	var tmp struct {
		PekkoSeeds []string `hocon:"pekko.cluster.seed-nodes"`
		AkkaSeeds  []string `hocon:"akka.cluster.seed-nodes"`
	}
	_ = cfg.Unmarshal(&tmp)
	if prefix == "pekko" {
		seedURIs = tmp.PekkoSeeds
	} else {
		seedURIs = tmp.AkkaSeeds
	}

	seeds := make([]actor.Address, 0, len(seedURIs))
	var systemName string
	for _, uri := range seedURIs {
		// seed-nodes entries may be quoted; strip surrounding quotes.
		uri = strings.Trim(uri, `"`)
		addr, err := actor.ParseAddress(uri)
		if err != nil {
			return ClusterConfig{}, fmt.Errorf("gekka: parse seed-node %q: %w", uri, err)
		}
		seeds = append(seeds, addr)
		if systemName == "" {
			systemName = addr.System
		}
	}
	nodeCfg.SeedNodes = seeds

	if nodeCfg.SystemName == "" && systemName != "" {
		nodeCfg.SystemName = systemName
	}
	if nodeCfg.SystemName == "" {
		nodeCfg.SystemName = "GekkaSystem"
	}

	if nodeCfg.Host == "" {
		nodeCfg.Host = "127.0.0.1"
	}

	nodeCfg.Address = actor.Address{
		Protocol: proto,
		System:   nodeCfg.SystemName,
		Host:     nodeCfg.Host,
		Port:     int(nodeCfg.Port),
	}

	// Extract deployment configs from the HOCON deployment block.
	if deps := core.ExtractDeployments(cfg); len(deps) > 0 {
		nodeCfg.Deployments = deps
	}

	// TLS transport configuration.
	arteryPrefix := prefix + ".remote.artery"
	if transport, err := cfg.GetString(arteryPrefix + ".transport"); err == nil {
		nodeCfg.Transport = transport
	}
	tlsPrefix := arteryPrefix + ".tls"
	if v, err := cfg.GetString(tlsPrefix + ".certificate"); err == nil {
		nodeCfg.TLS.CertFile = v
	}
	if v, err := cfg.GetString(tlsPrefix + ".private-key"); err == nil {
		nodeCfg.TLS.KeyFile = v
	}
	if v, err := cfg.GetString(tlsPrefix + ".ca-certificates"); err == nil {
		nodeCfg.TLS.CAFile = v
	}
	if v, err := cfg.GetString(tlsPrefix + ".min-version"); err == nil {
		switch strings.ToUpper(strings.TrimSpace(v)) {
		case "TLS1.3", "TLSV1.3":
			nodeCfg.TLS.MinVersion = 0x0304 // tls.VersionTLS13
		default:
			nodeCfg.TLS.MinVersion = 0x0303 // tls.VersionTLS12
		}
	}
	if v, err := cfg.GetString(tlsPrefix + ".require-client-auth"); err == nil {
		nodeCfg.TLS.RequireClientAuth = strings.EqualFold(strings.TrimSpace(v), "true")
	}
	if v, err := cfg.GetString(tlsPrefix + ".server-name"); err == nil {
		nodeCfg.TLS.ServerName = v
	}

	// ── Persistence plugins ─────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".persistence.journal.plugin"); err == nil {
		nodeCfg.Persistence.JournalPlugin = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(prefix + ".persistence.snapshot-store.plugin"); err == nil {
		nodeCfg.Persistence.SnapshotPlugin = strings.TrimSpace(v)
	}

	// ── Cluster Roles ───────────────────────────────────────────────────────
	var rolesTmp struct {
		PekkoRoles []string `hocon:"pekko.cluster.roles"`
		AkkaRoles  []string `hocon:"akka.cluster.roles"`
	}
	_ = cfg.Unmarshal(&rolesTmp)
	if prefix == "pekko" {
		nodeCfg.Roles = rolesTmp.PekkoRoles
	} else {
		nodeCfg.Roles = rolesTmp.AkkaRoles
	}

	// ── Multi-Data-Center ───────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.multi-data-center.self-data-center"); err == nil {
		nodeCfg.DataCenter = strings.TrimSpace(v)
	}
	if nodeCfg.DataCenter == "" {
		nodeCfg.DataCenter = "default"
	}
	if v, err := cfg.GetString(prefix + ".cluster.multi-data-center.cross-data-center-gossip-probability"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.CrossDataCenterGossipProbability = f
		}
	}

	// ── Cluster Sharding ────────────────────────────────────────────────────
	shardingPrefix := prefix + ".cluster.sharding"
	if v, err := cfg.GetString(shardingPrefix + ".passivation.idle-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Sharding.PassivationIdleTimeout = d
		}
	}
	if v, err := cfg.GetString(shardingPrefix + ".remember-entities"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Sharding.RememberEntities = v == "on" || v == "true"
	}
	if v, err := cfg.GetString(shardingPrefix + ".handoff-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Sharding.HandoffTimeout = d
		}
	}

	// ── Sharding Adaptive Rebalancing (gekka-native) ────────────────────────
	adaptivePrefix := "gekka.cluster.sharding.adaptive-rebalancing"
	if v, err := cfg.GetString(adaptivePrefix + ".enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Sharding.AdaptiveRebalancing.Enabled = v == "on" || v == "true"
	}
	if v, err := cfg.GetString(adaptivePrefix + ".load-weight"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.Sharding.AdaptiveRebalancing.LoadWeight = f
		}
	}
	if v, err := cfg.GetString(adaptivePrefix + ".cpu-weight"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.Sharding.AdaptiveRebalancing.CPUWeight = f
		}
	}
	if v, err := cfg.GetString(adaptivePrefix + ".memory-weight"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.Sharding.AdaptiveRebalancing.MemoryWeight = f
		}
	}
	if v, err := cfg.GetString(adaptivePrefix + ".mailbox-weight"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.Sharding.AdaptiveRebalancing.MailboxWeight = f
		}
	}
	if v, err := cfg.GetString(adaptivePrefix + ".rebalance-threshold"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.Sharding.AdaptiveRebalancing.RebalanceThreshold = f
		}
	}
	if v, err := cfg.GetInt(adaptivePrefix + ".max-simultaneous-rebalance"); err == nil {
		nodeCfg.Sharding.AdaptiveRebalancing.MaxSimultaneousRebalance = v
	}

	// ── Failure Detector (gekka-native) ─────────────────────────────────────
	fdPrefix := "gekka.cluster.failure-detector"
	if v, err := cfg.GetString(fdPrefix + ".threshold"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.FailureDetector.Threshold = f
		}
	}
	if v, err := cfg.GetInt(fdPrefix + ".max-sample-size"); err == nil {
		nodeCfg.FailureDetector.MaxSampleSize = v
	}
	if v, err := cfg.GetString(fdPrefix + ".min-std-deviation"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.FailureDetector.MinStdDeviation = d
		}
	}

	// ── Internal SBR Strategy (gekka-native) ────────────────────────────────
	iSBRPrefix := "gekka.cluster.split-brain-resolver"
	if v, err := cfg.GetString(iSBRPrefix + ".active-strategy"); err == nil {
		nodeCfg.InternalSBR.ActiveStrategy = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(iSBRPrefix + ".stable-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.InternalSBR.StableAfter = d
		}
	}
	if v, err := cfg.GetInt(iSBRPrefix + ".static-quorum.size"); err == nil {
		nodeCfg.InternalSBR.QuorumSize = v
	}
	if v, err := cfg.GetString(iSBRPrefix + ".keep-oldest.role"); err == nil {
		nodeCfg.InternalSBR.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(iSBRPrefix + ".keep-oldest.down-if-alone"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.InternalSBR.DownIfAlone = v == "on" || v == "true"
	}

	// ── Split Brain Resolver ────────────────────────────────────────────────
	sbrPrefix := prefix + ".cluster.split-brain-resolver"
	if v, err := cfg.GetString(sbrPrefix + ".active-strategy"); err == nil {
		nodeCfg.SBR.ActiveStrategy = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(sbrPrefix + ".stable-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.SBR.StableAfter = d
		}
	}
	if v, err := cfg.GetString(sbrPrefix + ".keep-majority.role"); err == nil {
		nodeCfg.SBR.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(sbrPrefix + ".keep-oldest.role"); err == nil && nodeCfg.SBR.Role == "" {
		nodeCfg.SBR.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(sbrPrefix + ".keep-oldest.down-if-alone"); err == nil {
		nodeCfg.SBR.DownIfAlone = strings.EqualFold(strings.TrimSpace(v), "on") ||
			strings.EqualFold(strings.TrimSpace(v), "true")
	}
	if v, err := cfg.GetString(sbrPrefix + ".keep-referee.referee"); err == nil {
		nodeCfg.SBR.RefereeAddress = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(sbrPrefix + ".static-quorum.quorum-size"); err == nil {
		nodeCfg.SBR.QuorumSize = v
	}

	// Unmarshal Management and Metrics configs from HOCON.
	// cfg.Unmarshal is used as a first pass, but bool fields inside nested
	// structs are not reliably populated by the HOCON library.  Explicit
	// GetString calls below mirror the pattern used for Discovery / Telemetry.
	nodeCfg.Management = core.DefaultManagementConfig()
	nodeCfg.Metrics = core.DefaultMetricsExporterConfig()
	_ = cfg.Unmarshal(&nodeCfg)

	// ── Management HTTP API ───────────────────────────────────────────────────
	mgmtPrefix := "gekka.management.http"

	// Task 1: Auto-enable Management Server if port or hostname is defined.
	// We check for presence in HOCON to establish the default, which can
	// still be overridden by an explicit .enabled = false.
	_, errH := cfg.GetString(mgmtPrefix + ".hostname")
	_, errP := cfg.GetInt(mgmtPrefix + ".port")
	if errH == nil || errP == nil {
		nodeCfg.Management.Enabled = true
	}

	if v, err := cfg.GetString(mgmtPrefix + ".enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Management.Enabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString(mgmtPrefix + ".hostname"); err == nil {
		if v = strings.TrimSpace(v); v != "" {
			nodeCfg.Management.Hostname = v
		}
	}
	if v, err := cfg.GetInt(mgmtPrefix + ".port"); err == nil && v > 0 {
		nodeCfg.Management.Port = v
	}
	if v, err := cfg.GetString(mgmtPrefix + ".health-checks.enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Management.HealthChecksEnabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString("gekka.management.debug.enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Management.DebugEnabled = v == "true" || v == "on"
	}

	// ── Telemetry ────────────────────────────────────────────────────────────
	if v, err := cfg.GetString("gekka.telemetry.tracing.enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Telemetry.TracingEnabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString("gekka.telemetry.metrics.enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Telemetry.MetricsEnabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString("gekka.telemetry.exporter.otlp.endpoint"); err == nil {
		nodeCfg.Telemetry.OtlpEndpoint = strings.TrimSpace(v)
	}

	// ── Discovery ────────────────────────────────────────────────────────────
	discoveryPrefix := "gekka.cluster.discovery"
	if v, err := cfg.GetString(discoveryPrefix + ".enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Discovery.Enabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString(discoveryPrefix + ".type"); err == nil {
		nodeCfg.Discovery.Type = strings.TrimSpace(v)
	}

	if nodeCfg.Discovery.Config.Config == nil {
		nodeCfg.Discovery.Config.Config = make(map[string]any)
	}
	if configObj, err := cfg.GetConfig(discoveryPrefix + ".config"); err == nil {
		_ = configObj.Unmarshal(&nodeCfg.Discovery.Config.Config)
	}

	// ── Distributed Data ─────────────────────────────────────────────────────
	ddataPrefix := "gekka.cluster.distributed-data"
	if v, err := cfg.GetString(ddataPrefix + ".enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.DistributedData.Enabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString(ddataPrefix + ".gossip-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.DistributedData.GossipInterval = d
		}
	}

	// Support for specific blocks (v0.9.0)
	apiPrefix := discoveryPrefix + ".kubernetes-api"
	if v, err := cfg.GetString(apiPrefix + ".namespace"); err == nil {
		nodeCfg.Discovery.Config.Config["namespace"] = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(apiPrefix + ".label-selector"); err == nil {
		nodeCfg.Discovery.Config.Config["label-selector"] = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(apiPrefix + ".port"); err == nil {
		nodeCfg.Discovery.Config.Config["port"] = v
	}

	dnsPrefix := discoveryPrefix + ".kubernetes-dns"
	if v, err := cfg.GetString(dnsPrefix + ".service-name"); err == nil {
		nodeCfg.Discovery.Config.Config["service-name"] = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(dnsPrefix + ".port"); err == nil {
		// Only override if not already set by apiPrefix or config block (though port is usually distinct)
		nodeCfg.Discovery.Config.Config["port"] = v
	}

	// ── Dispatcher Configuration ────────────────────────────────────────────
	extractDispatchers(cfg, prefix)

	// Preserve the raw config so NewCluster can call LoadFromConfig for
	// user-defined serializers declared under pekko.actor.serializers.
	nodeCfg.HOCON = cfg

	return nodeCfg, nil
}

// extractDispatchers reads dispatcher definitions from the HOCON config
// and registers them with the actor package's dispatcher registry.
//
// HOCON format:
//
//	pekko.dispatchers {
//	  my-dispatcher {
//	    type = "pinned-dispatcher"
//	    throughput = 1
//	  }
//	}
func extractDispatchers(cfg *hocon.Config, prefix string) {
	dispPrefix := prefix + ".dispatchers"

	// Try to enumerate known dispatcher keys. The HOCON library doesn't
	// expose key iteration, so we check the sub-config for each entry.
	sub, err := cfg.GetConfig(dispPrefix)
	if err != nil {
		return
	}

	// Iterate over sub-config keys.
	for _, key := range sub.Keys() {
		dcfg := actor.DispatcherConfig{}
		path := dispPrefix + "." + key
		if v, e := cfg.GetString(path + ".type"); e == nil {
			dcfg.Type = strings.TrimSpace(v)
		} else {
			dcfg.Type = key // default: use the key name as the type
		}
		if v, e := cfg.GetInt(path + ".throughput"); e == nil {
			dcfg.Throughput = v
		}
		if v, e := cfg.GetString(path + ".mailbox-type"); e == nil && v != "" {
			dcfg.MailboxType = strings.TrimSpace(v)
		}
		actor.RegisterDispatcherConfig(key, dcfg)
	}
}

// parseHOCONDuration parses a Pekko/HOCON duration string such as "20s", "5 seconds",
// "500ms", "1 minute" into a time.Duration. Returns 0, error on parse failure.
func parseHOCONDuration(s string) (time.Duration, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	// Handle unit aliases: "seconds"→"s", "minutes"→"m", etc.
	replacer := strings.NewReplacer(
		" seconds", "s", " second", "s", "seconds", "s", "second", "s",
		" minutes", "m", " minute", "m", "minutes", "m", "minute", "m",
		" milliseconds", "ms", " millisecond", "ms", "milliseconds", "ms", "millisecond", "ms",
		" hours", "h", " hour", "h", "hours", "h", "hour", "h",
	)
	normalized := strings.TrimSpace(replacer.Replace(s))
	d, err := time.ParseDuration(normalized)
	if err != nil {
		return 0, fmt.Errorf("parseHOCONDuration: cannot parse %q: %w", s, err)
	}
	return d, nil
}

// detectProtocol returns "pekko" or "akka" by checking which top-level key
// is present in the config. It prefers "pekko" if both are present.
func detectProtocol(cfg *hocon.Config) string {
	if _, err := cfg.GetString("pekko.remote.artery.canonical.hostname"); err == nil {
		return "pekko"
	}
	if _, err := cfg.GetString("pekko.cluster.seed-nodes"); err == nil {
		return "pekko"
	}
	if _, err := cfg.GetString("pekko.actor.provider"); err == nil {
		return "pekko"
	}

	if _, err := cfg.GetString("akka.remote.artery.canonical.hostname"); err == nil {
		return "akka"
	}
	if _, err := cfg.GetString("akka.cluster.seed-nodes"); err == nil {
		return "akka"
	}
	if _, err := cfg.GetString("akka.actor.provider"); err == nil {
		return "akka"
	}
	return "pekko"
}
