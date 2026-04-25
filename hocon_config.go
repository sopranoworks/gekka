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
	"log/slog"
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

	// Log level: pekko.loglevel (or akka.loglevel), fallback gekka.logging.level
	if nodeCfg.LogLevel == "" {
		if v, err := cfg.GetString(prefix + ".loglevel"); err == nil {
			nodeCfg.LogLevel = strings.TrimSpace(v)
		}
	}
	if nodeCfg.LogLevel == "" {
		if v, err := cfg.GetString("gekka.logging.level"); err == nil {
			nodeCfg.LogLevel = strings.TrimSpace(v)
		}
	}

	// Dead letter logging: pekko.log-dead-letters (default: 10)
	nodeCfg.LogDeadLetters = 10 // default
	if v, err := cfg.GetString(prefix + ".log-dead-letters"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "off", "false":
			nodeCfg.LogDeadLetters = 0
		case "on", "true":
			nodeCfg.LogDeadLetters = -1 // unlimited
		default:
			if n, parseErr := strconv.Atoi(v); parseErr == nil {
				nodeCfg.LogDeadLetters = n
			}
		}
	}

	// Dead letter logging during shutdown: pekko.log-dead-letters-during-shutdown (default: off)
	if v, err := cfg.GetString(prefix + ".log-dead-letters-during-shutdown"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.LogDeadLettersDuringShutdown = v == "on" || v == "true"
	}

	// Accept protocol names: pekko.remote.accept-protocol-names (default: ["pekko", "akka"])
	nodeCfg.AcceptProtocolNames = []string{"pekko", "akka"} // gekka default for compat
	{
		var apnTmp struct {
			PekkoNames []string `hocon:"pekko.remote.accept-protocol-names"`
			AkkaNames  []string `hocon:"akka.remote.accept-protocol-names"`
		}
		_ = cfg.Unmarshal(&apnTmp)
		if prefix == "pekko" && len(apnTmp.PekkoNames) > 0 {
			nodeCfg.AcceptProtocolNames = apnTmp.PekkoNames
		} else if prefix == "akka" && len(apnTmp.AkkaNames) > 0 {
			nodeCfg.AcceptProtocolNames = apnTmp.AkkaNames
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

	// ── Flight Recorder ─────────────────────────────────────────────────────
	frPrefix := arteryPrefix + ".advanced.flight-recorder"
	nodeCfg.FlightRecorder.Enabled = true // default on
	if v, err := cfg.GetString(frPrefix + ".enabled"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.FlightRecorder.Enabled = v == "true" || v == "on"
	}
	if v, err := cfg.GetString(frPrefix + ".level"); err == nil {
		nodeCfg.FlightRecorder.Level = strings.TrimSpace(v)
	}

	// ── Persistence plugins ─────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".persistence.journal.plugin"); err == nil {
		nodeCfg.Persistence.JournalPlugin = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(prefix + ".persistence.snapshot-store.plugin"); err == nil {
		nodeCfg.Persistence.SnapshotPlugin = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(prefix + ".persistence.max-concurrent-recoveries"); err == nil && v > 0 {
		nodeCfg.Persistence.MaxConcurrentRecoveries = v
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
	if v, err := cfg.GetInt(prefix + ".cluster.multi-data-center.cross-data-center-connections"); err == nil {
		nodeCfg.CrossDataCenterConnections = v
	}

	// ── Cluster Sharding ────────────────────────────────────────────────────
	shardingPrefix := prefix + ".cluster.sharding"
	// Passivation: correct Pekko path only (no fallback to legacy .passivation.idle-timeout)
	if v, err := cfg.GetString(shardingPrefix + ".passivation.default-idle-strategy.idle-entity.timeout"); err == nil {
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
	if v, err := cfg.GetInt(shardingPrefix + ".number-of-shards"); err == nil {
		nodeCfg.Sharding.NumberOfShards = v
	}
	if v, err := cfg.GetString(shardingPrefix + ".role"); err == nil {
		nodeCfg.Sharding.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(shardingPrefix + ".guardian-name"); err == nil {
		nodeCfg.Sharding.GuardianName = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(shardingPrefix + ".remember-entities-store"); err == nil {
		nodeCfg.Sharding.RememberEntitiesStore = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(shardingPrefix + ".passivation.strategy"); err == nil {
		nodeCfg.Sharding.PassivationStrategy = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(shardingPrefix + ".passivation.custom-lru-strategy.active-entity-limit"); err == nil {
		nodeCfg.Sharding.PassivationActiveEntityLimit = v
	}
	if v, err := cfg.GetString(shardingPrefix + ".rebalance-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Sharding.RebalanceInterval = d
		}
	}
	leastPrefix := shardingPrefix + ".least-shard-allocation-strategy"
	if v, err := cfg.GetInt(leastPrefix + ".rebalance-threshold"); err == nil {
		nodeCfg.Sharding.LeastShardAllocation.RebalanceThreshold = v
	}
	if v, err := cfg.GetInt(leastPrefix + ".max-simultaneous-rebalance"); err == nil {
		nodeCfg.Sharding.LeastShardAllocation.MaxSimultaneousRebalance = v
	}

	// pekko.cluster.sharding.distributed-data.* — sharding-specific replicator overrides.
	shardingDDataPrefix := shardingPrefix + ".distributed-data"
	if v, err := cfg.GetInt(shardingDDataPrefix + ".majority-min-cap"); err == nil {
		nodeCfg.Sharding.DistributedData.MajorityMinCap = v
	}
	if v, err := cfg.GetInt(shardingDDataPrefix + ".max-delta-elements"); err == nil {
		nodeCfg.Sharding.DistributedData.MaxDeltaElements = v
	}
	if v, err := cfg.GetString(shardingDDataPrefix + ".prefer-oldest"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.Sharding.DistributedData.PreferOldest = v == "on" || v == "true"
	}
	var shardingDDataKeys struct {
		PekkoKeys []string `hocon:"pekko.cluster.sharding.distributed-data.durable.keys"`
		AkkaKeys  []string `hocon:"akka.cluster.sharding.distributed-data.durable.keys"`
	}
	_ = cfg.Unmarshal(&shardingDDataKeys)
	if prefix == "pekko" && len(shardingDDataKeys.PekkoKeys) > 0 {
		nodeCfg.Sharding.DistributedData.DurableKeys = shardingDDataKeys.PekkoKeys
	} else if prefix == "akka" && len(shardingDDataKeys.AkkaKeys) > 0 {
		nodeCfg.Sharding.DistributedData.DurableKeys = shardingDDataKeys.AkkaKeys
	}

	// pekko.cluster.sharding.coordinator-singleton.* — singleton-manager
	// settings for the coordinator. Mirrors pekko.cluster.singleton layout.
	csPrefix := shardingPrefix + ".coordinator-singleton"
	if v, err := cfg.GetString(csPrefix + ".role"); err == nil {
		nodeCfg.Sharding.CoordinatorSingleton.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(csPrefix + ".hand-over-retry-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Sharding.CoordinatorSingleton.HandOverRetryInterval = d
		}
	}
	if v, err := cfg.GetString(csPrefix + ".singleton-name"); err == nil {
		nodeCfg.Sharding.CoordinatorSingleton.SingletonName = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(csPrefix + ".min-number-of-hand-over-retries"); err == nil {
		nodeCfg.Sharding.CoordinatorSingleton.MinNumberOfHandOverRetries = v
	}

	// coordinator-singleton-role-override defaults to true (Pekko default: on).
	nodeCfg.Sharding.CoordinatorSingletonRoleOverride = true
	if v, err := cfg.GetString(shardingPrefix + ".coordinator-singleton-role-override"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "off" || v == "false" {
			nodeCfg.Sharding.CoordinatorSingletonRoleOverride = false
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

	// ── Cluster Singleton ──────────────────────────────────────────────────
	singletonPrefix := prefix + ".cluster.singleton"
	if v, err := cfg.GetString(singletonPrefix + ".role"); err == nil {
		nodeCfg.Singleton.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(singletonPrefix + ".hand-over-retry-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Singleton.HandOverRetryInterval = d
		}
	}
	if v, err := cfg.GetString(singletonPrefix + ".singleton-name"); err == nil {
		nodeCfg.Singleton.SingletonName = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(singletonPrefix + ".min-number-of-hand-over-retries"); err == nil {
		nodeCfg.Singleton.MinNumberOfHandOverRetries = v
	}

	// ── Cluster Singleton Proxy ────────────────────────────────────────────
	singletonProxyPrefix := prefix + ".cluster.singleton-proxy"
	if v, err := cfg.GetString(singletonProxyPrefix + ".singleton-identification-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.SingletonProxy.SingletonIdentificationInterval = d
		}
	}
	if v, err := cfg.GetInt(singletonProxyPrefix + ".buffer-size"); err == nil {
		nodeCfg.SingletonProxy.BufferSize = v
	}
	if v, err := cfg.GetString(singletonProxyPrefix + ".singleton-name"); err == nil {
		nodeCfg.SingletonProxy.SingletonName = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(singletonProxyPrefix + ".role"); err == nil {
		nodeCfg.SingletonProxy.Role = strings.TrimSpace(v)
	}

	// ── Failure Detector ────────────────────────────────────────────────────
	// Parse pekko/akka namespace first (standard Pekko config), then fall back
	// to gekka-native namespace for any fields not yet set.
	pekkoFdPrefix := prefix + ".cluster.failure-detector"
	if v, err := cfg.GetString(pekkoFdPrefix + ".threshold"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.FailureDetector.Threshold = f
		}
	}
	// Pekko also supports "phi-threshold" as an alias
	if nodeCfg.FailureDetector.Threshold == 0 {
		if v, err := cfg.GetString(pekkoFdPrefix + ".phi-threshold"); err == nil {
			if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
				nodeCfg.FailureDetector.Threshold = f
			}
		}
	}
	if v, err := cfg.GetInt(pekkoFdPrefix + ".max-sample-size"); err == nil {
		nodeCfg.FailureDetector.MaxSampleSize = v
	}
	if v, err := cfg.GetString(pekkoFdPrefix + ".min-std-deviation"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.FailureDetector.MinStdDeviation = d
		}
	}
	if v, err := cfg.GetString(pekkoFdPrefix + ".heartbeat-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.FailureDetector.HeartbeatInterval = d
		}
	}
	if v, err := cfg.GetString(pekkoFdPrefix + ".acceptable-heartbeat-pause"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.FailureDetector.AcceptableHeartbeatPause = d
		}
	}
	if v, err := cfg.GetString(pekkoFdPrefix + ".expected-response-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.FailureDetector.ExpectedResponseAfter = d
		}
	}
	if v, err := cfg.GetInt(pekkoFdPrefix + ".monitored-by-nr-of-members"); err == nil {
		nodeCfg.FailureDetector.MonitoredByNrOfMembers = v
	}

	// Fallback: gekka-native namespace (lower priority)
	fdPrefix := "gekka.cluster.failure-detector"
	if nodeCfg.FailureDetector.Threshold == 0 {
		if v, err := cfg.GetString(fdPrefix + ".threshold"); err == nil {
			if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
				nodeCfg.FailureDetector.Threshold = f
			}
		}
	}
	if nodeCfg.FailureDetector.MaxSampleSize == 0 {
		if v, err := cfg.GetInt(fdPrefix + ".max-sample-size"); err == nil {
			nodeCfg.FailureDetector.MaxSampleSize = v
		}
	}
	if nodeCfg.FailureDetector.MinStdDeviation == 0 {
		if v, err := cfg.GetString(fdPrefix + ".min-std-deviation"); err == nil {
			if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
				nodeCfg.FailureDetector.MinStdDeviation = d
			}
		}
	}

	// ── Maximum Frame Size ─────────────────────────────────────────────────
	if v, err := cfg.GetString(arteryPrefix + ".advanced.maximum-frame-size"); err == nil {
		if size, parseErr := parseHOCONByteSize(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.MaxFrameSize = size
		}
	}

	// ── Artery Advanced: lanes + queue sizes ──────────────────────────────
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.inbound-lanes"); err == nil {
		nodeCfg.ArteryAdvanced.InboundLanes = v
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.outbound-lanes"); err == nil {
		nodeCfg.ArteryAdvanced.OutboundLanes = v
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.outbound-message-queue-size"); err == nil {
		nodeCfg.ArteryAdvanced.OutboundMessageQueueSize = v
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.system-message-buffer-size"); err == nil {
		nodeCfg.ArteryAdvanced.SystemMessageBufferSize = v
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.outbound-control-queue-size"); err == nil {
		nodeCfg.ArteryAdvanced.OutboundControlQueueSize = v
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.handshake-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.HandshakeTimeout = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.handshake-retry-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.HandshakeRetryInterval = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.system-message-resend-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.SystemMessageResendInterval = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.give-up-system-message-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.GiveUpSystemMessageAfter = d
		}
	}
	// ── Artery Advanced: quarantine + lifecycle timers ───────────────────
	if v, err := cfg.GetString(arteryPrefix + ".advanced.stop-idle-outbound-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.StopIdleOutboundAfter = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.quarantine-idle-outbound-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.QuarantineIdleOutboundAfter = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.stop-quarantined-after-idle"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.StopQuarantinedAfterIdle = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.remove-quarantined-association-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.RemoveQuarantinedAssociationAfter = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.shutdown-flush-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.ShutdownFlushTimeout = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.death-watch-notification-flush-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.DeathWatchNotificationFlushTimeout = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.inbound-restart-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.InboundRestartTimeout = d
		}
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.inbound-max-restarts"); err == nil {
		nodeCfg.ArteryAdvanced.InboundMaxRestarts = v
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.outbound-restart-backoff"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.OutboundRestartBackoff = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.outbound-restart-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.OutboundRestartTimeout = d
		}
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.outbound-max-restarts"); err == nil {
		nodeCfg.ArteryAdvanced.OutboundMaxRestarts = v
	}

	// ── Artery Advanced: compression + TCP + buffers (round2 session 04) ──
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.compression.actor-refs.max"); err == nil {
		nodeCfg.ArteryAdvanced.CompressionActorRefsMax = v
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.compression.actor-refs.advertisement-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.CompressionActorRefsAdvertisementInterval = d
		}
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.compression.manifests.max"); err == nil {
		nodeCfg.ArteryAdvanced.CompressionManifestsMax = v
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.compression.manifests.advertisement-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.CompressionManifestsAdvertisementInterval = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.tcp.connection-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.TcpConnectionTimeout = d
		}
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.tcp.outbound-client-hostname"); err == nil {
		nodeCfg.ArteryAdvanced.TcpOutboundClientHostname = strings.TrimSpace(v)
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.buffer-pool-size"); err == nil {
		nodeCfg.ArteryAdvanced.BufferPoolSize = v
	}
	if v, err := cfg.GetString(arteryPrefix + ".advanced.maximum-large-frame-size"); err == nil {
		if size, parseErr := parseHOCONByteSize(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ArteryAdvanced.MaximumLargeFrameSize = size
		}
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.large-buffer-pool-size"); err == nil {
		nodeCfg.ArteryAdvanced.LargeBufferPoolSize = v
	}
	if v, err := cfg.GetInt(arteryPrefix + ".advanced.outbound-large-message-queue-size"); err == nil {
		nodeCfg.ArteryAdvanced.OutboundLargeMessageQueueSize = v
	}

	// ── Bind Address (NAT/Docker support) ──────────────────────────────────
	if v, err := cfg.GetString(arteryPrefix + ".bind.hostname"); err == nil {
		nodeCfg.BindHostname = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(arteryPrefix + ".bind.port"); err == nil {
		v = strings.TrimSpace(v)
		if v != "" {
			if p, parseErr := strconv.Atoi(v); parseErr == nil && p > 0 {
				nodeCfg.BindPort = uint32(p)
			}
		}
	}

	// ── App Version ─────────────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.app-version"); err == nil {
		nodeCfg.AppVersion = strings.TrimSpace(v)
	}

	// ── Coordinated Shutdown When Down ──────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.run-coordinated-shutdown-when-down"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		b := v == "on" || v == "true"
		nodeCfg.RunCoordinatedShutdownWhenDown = &b
	}

	// ── Down Removal Margin ────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.down-removal-margin"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "off" && v != "" {
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				nodeCfg.DownRemovalMargin = d
			}
		}
	}

	// ── Seed Node Timeout ──────────────────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.seed-node-timeout"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.SeedNodeTimeout = d
		}
	}

	// ── Configuration Compatibility Check ──────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.configuration-compatibility-check.enforce-on-join"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		b := v == "on" || v == "true"
		nodeCfg.ConfigCompatCheck.EnforceOnJoin = &b
	}

	// ── Quarantine Removed Node After ───────────────────────────────────────
	if v, err := cfg.GetString(prefix + ".cluster.quarantine-removed-node-after"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "off" && v != "" {
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				nodeCfg.QuarantineRemovedNodeAfter = d
			}
		}
	}

	// ── Cluster Timing ──────────────────────────────────────────────────────
	if v, err := cfg.GetInt(prefix + ".cluster.min-nr-of-members"); err == nil {
		nodeCfg.MinNrOfMembers = v
	}
	if v, err := cfg.GetString(prefix + ".cluster.retry-unsuccessful-join-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.RetryUnsuccessfulJoinAfter = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.gossip-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.GossipInterval = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.leader-actions-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.LeaderActionsInterval = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.periodic-tasks-initial-delay"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.PeriodicTasksInitialDelay = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.shutdown-after-unsuccessful-join-seed-nodes"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "off" && v != "" {
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				nodeCfg.ShutdownAfterUnsuccessfulJoinSeedNodes = d
			}
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.log-info"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		b := v == "on" || v == "true"
		nodeCfg.LogInfo = &b
	}
	if v, err := cfg.GetString(prefix + ".cluster.log-info-verbose"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.LogInfoVerbose = v == "on" || v == "true"
	}
	if v, err := cfg.GetString(prefix + ".cluster.allow-weakly-up-members"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "off" || v == "false" {
			// Explicit zero disables WeaklyUp
			nodeCfg.AllowWeaklyUpMembers = 0
		} else {
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				nodeCfg.AllowWeaklyUpMembers = d
			}
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.gossip-different-view-probability"); err == nil {
		if f, parseErr := strconv.ParseFloat(strings.TrimSpace(v), 64); parseErr == nil {
			nodeCfg.GossipDifferentViewProbability = f
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.reduce-gossip-different-view-probability"); err == nil {
		if n, parseErr := strconv.Atoi(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ReduceGossipDifferentViewProbability = n
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.gossip-time-to-live"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.GossipTimeToLive = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.prune-gossip-tombstones-after"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.PruneGossipTombstonesAfter = d
		}
	}
	if v, err := cfg.GetString(prefix + ".cluster.unreachable-nodes-reaper-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.UnreachableNodesReaperInterval = d
		}
	}

	// ── Pub-Sub ────────────────────────────────────────────────────────────
	pubSubPrefix := prefix + ".cluster.pub-sub"
	if v, err := cfg.GetString(pubSubPrefix + ".gossip-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.PubSub.GossipInterval = d
		}
	}
	if v, err := cfg.GetString(pubSubPrefix + ".name"); err == nil {
		nodeCfg.PubSub.Name = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(pubSubPrefix + ".role"); err == nil {
		nodeCfg.PubSub.Role = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(pubSubPrefix + ".routing-logic"); err == nil {
		nodeCfg.PubSub.RoutingLogic = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(pubSubPrefix + ".removed-time-to-live"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.PubSub.RemovedTimeToLive = d
		}
	}
	if v, err := cfg.GetInt(pubSubPrefix + ".max-delta-elements"); err == nil {
		nodeCfg.PubSub.MaxDeltaElements = v
	}
	if v, err := cfg.GetString(pubSubPrefix + ".send-to-dead-letters-when-no-subscribers"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		nodeCfg.PubSub.SendToDeadLettersWhenNoSubscribers = v != "off" && v != "false"
	}

	// ── Cluster Client ─────────────────────────────────────────────────────
	// pekko.cluster.client.* — parsed into ClusterConfig.ClusterClient (re-export
	// of cluster/client.Config). Defaults match Pekko reference.conf.
	nodeCfg.ClusterClient.EstablishingGetContactsInterval = 3 * time.Second
	nodeCfg.ClusterClient.RefreshContactsInterval = 60 * time.Second
	nodeCfg.ClusterClient.HeartbeatInterval = 2 * time.Second
	nodeCfg.ClusterClient.AcceptableHeartbeatPause = 13 * time.Second
	nodeCfg.ClusterClient.BufferSize = 1000
	{
		var clientTmp struct {
			Pekko []string `hocon:"pekko.cluster.client.initial-contacts"`
			Akka  []string `hocon:"akka.cluster.client.initial-contacts"`
		}
		_ = cfg.Unmarshal(&clientTmp)
		var contacts []string
		if prefix == "pekko" && len(clientTmp.Pekko) > 0 {
			contacts = clientTmp.Pekko
		} else if prefix == "akka" && len(clientTmp.Akka) > 0 {
			contacts = clientTmp.Akka
		}
		if len(contacts) > 0 {
			cleaned := make([]string, 0, len(contacts))
			for _, c := range contacts {
				cleaned = append(cleaned, strings.Trim(strings.TrimSpace(c), `"`))
			}
			nodeCfg.ClusterClient.InitialContacts = cleaned
		}
	}
	clientPrefix := prefix + ".cluster.client"
	if v, err := cfg.GetString(clientPrefix + ".establishing-get-contacts-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterClient.EstablishingGetContactsInterval = d
		}
	}
	if v, err := cfg.GetString(clientPrefix + ".refresh-contacts-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterClient.RefreshContactsInterval = d
		}
	}
	if v, err := cfg.GetString(clientPrefix + ".heartbeat-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterClient.HeartbeatInterval = d
		}
	}
	if v, err := cfg.GetString(clientPrefix + ".acceptable-heartbeat-pause"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterClient.AcceptableHeartbeatPause = d
		}
	}
	if v, err := cfg.GetInt(clientPrefix + ".buffer-size"); err == nil {
		nodeCfg.ClusterClient.BufferSize = v
	}
	if v, err := cfg.GetString(clientPrefix + ".reconnect-timeout"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "off", "false", "":
			nodeCfg.ClusterClient.ReconnectTimeout = 0
		default:
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				nodeCfg.ClusterClient.ReconnectTimeout = d
			}
		}
	}

	// ── Cluster Client Receptionist ────────────────────────────────────────
	// pekko.cluster.client.receptionist.* — parsed into
	// ClusterConfig.ClusterReceptionist (re-export of cluster/client.ReceptionistConfig).
	nodeCfg.ClusterReceptionist.Name = "receptionist"
	nodeCfg.ClusterReceptionist.NumberOfContacts = 3
	nodeCfg.ClusterReceptionist.HeartbeatInterval = 2 * time.Second
	nodeCfg.ClusterReceptionist.AcceptableHeartbeatPause = 13 * time.Second
	receptionistPrefix := clientPrefix + ".receptionist"
	if v, err := cfg.GetString(receptionistPrefix + ".name"); err == nil {
		v = strings.Trim(strings.TrimSpace(v), `"`)
		if v != "" {
			nodeCfg.ClusterReceptionist.Name = v
		}
	}
	if v, err := cfg.GetString(receptionistPrefix + ".role"); err == nil {
		nodeCfg.ClusterReceptionist.Role = strings.Trim(strings.TrimSpace(v), `"`)
	}
	if v, err := cfg.GetInt(receptionistPrefix + ".number-of-contacts"); err == nil && v > 0 {
		nodeCfg.ClusterReceptionist.NumberOfContacts = v
	}
	if v, err := cfg.GetString(receptionistPrefix + ".heartbeat-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterReceptionist.HeartbeatInterval = d
		}
	}
	if v, err := cfg.GetString(receptionistPrefix + ".acceptable-heartbeat-pause"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.ClusterReceptionist.AcceptableHeartbeatPause = d
		}
	}

	// ��─ Per-Role Min Nr Of Members ──────────────────────────────────────────
	// Parse pekko.cluster.role.{name}.min-nr-of-members for each role.
	rolePrefix := prefix + ".cluster.role"
	if roleObj, err := cfg.GetConfig(rolePrefix); err == nil {
		for _, roleName := range roleObj.Keys() {
			if v, e := cfg.GetInt(rolePrefix + "." + roleName + ".min-nr-of-members"); e == nil && v > 0 {
				if nodeCfg.RoleMinNrOfMembers == nil {
					nodeCfg.RoleMinNrOfMembers = make(map[string]int)
				}
				nodeCfg.RoleMinNrOfMembers[roleName] = v
			}
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
	if v, err := cfg.GetString(sbrPrefix + ".static-quorum.role"); err == nil {
		nodeCfg.SBR.StaticQuorumRole = strings.TrimSpace(v)
	}
	if v, err := cfg.GetString(sbrPrefix + ".down-all-when-unstable"); err == nil {
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "off", "false":
			f := false
			nodeCfg.SBR.DownAllWhenUnstableEnabled = &f
		case "on", "true":
			t := true
			nodeCfg.SBR.DownAllWhenUnstableEnabled = &t
		default:
			// Explicit duration
			if d, parseErr := parseHOCONDuration(v); parseErr == nil {
				t := true
				nodeCfg.SBR.DownAllWhenUnstableEnabled = &t
				nodeCfg.SBR.DownAllWhenUnstable = d
			}
		}
	}

	// Unmarshal Management and Metrics configs from HOCON.
	// cfg.Unmarshal is used as a first pass, but bool fields inside nested
	// structs are not reliably populated by the HOCON library.  Explicit
	// GetString calls below mirror the pattern used for Discovery / Telemetry.
	nodeCfg.Management = core.DefaultManagementConfig()
	nodeCfg.Metrics = core.DefaultMetricsExporterConfig()
	_ = cfg.Unmarshal(&nodeCfg)

	// ── Management HTTP API ───────────────────────────────────────────────────
	// Primary: pekko.management.http (Pekko Management standard)
	// Fallback: gekka.management.http (deprecated, same semantics)
	// Both namespaces are checked; last-writer wins (gekka overrides pekko if both present).
	mgmtPrefixes := []string{prefix + ".management.http", "gekka.management.http"}
	for _, mgmtPrefix := range mgmtPrefixes {
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

	// ── Cluster Bootstrap (Pekko-compatible) ─────────────────────────────────
	// Primary: pekko.management.cluster.bootstrap.contact-point-discovery
	// Deprecated fallback: gekka.cluster.discovery (logs warning if used)
	if nodeCfg.Discovery.Config.Config == nil {
		nodeCfg.Discovery.Config.Config = make(map[string]any)
	}

	bootstrapPrefix := "pekko.management.cluster.bootstrap.contact-point-discovery"
	bootstrapUsed := false
	if v, err := cfg.GetString(bootstrapPrefix + ".discovery-method"); err == nil {
		nodeCfg.Discovery.Enabled = true
		nodeCfg.Discovery.Type = strings.TrimSpace(v)
		bootstrapUsed = true
	}
	if v, err := cfg.GetString(bootstrapPrefix + ".required-contact-point-nr"); err == nil {
		if n, parseErr := strconv.Atoi(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Discovery.Config.Config["required-contact-points"] = n
		}
	}
	if v, err := cfg.GetString(bootstrapPrefix + ".stable-margin"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Discovery.Config.Config["stable-margin"] = d
		}
	}
	if v, err := cfg.GetString(bootstrapPrefix + ".discovery-interval"); err == nil {
		if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
			nodeCfg.Discovery.Config.Config["discovery-interval"] = d
		}
	}

	// pekko.discovery.{method}.* — provider-specific config
	if v, err := cfg.GetString("pekko.discovery.method"); err == nil && !bootstrapUsed {
		nodeCfg.Discovery.Enabled = true
		nodeCfg.Discovery.Type = strings.TrimSpace(v)
	}
	if discoveryObj, err := cfg.GetConfig("pekko.discovery.kubernetes-api"); err == nil {
		if ns, e := discoveryObj.GetString("namespace"); e == nil {
			nodeCfg.Discovery.Config.Config["namespace"] = strings.TrimSpace(ns)
		}
		if ls, e := discoveryObj.GetString("label-selector"); e == nil {
			nodeCfg.Discovery.Config.Config["label-selector"] = strings.TrimSpace(ls)
		}
		if p, e := discoveryObj.GetInt("port"); e == nil {
			nodeCfg.Discovery.Config.Config["port"] = p
		}
	}
	if discoveryObj, err := cfg.GetConfig("pekko.discovery.kubernetes-dns"); err == nil {
		if sn, e := discoveryObj.GetString("service-name"); e == nil {
			nodeCfg.Discovery.Config.Config["service-name"] = strings.TrimSpace(sn)
		}
		if p, e := discoveryObj.GetInt("port"); e == nil {
			nodeCfg.Discovery.Config.Config["port"] = p
		}
	}

	// Deprecated: gekka.cluster.discovery (fallback with warning)
	if !nodeCfg.Discovery.Enabled {
		deprecatedPrefix := "gekka.cluster.discovery"
		if v, err := cfg.GetString(deprecatedPrefix + ".enabled"); err == nil {
			v = strings.ToLower(strings.TrimSpace(v))
			if v == "true" || v == "on" {
				slog.Warn("config: gekka.cluster.discovery is deprecated, use pekko.management.cluster.bootstrap instead")
				nodeCfg.Discovery.Enabled = true
			}
		}
		if v, err := cfg.GetString(deprecatedPrefix + ".type"); err == nil {
			nodeCfg.Discovery.Type = strings.TrimSpace(v)
		}
		if configObj, err := cfg.GetConfig(deprecatedPrefix + ".config"); err == nil {
			_ = configObj.Unmarshal(&nodeCfg.Discovery.Config.Config)
		}
		apiPrefix := deprecatedPrefix + ".kubernetes-api"
		if v, err := cfg.GetString(apiPrefix + ".namespace"); err == nil {
			nodeCfg.Discovery.Config.Config["namespace"] = strings.TrimSpace(v)
		}
		if v, err := cfg.GetString(apiPrefix + ".label-selector"); err == nil {
			nodeCfg.Discovery.Config.Config["label-selector"] = strings.TrimSpace(v)
		}
		if v, err := cfg.GetInt(apiPrefix + ".port"); err == nil {
			nodeCfg.Discovery.Config.Config["port"] = v
		}
		dnsPrefix := deprecatedPrefix + ".kubernetes-dns"
		if v, err := cfg.GetString(dnsPrefix + ".service-name"); err == nil {
			nodeCfg.Discovery.Config.Config["service-name"] = strings.TrimSpace(v)
		}
		if v, err := cfg.GetInt(dnsPrefix + ".port"); err == nil {
			nodeCfg.Discovery.Config.Config["port"] = v
		}
	}

	// ── Distributed Data ─────────────────────────────────────────────────────
	// Primary: pekko.cluster.distributed-data (Pekko-compatible)
	// Fallback: gekka.cluster.distributed-data (deprecated)
	nodeCfg.DistributedData.Name = "ddataReplicator"
	nodeCfg.DistributedData.NotifySubscribersInterval = 500 * time.Millisecond
	nodeCfg.DistributedData.MaxDeltaElements = 500
	nodeCfg.DistributedData.DeltaCRDTEnabled = true
	nodeCfg.DistributedData.DeltaCRDTMaxDeltaSize = 50
	nodeCfg.DistributedData.PruningInterval = 120 * time.Second
	nodeCfg.DistributedData.MaxPruningDissemination = 300 * time.Second
	ddataPrefixes := []string{prefix + ".cluster.distributed-data", "gekka.cluster.distributed-data"}
	for _, ddataPrefix := range ddataPrefixes {
		if v, err := cfg.GetString(ddataPrefix + ".enabled"); err == nil {
			v = strings.ToLower(strings.TrimSpace(v))
			nodeCfg.DistributedData.Enabled = v == "true" || v == "on"
		}
		if v, err := cfg.GetString(ddataPrefix + ".gossip-interval"); err == nil {
			if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
				nodeCfg.DistributedData.GossipInterval = d
			}
		}
		if v, err := cfg.GetString(ddataPrefix + ".name"); err == nil {
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				nodeCfg.DistributedData.Name = trimmed
			}
		}
		if v, err := cfg.GetString(ddataPrefix + ".role"); err == nil {
			nodeCfg.DistributedData.Role = strings.TrimSpace(v)
		}
		if v, err := cfg.GetString(ddataPrefix + ".notify-subscribers-interval"); err == nil {
			if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
				nodeCfg.DistributedData.NotifySubscribersInterval = d
			}
		}
		if v, err := cfg.GetInt(ddataPrefix + ".max-delta-elements"); err == nil {
			nodeCfg.DistributedData.MaxDeltaElements = v
		}
		if v, err := cfg.GetString(ddataPrefix + ".delta-crdt.enabled"); err == nil {
			v = strings.ToLower(strings.TrimSpace(v))
			nodeCfg.DistributedData.DeltaCRDTEnabled = !(v == "off" || v == "false")
		}
		if v, err := cfg.GetInt(ddataPrefix + ".delta-crdt.max-delta-size"); err == nil {
			nodeCfg.DistributedData.DeltaCRDTMaxDeltaSize = v
		}
		if v, err := cfg.GetString(ddataPrefix + ".prefer-oldest"); err == nil {
			v = strings.ToLower(strings.TrimSpace(v))
			nodeCfg.DistributedData.PreferOldest = v == "on" || v == "true"
		}
		if v, err := cfg.GetString(ddataPrefix + ".pruning-interval"); err == nil {
			if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
				nodeCfg.DistributedData.PruningInterval = d
			}
		}
		if v, err := cfg.GetString(ddataPrefix + ".max-pruning-dissemination"); err == nil {
			if d, parseErr := parseHOCONDuration(strings.TrimSpace(v)); parseErr == nil {
				nodeCfg.DistributedData.MaxPruningDissemination = d
			}
		}
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

// parseHOCONByteSize parses a Pekko/HOCON byte-size string such as "256 KiB",
// "256k", "1048576", "1 MiB" into an int (number of bytes).
func parseHOCONByteSize(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty byte size")
	}

	// Find where the numeric part ends.
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	numStr := strings.TrimSpace(s[:i])
	unit := strings.ToLower(strings.TrimSpace(s[i:]))

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("parseHOCONByteSize: cannot parse number %q: %w", numStr, err)
	}

	multiplier := 1.0
	switch unit {
	case "", "b":
		multiplier = 1
	case "k", "kb", "kib":
		multiplier = 1024
	case "m", "mb", "mib":
		multiplier = 1024 * 1024
	case "g", "gb", "gib":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("parseHOCONByteSize: unknown unit %q", unit)
	}

	return int(num * multiplier), nil
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
