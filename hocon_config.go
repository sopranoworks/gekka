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
	"strings"

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

	return nodeCfg, nil
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
