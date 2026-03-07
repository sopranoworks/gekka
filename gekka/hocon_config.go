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

	goacfg "github.com/go-akka/configuration"

	"gekka/gekka/actor"
)

// LoadConfig reads a HOCON configuration file and converts it to a NodeConfig.
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
func LoadConfig(path string, fallbacks ...string) (NodeConfig, error) {
	// HOCON merges in text order: later assignments override earlier ones.
	// Load fallbacks first so the primary file's values always win.
	var combined strings.Builder
	for _, fb := range fallbacks {
		data, err := os.ReadFile(fb)
		if err != nil {
			return NodeConfig{}, fmt.Errorf("gekka: read fallback %q: %w", fb, err)
		}
		combined.Write(data)
		combined.WriteByte('\n')
	}
	primary, err := os.ReadFile(path)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("gekka: read config %q: %w", path, err)
	}
	combined.Write(primary)

	cfg := goacfg.ParseString(combined.String())
	return hoconToNodeConfig(cfg)
}

// SpawnFromConfig is a convenience wrapper that calls LoadConfig then Spawn.
// After SpawnFromConfig, call node.JoinSeeds() to connect to the cluster.
//
//	node, err := gekka.SpawnFromConfig("application.conf")
//	if err != nil { log.Fatal(err) }
//	defer node.Shutdown()
//	node.Join(...) // or node.JoinSeeds()
func SpawnFromConfig(path string, fallbacks ...string) (*GekkaNode, error) {
	cfg, err := LoadConfig(path, fallbacks...)
	if err != nil {
		return nil, err
	}
	return Spawn(cfg)
}

// ParseHOCONString parses an in-memory HOCON string and returns a NodeConfig.
// Useful for embedding configuration in tests or when the config comes from
// a source other than a file (e.g. Kubernetes ConfigMap, etcd).
func ParseHOCONString(text string) (NodeConfig, error) {
	return parseHOCONString(text)
}

func parseHOCONString(text string) (NodeConfig, error) {
	cfg := goacfg.ParseString(text)
	return hoconToNodeConfig(cfg)
}

// hoconToNodeConfig maps a parsed HOCON Config to a NodeConfig.
func hoconToNodeConfig(cfg *goacfg.Config) (NodeConfig, error) {
	// Auto-detect protocol: prefer "pekko", fall back to "akka".
	proto := detectProtocol(cfg)
	prefix := proto // "pekko" or "akka"

	hostname := cfg.GetString(prefix+".remote.artery.canonical.hostname", "127.0.0.1")
	port := int(cfg.GetInt32(prefix+".remote.artery.canonical.port", 0))

	// Parse seed nodes.
	seedURIs := cfg.GetStringList(prefix + ".cluster.seed-nodes")
	seeds := make([]actor.Address, 0, len(seedURIs))
	var systemName string
	for _, uri := range seedURIs {
		// seed-nodes entries may be quoted; strip surrounding quotes.
		uri = strings.Trim(uri, `"`)
		addr, err := actor.ParseAddress(uri)
		if err != nil {
			return NodeConfig{}, fmt.Errorf("gekka: parse seed-node %q: %w", uri, err)
		}
		seeds = append(seeds, addr)
		if systemName == "" {
			systemName = addr.System
		}
	}
	if systemName == "" {
		systemName = "GekkaSystem"
	}

	self := actor.Address{
		Protocol: proto,
		System:   systemName,
		Host:     hostname,
		Port:     port,
	}

	return NodeConfig{
		Address:   self,
		SeedNodes: seeds,
	}, nil
}

// detectProtocol returns "pekko" or "akka" by checking which top-level key
// is present in the config.
func detectProtocol(cfg *goacfg.Config) string {
	if cfg.HasPath("akka.remote.artery.canonical.hostname") ||
		cfg.HasPath("akka.cluster.seed-nodes") ||
		cfg.HasPath("akka.actor.provider") {
		return "akka"
	}
	return "pekko"
}
