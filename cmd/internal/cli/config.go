/*
 * config.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package cli provides configuration loading and flag-override logic shared
// across all gekka-cli sub-commands.
//
// Configuration is read from a YAML file whose default location is
// ~/.gekka/config.yaml.  The path can be overridden with --config.  Any
// value supplied via a command-line flag always takes precedence over the
// file value ("flag wins").
//
// Example config file:
//
//	management_url: http://127.0.0.1:8558
//	default_profile: local
//
//	profiles:
//	  local:
//	    management_url: http://127.0.0.1:8558
//	  staging:
//	    management_url: http://staging-node:8558
package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ProfileConfig holds per-named-profile overrides.
type ProfileConfig struct {
	// ManagementURL is the base URL of the Cluster HTTP Management API for
	// this profile (e.g. "http://127.0.0.1:8558").
	ManagementURL string `yaml:"management_url"`
}

// Config is the top-level structure representing a gekka-cli configuration
// file.  Fields correspond directly to YAML keys.
type Config struct {
	// ManagementURL is the default base URL used when no profile is active and
	// no --url flag is provided.
	// YAML: management_url
	ManagementURL string `yaml:"management_url"`

	// DefaultProfile selects which entry in Profiles to use when --profile is
	// not specified on the command line.
	// YAML: default_profile
	DefaultProfile string `yaml:"default_profile"`

	// Profiles is a map of named profile configurations.
	// YAML: profiles
	Profiles map[string]ProfileConfig `yaml:"profiles,omitempty"`
}

// DefaultConfigPath returns the default path for the config file:
// $HOME/.gekka/config.yaml.
func DefaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".gekka/config.yaml"
	}
	return filepath.Join(home, ".gekka", "config.yaml")
}

// Load reads a YAML config file from path and returns the parsed Config.
// If the file does not exist, an empty Config is returned (not an error) so
// that flag-only invocations work without requiring a config file.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return Config{}, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("cli: open config %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("cli: parse config %q: %w", path, err)
	}
	return cfg, nil
}

// ResolveManagementURL returns the effective management URL, applying the
// following precedence (highest to lowest):
//
//  1. flagURL — value of the --url flag (non-empty string means "flag was set")
//  2. Profile named by profile — if non-empty and present in cfg.Profiles
//  3. cfg.DefaultProfile — looked up in cfg.Profiles
//  4. cfg.ManagementURL — top-level fallback
//  5. defaultURL — compile-time default ("http://127.0.0.1:8558")
func ResolveManagementURL(cfg Config, flagURL, profile string) string {
	const defaultURL = "http://127.0.0.1:8558"

	// 1. Explicit --url flag wins unconditionally.
	if flagURL != "" {
		return flagURL
	}

	// 2. Explicit --profile flag.
	if profile != "" {
		if p, ok := cfg.Profiles[profile]; ok && p.ManagementURL != "" {
			return p.ManagementURL
		}
	}

	// 3. Default profile from config file.
	if cfg.DefaultProfile != "" {
		if p, ok := cfg.Profiles[cfg.DefaultProfile]; ok && p.ManagementURL != "" {
			return p.ManagementURL
		}
	}

	// 4. Top-level management_url.
	if cfg.ManagementURL != "" {
		return cfg.ManagementURL
	}

	// 5. Hard-coded default.
	return defaultURL
}
