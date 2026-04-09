/*
 * main_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"testing"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/internal/core"
)

func TestApplyManagementDefaults_DefaultsWhenBlank(t *testing.T) {
	cfg := &gekka.ClusterConfig{Management: core.ManagementConfig{}}
	applyManagementDefaults(cfg, false /*disable*/)

	if !cfg.Management.Enabled {
		t.Error("expected Enabled=true")
	}
	if cfg.Management.Hostname != "127.0.0.1" {
		t.Errorf("hostname = %q, want 127.0.0.1", cfg.Management.Hostname)
	}
	if cfg.Management.Port != 8559 {
		t.Errorf("port = %d, want 8559", cfg.Management.Port)
	}
}

// TestApplyManagementDefaults_OverridesLoadConfigDefault covers the realistic
// post-LoadConfig state where DefaultManagementConfig has pre-filled Port=8558.
// gekka-metrics must detect this and bump to 8559 to avoid colliding with a
// seed node on the same host.
func TestApplyManagementDefaults_OverridesLoadConfigDefault(t *testing.T) {
	cfg := &gekka.ClusterConfig{Management: core.DefaultManagementConfig()}
	applyManagementDefaults(cfg, false)

	if cfg.Management.Port != 8559 {
		t.Errorf("LoadConfig default 8558 should be bumped to 8559, got %d", cfg.Management.Port)
	}
}

func TestApplyManagementDefaults_RespectsExplicitPort(t *testing.T) {
	cfg := &gekka.ClusterConfig{Management: core.ManagementConfig{Port: 9999}}
	applyManagementDefaults(cfg, false)

	if cfg.Management.Port != 9999 {
		t.Errorf("explicit port 9999 was overwritten to %d", cfg.Management.Port)
	}
	if !cfg.Management.Enabled {
		t.Error("Enabled should still be true")
	}
}

func TestApplyManagementDefaults_RespectsExplicitHostname(t *testing.T) {
	cfg := &gekka.ClusterConfig{Management: core.ManagementConfig{Hostname: "0.0.0.0"}}
	applyManagementDefaults(cfg, false)

	if cfg.Management.Hostname != "0.0.0.0" {
		t.Errorf("hostname overwritten to %q", cfg.Management.Hostname)
	}
}

func TestApplyManagementDefaults_DisableFlag(t *testing.T) {
	cfg := &gekka.ClusterConfig{Management: core.ManagementConfig{}}
	applyManagementDefaults(cfg, true /*disable*/)

	if cfg.Management.Enabled {
		t.Error("disable flag should leave Enabled=false")
	}
	if cfg.Management.Port != 0 {
		t.Errorf("disable flag should leave Port untouched, got %d", cfg.Management.Port)
	}
}
