/*
 * tree_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"strings"
	"testing"
)

func TestBuildTree_Basic(t *testing.T) {
	entries := map[string]any{
		"pekko.cluster.roles":            []any{"frontend", "backend"},
		"pekko.remote.artery.port":       2551,
		"pekko.remote.artery.host":       "127.0.0.1",
	}
	root := BuildTree(entries)
	if root == nil {
		t.Fatal("BuildTree returned nil")
	}
	pekko := root.Find("pekko")
	if pekko == nil {
		t.Fatal("missing pekko node")
	}
	if pekko.Find("cluster") == nil || pekko.Find("remote") == nil {
		t.Errorf("expected cluster and remote under pekko")
	}
}

func TestFilterTree_Prefix(t *testing.T) {
	entries := map[string]any{
		"pekko.cluster.roles":      []any{"frontend"},
		"pekko.remote.artery.port": 2551,
		"other.config.value":       "x",
	}
	root := BuildTree(entries)
	filtered := FilterTree(root, "pekko.cluster")
	if filtered == nil {
		t.Fatal("FilterTree returned nil")
	}
	lines := RenderTree(filtered)
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "other") || strings.Contains(joined, "remote") {
		t.Errorf("filter should exclude non-matching keys; got:\n%s", joined)
	}
	if !strings.Contains(joined, "roles") {
		t.Errorf("filter should include roles; got:\n%s", joined)
	}
}

func TestRenderTree_LeafValues(t *testing.T) {
	entries := map[string]any{
		"a.b": 42,
		"a.c": "hello",
	}
	root := BuildTree(entries)
	lines := RenderTree(root)
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "42") {
		t.Errorf("expected value 42 in output:\n%s", joined)
	}
	if !strings.Contains(joined, "hello") {
		t.Errorf("expected value 'hello' in output:\n%s", joined)
	}
}
