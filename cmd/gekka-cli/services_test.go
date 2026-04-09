/*
 * services_test.go
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

func TestFormatServices_EmptyServiceColumnOnRepeat(t *testing.T) {
	in := map[string][]string{
		"billing": {"host1:2551", "host2:2551", "host4:2551"},
		"orders":  {"host3:2551"},
	}
	lines := formatServices(in)
	joined := strings.Join(lines, "\n")
	if strings.Count(joined, "billing") != 1 {
		t.Errorf("billing should appear exactly once:\n%s", joined)
	}
	if !strings.Contains(joined, "host2:2551") || !strings.Contains(joined, "host4:2551") {
		t.Errorf("missing additional addresses:\n%s", joined)
	}
	if !strings.Contains(joined, "orders") {
		t.Errorf("missing orders:\n%s", joined)
	}
	billingIdx := strings.Index(joined, "billing")
	ordersIdx := strings.Index(joined, "orders")
	if billingIdx > ordersIdx {
		t.Errorf("services should be alphabetically sorted")
	}
}

func TestFormatServices_Empty(t *testing.T) {
	lines := formatServices(nil)
	if len(lines) != 1 || !strings.Contains(lines[0], "no services") {
		t.Errorf("expected empty placeholder, got %v", lines)
	}
}
