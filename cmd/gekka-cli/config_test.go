/*
 * config_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestConfigModel_FilterTyping(t *testing.T) {
	m := newConfigModel("", "")
	m.entries = map[string]any{
		"pekko.cluster.roles": []any{"frontend"},
		"pekko.remote.port":   2551,
	}
	m.rebuildContent()

	mi, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = mi.(*configModel)
	if !m.filtering {
		t.Error("expected filtering=true after /")
	}

	for _, r := range "pekko.cluster" {
		mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = mi.(*configModel)
	}
	if m.filterText != "pekko.cluster" {
		t.Errorf("filterText = %q, want pekko.cluster", m.filterText)
	}

	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = mi.(*configModel)
	if m.filtering {
		t.Error("expected filtering=false after enter")
	}
	content := m.vp.View()
	if strings.Contains(content, "remote") {
		t.Errorf("filter should hide remote: %s", content)
	}
}

func TestConfigModel_FilterEscape(t *testing.T) {
	m := newConfigModel("", "")
	m.entries = map[string]any{"a.b": 1}
	m.rebuildContent()

	mi, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = mi.(*configModel)
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	m = mi.(*configModel)
	mi, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = mi.(*configModel)
	if m.filtering {
		t.Error("filtering should be false after esc")
	}
	if m.filterText != "" {
		t.Errorf("filterText should be cleared, got %q", m.filterText)
	}
}
