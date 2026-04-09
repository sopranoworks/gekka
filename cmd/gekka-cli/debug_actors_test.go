/*
 * debug_actors_test.go
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
	"github.com/sopranoworks/gekka/management/client"
)

func TestDebugActorsModel_RendersPaths(t *testing.T) {
	m := newDebugActorsModel("", false)
	// Force viewport to a usable size so the rebuilt content is actually set.
	m.vp.Width = 80
	m.vp.Height = 20
	m.ready = true

	mi, _ := m.Update(debugActorsDataMsg{entries: []client.ActorEntry{
		{Path: "/user/worker-1", Kind: "user"},
		{Path: "/user/supervisor/child-a", Kind: "user"},
	}})
	m = mi.(*debugActorsModel)

	content := m.vp.View()
	if !strings.Contains(content, "user") {
		t.Errorf("viewport content missing 'user':\n%s", content)
	}
	if !strings.Contains(content, "worker-1") {
		t.Errorf("viewport content missing worker-1:\n%s", content)
	}
}

func TestDebugActorsModel_RefreshKey(t *testing.T) {
	m := newDebugActorsModel("", false)
	// Press 'r' → expect a fetch command to be returned.
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	if cmd == nil {
		t.Error("expected fetch cmd on 'r' press")
	}
}
