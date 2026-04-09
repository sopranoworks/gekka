/*
 * tui_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestConfirmExit_InitialInactive(t *testing.T) {
	m := NewConfirmExit()
	if m.Active() {
		t.Error("expected inactive on init")
	}
}

func TestConfirmExit_EscActivates(t *testing.T) {
	m := NewConfirmExit()
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if !m.Active() {
		t.Error("expected Active after ESC")
	}
}

func TestConfirmExit_YConfirms(t *testing.T) {
	m := NewConfirmExit()
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	if !m.Confirmed() {
		t.Error("expected Confirmed after y")
	}
}

func TestConfirmExit_NCancels(t *testing.T) {
	m := NewConfirmExit()
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	if m.Active() || m.Confirmed() {
		t.Error("expected inactive + not confirmed after n")
	}
}

func TestConfirmExit_TimeoutCancels(t *testing.T) {
	m := NewConfirmExit()
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	id := m.TimeoutID()
	m, _ = m.Update(ConfirmTimeoutMsg{ID: id})
	if m.Active() {
		t.Error("expected inactive after matching timeout")
	}
}

func TestConfirmExit_StaleTimeoutIgnored(t *testing.T) {
	m := NewConfirmExit()
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc}) // id=1
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc}) // id=2
	m, _ = m.Update(ConfirmTimeoutMsg{ID: 1})     // stale
	if !m.Active() {
		t.Error("stale timeout should not cancel current confirm")
	}
}
