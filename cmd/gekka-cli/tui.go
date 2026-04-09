/*
 * tui.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// ConfirmTimeoutMsg is delivered by a tea.Tick when the confirm-exit timer
// fires.  If ID matches the current confirmExitID the confirmation is
// cancelled.
type ConfirmTimeoutMsg struct {
	ID int
}

// ConfirmExit is a reusable sub-model for "are you sure you want to quit?"
// prompts with a 5-second auto-cancel timer.  Owners embed it and forward
// Update calls while Active() is true.
type ConfirmExit struct {
	active    bool
	confirmed bool
	timeoutID int
}

// NewConfirmExit returns a fresh inactive ConfirmExit.
func NewConfirmExit() ConfirmExit {
	return ConfirmExit{}
}

// Active reports whether the confirm prompt is currently displayed.
func (c ConfirmExit) Active() bool { return c.active }

// Confirmed reports whether the user confirmed the exit.  Once true, the
// owning model should return tea.Quit.
func (c ConfirmExit) Confirmed() bool { return c.confirmed }

// TimeoutID returns the current timer generation — used by tests and by
// the owning model to schedule a tea.Tick.
func (c ConfirmExit) TimeoutID() int { return c.timeoutID }

// Update processes a tea.Msg and returns the updated model + any tea.Cmd.
// The caller is responsible for also scheduling the 5s tea.Tick when Active()
// transitions from false to true; Update returns nil cmd to keep the helper
// side-effect-free for unit tests.
func (c ConfirmExit) Update(msg tea.Msg) (ConfirmExit, tea.Cmd) {
	switch m := msg.(type) {
	case tea.KeyMsg:
		if !c.active {
			if m.Type == tea.KeyEsc || m.String() == "q" || m.Type == tea.KeyCtrlC {
				c.active = true
				c.timeoutID++
				return c, nil
			}
			return c, nil
		}
		// Active — handle y/n.
		switch m.String() {
		case "y", "Y":
			c.confirmed = true
			c.active = false
			return c, nil
		case "n", "N", "esc":
			c.active = false
			return c, nil
		}
		if m.Type == tea.KeyEsc {
			c.active = false
			return c, nil
		}
	case ConfirmTimeoutMsg:
		if c.active && m.ID == c.timeoutID {
			c.active = false
		}
	}
	return c, nil
}

// ScheduleTimeout returns a tea.Cmd that fires a ConfirmTimeoutMsg after 5s.
// Callers invoke this from their Update when ConfirmExit.Active() flips to
// true.
func (c ConfirmExit) ScheduleTimeout() tea.Cmd {
	id := c.timeoutID
	return tea.Tick(5*time.Second, func(_ time.Time) tea.Msg {
		return ConfirmTimeoutMsg{ID: id}
	})
}

// View returns the confirm prompt string, or "" if not active.
func (c ConfirmExit) View() string {
	if !c.active {
		return ""
	}
	return "Quit? [y/N] (auto-cancel in 5s)"
}
