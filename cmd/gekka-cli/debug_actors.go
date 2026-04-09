/*
 * debug_actors.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/management/client"
	"github.com/spf13/cobra"
)

func newDebugActorsCmd(root *rootState) *cobra.Command {
	var (
		flagURL    string
		withSystem bool
	)
	cmd := &cobra.Command{
		Use:   "actors",
		Short: "Interactive tree view of actors on a node",
		RunE: func(cmd *cobra.Command, _ []string) error {
			baseURL := root.resolveURL(flagURL)
			if root.quiet {
				return fmt.Errorf("--quiet is incompatible with the interactive debug actors view")
			}
			if root.jsonOutput || !isTerminal(os.Stdout) {
				c := client.New(baseURL)
				entries, err := c.DebugActors(withSystem)
				if err != nil {
					return fmt.Errorf("debug actors: %w", err)
				}
				b, _ := json.MarshalIndent(entries, "", "  ")
				fmt.Println(string(b))
				return nil
			}
			m := newDebugActorsModel(baseURL, withSystem)
			m.client = client.New(baseURL)
			_, err := tea.NewProgram(m, tea.WithAltScreen()).Run()
			return err
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	cmd.Flags().BoolVar(&withSystem, "system", false, "Include /system/* actors in the output")
	return cmd
}

type debugActorsDataMsg struct {
	entries []client.ActorEntry
	err     error
}

type debugActorsModel struct {
	baseURL    string
	client     *client.Client
	withSystem bool
	vp         viewport.Model
	confirm    ConfirmExit
	entries    []client.ActorEntry
	lastErr    error
	ready      bool
}

func newDebugActorsModel(baseURL string, withSystem bool) *debugActorsModel {
	return &debugActorsModel{
		baseURL:    baseURL,
		withSystem: withSystem,
		vp:         viewport.New(80, 20),
	}
}

func (m *debugActorsModel) Init() tea.Cmd {
	return tea.Batch(m.fetchCmd(), tea.EnterAltScreen)
}

func (m *debugActorsModel) fetchCmd() tea.Cmd {
	return func() tea.Msg {
		if m.client == nil {
			return debugActorsDataMsg{err: fmt.Errorf("nil client")}
		}
		entries, err := m.client.DebugActors(m.withSystem)
		return debugActorsDataMsg{entries: entries, err: err}
	}
}

func (m *debugActorsModel) rebuildContent() {
	paths := make([]string, 0, len(m.entries))
	for _, e := range m.entries {
		paths = append(paths, e.Path)
	}
	root := BuildPathTree(paths)
	lines := RenderTree(root)
	m.vp.SetContent(strings.Join(lines, "\n"))
}

func (m *debugActorsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.vp.Width = msg.Width
		m.vp.Height = msg.Height - 3
		m.ready = true
		m.rebuildContent()
		return m, nil

	case debugActorsDataMsg:
		m.lastErr = msg.err
		if msg.err == nil {
			m.entries = msg.entries
			m.rebuildContent()
		}
		return m, nil

	case tea.KeyMsg:
		wasActive := m.confirm.Active()
		nextConfirm, _ := m.confirm.Update(msg)
		m.confirm = nextConfirm
		if m.confirm.Confirmed() {
			return m, tea.Quit
		}
		if !wasActive && m.confirm.Active() {
			return m, m.confirm.ScheduleTimeout()
		}
		if m.confirm.Active() {
			return m, nil
		}
		if msg.String() == "r" {
			return m, m.fetchCmd()
		}
		var cmd tea.Cmd
		m.vp, cmd = m.vp.Update(msg)
		return m, cmd

	case ConfirmTimeoutMsg:
		nextConfirm, _ := m.confirm.Update(msg)
		m.confirm = nextConfirm
		return m, nil
	}

	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

func (m *debugActorsModel) View() string {
	if !m.ready {
		return "Loading..."
	}
	title := lipgloss.NewStyle().Bold(true).Render("gekka-cli debug actors")
	if m.withSystem {
		title += lipgloss.NewStyle().Faint(true).Render(" [system=true]")
	}
	status := "j/k:scroll  r:refresh  q/ESC:quit"
	if m.lastErr != nil {
		status = fmt.Sprintf("error: %v", m.lastErr)
	} else if m.confirm.Active() {
		status = m.confirm.View()
	}
	return title + "\n" + m.vp.View() + "\n" + status
}
