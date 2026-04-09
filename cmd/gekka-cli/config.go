/*
 * config.go
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

func newConfigCmd(root *rootState) *cobra.Command {
	var (
		flagURL string
		filter  string
	)
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Interactive tree view of the cluster configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			baseURL := root.resolveURL(flagURL)
			if root.quiet {
				return fmt.Errorf("--quiet is incompatible with the interactive config view")
			}
			if root.jsonOutput || !isTerminal(os.Stdout) {
				c := client.New(baseURL)
				entries, err := c.ConfigEntries()
				if err != nil {
					return fmt.Errorf("config: %w", err)
				}
				b, _ := json.MarshalIndent(entries, "", "  ")
				fmt.Println(string(b))
				return nil
			}
			m := newConfigModel(baseURL, filter)
			m.client = client.New(baseURL)
			_, err := tea.NewProgram(m, tea.WithAltScreen()).Run()
			return err
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	cmd.Flags().StringVar(&filter, "filter", "", "Restrict output to keys under this dotted prefix")
	return cmd
}

type configDataMsg struct {
	entries map[string]any
	err     error
}

type configModel struct {
	baseURL    string
	client     *client.Client
	vp         viewport.Model
	confirm    ConfirmExit
	entries    map[string]any
	filter     string // applied filter
	filterText string // text being typed
	filtering  bool
	lastErr    error
	ready      bool
}

func newConfigModel(baseURL, initialFilter string) *configModel {
	return &configModel{
		baseURL: baseURL,
		filter:  initialFilter,
		vp:      viewport.New(80, 20),
	}
}

func (m *configModel) Init() tea.Cmd {
	return tea.Batch(m.fetchCmd(), tea.EnterAltScreen)
}

func (m *configModel) fetchCmd() tea.Cmd {
	return func() tea.Msg {
		entries, err := m.client.ConfigEntries()
		return configDataMsg{entries: entries, err: err}
	}
}

func (m *configModel) rebuildContent() {
	root := BuildTree(m.entries)
	filtered := FilterTree(root, m.filter)
	lines := RenderTree(filtered)
	m.vp.SetContent(strings.Join(lines, "\n"))
}

func (m *configModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.vp.Width = msg.Width
		m.vp.Height = msg.Height - 3
		m.ready = true
		m.rebuildContent()
		return m, nil

	case configDataMsg:
		m.lastErr = msg.err
		if msg.err == nil {
			m.entries = msg.entries
			m.rebuildContent()
		}
		return m, nil

	case tea.KeyMsg:
		if m.filtering {
			switch msg.Type {
			case tea.KeyEnter:
				m.filter = m.filterText
				m.filtering = false
				m.filterText = ""
				m.rebuildContent()
				return m, nil
			case tea.KeyEsc:
				m.filtering = false
				m.filterText = ""
				return m, nil
			case tea.KeyBackspace:
				if len(m.filterText) > 0 {
					m.filterText = m.filterText[:len(m.filterText)-1]
				}
				return m, nil
			case tea.KeyRunes:
				m.filterText += string(msg.Runes)
				return m, nil
			}
			return m, nil
		}

		if msg.String() == "/" {
			m.filtering = true
			m.filterText = ""
			return m, nil
		}

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

func (m *configModel) View() string {
	if !m.ready {
		return "Loading..."
	}
	header := lipgloss.NewStyle().Bold(true).Render("gekka-cli config")
	if m.filter != "" {
		header += lipgloss.NewStyle().Faint(true).Render(" [filter: " + m.filter + "]")
	}
	status := "j/k:scroll  /:filter  q/ESC:quit"
	if m.filtering {
		status = "filter> " + m.filterText + "_"
	} else if m.lastErr != nil {
		status = fmt.Sprintf("error: %v", m.lastErr)
	} else if m.confirm.Active() {
		status = m.confirm.View()
	}
	return header + "\n" + m.vp.View() + "\n" + status
}
