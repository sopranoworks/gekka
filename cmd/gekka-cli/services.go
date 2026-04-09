/*
 * services.go
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
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/cmd/internal/cli"
	"github.com/sopranoworks/gekka/management/client"
	"github.com/spf13/cobra"
)

func newServicesCmd(root *rootState) *cobra.Command {
	var flagURL string
	cmd := &cobra.Command{
		Use:   "services",
		Short: "Interactive view of registered services (auto-refreshing)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			baseURL := root.resolveURL(flagURL)
			if root.quiet {
				return fmt.Errorf("--quiet is incompatible with the interactive services view")
			}
			if root.jsonOutput || !isTerminal(os.Stdout) {
				c := client.New(baseURL)
				svc, err := c.Services()
				if err != nil {
					return fmt.Errorf("services: %w", err)
				}
				b, _ := json.MarshalIndent(svc, "", "  ")
				fmt.Println(string(b))
				return nil
			}
			m := newServicesModel(baseURL)
			_, err := tea.NewProgram(m, tea.WithAltScreen()).Run()
			return err
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	return cmd
}

// formatServices turns a services map into display lines.  Services are
// sorted alphabetically; the SERVICE column is blank on rows after the first
// for multi-address services.
func formatServices(svc map[string][]string) []string {
	if len(svc) == 0 {
		return []string{"(no services registered)"}
	}
	names := make([]string, 0, len(svc))
	for k := range svc {
		names = append(names, k)
	}
	sort.Strings(names)

	var lines []string
	lines = append(lines, fmt.Sprintf("%-20s %s",
		cli.HeaderStyle.Render("SERVICE"),
		cli.HeaderStyle.Render("ADDRESS")))
	for _, name := range names {
		addrs := svc[name]
		sort.Strings(addrs)
		for i, a := range addrs {
			col := ""
			if i == 0 {
				col = name
			}
			lines = append(lines, fmt.Sprintf("%-20s %s", col, a))
		}
	}
	return lines
}

type servicesDataMsg struct {
	svc map[string][]string
	err error
}

type servicesRefreshTick time.Time

type servicesModel struct {
	baseURL string
	client  *client.Client
	vp      viewport.Model
	confirm ConfirmExit
	svc     map[string][]string
	lastErr error
	ready   bool
}

func newServicesModel(baseURL string) *servicesModel {
	return &servicesModel{
		baseURL: baseURL,
		client:  client.New(baseURL),
		vp:      viewport.New(80, 20),
	}
}

func (m *servicesModel) Init() tea.Cmd {
	return tea.Batch(m.fetchCmd(), tea.EnterAltScreen)
}

func (m *servicesModel) fetchCmd() tea.Cmd {
	return func() tea.Msg {
		svc, err := m.client.Services()
		return servicesDataMsg{svc: svc, err: err}
	}
}

func (m *servicesModel) scheduleRefresh() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return servicesRefreshTick(t)
	})
}

func (m *servicesModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.vp.Width = msg.Width
		m.vp.Height = msg.Height - 3
		m.ready = true
		m.refreshContent()
		return m, nil

	case servicesDataMsg:
		m.lastErr = msg.err
		if msg.err == nil {
			m.svc = msg.svc
			m.refreshContent()
		}
		return m, m.scheduleRefresh()

	case servicesRefreshTick:
		return m, m.fetchCmd()

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

func (m *servicesModel) refreshContent() {
	lines := formatServices(m.svc)
	m.vp.SetContent(strings.Join(lines, "\n"))
}

func (m *servicesModel) View() string {
	if !m.ready {
		return "Loading..."
	}
	header := lipgloss.NewStyle().Bold(true).Render("gekka-cli services")
	status := "j/k:scroll  r:refresh  q/ESC:quit"
	if m.lastErr != nil {
		status = fmt.Sprintf("error: %v", m.lastErr)
	}
	if m.confirm.Active() {
		status = m.confirm.View()
	}
	return header + "\n" + m.vp.View() + "\n" + status
}
