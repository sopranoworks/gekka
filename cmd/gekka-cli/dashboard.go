/*
 * dashboard.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/discovery"
	_ "github.com/sopranoworks/gekka/discovery/kubernetes"
	"github.com/spf13/cobra"
)

// ── Styles ──────────────────────────────────────────────────────────────────

var (
	headerBoxStyle = lipgloss.NewStyle().
			MarginBottom(1)

	nodeUpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10"))

	nodeDownStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("9"))

	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("7")).
			Background(lipgloss.Color("235")).
			Padding(0, 1)

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("14"))
)

// ── Model ───────────────────────────────────────────────────────────────────

type tickMsg time.Time

type discoveryMsg struct {
	seeds []string
	err   error
}

type dashboardModel struct {
	cfg        gekka.ClusterConfig
	provider   discovery.SeedProvider
	seeds      []string
	lastUpdate time.Time
	err        error
	quitting   bool
}

func (m dashboardModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchSeeds(),
		m.tick(),
	)
}

func (m dashboardModel) fetchSeeds() tea.Cmd {
	return func() tea.Msg {
		seeds, err := m.provider.FetchSeedNodes()
		return discoveryMsg{seeds: seeds, err: err}
	}
}

func (m dashboardModel) tick() tea.Cmd {
	return tea.Every(5*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m dashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC || msg.String() == "q" {
			m.quitting = true
			return m, tea.Quit
		}

	case tickMsg:
		return m, m.fetchSeeds()

	case discoveryMsg:
		m.seeds = msg.seeds
		m.err = msg.err
		m.lastUpdate = time.Now()
		return m, m.tick()
	}

	return m, nil
}

func (m dashboardModel) View() string {
	if m.quitting {
		return "Shutting down dashboard...\n"
	}

	// Nebula Parallel-Slash Icon Colors
	c1 := lipgloss.Color("#6A4CFF")
	c2 := lipgloss.Color("#8265FF")
	c3 := lipgloss.Color("#9B7FFF")
	c4 := lipgloss.Color("#B399FF")
	c5 := lipgloss.Color("#C678FF")
	c6 := lipgloss.Color("#DD94FF")
	c7 := lipgloss.Color("#F2AEFF")
	c8 := lipgloss.Color("#FFC9FF")

	// Icon Segments
	iconTop := "  " + lipgloss.NewStyle().Foreground(c3).Render("▄") + lipgloss.NewStyle().Foreground(c4).Render("▀") + "  " + lipgloss.NewStyle().Foreground(c7).Render("▄") + lipgloss.NewStyle().Foreground(c8).Render("▀")
	iconBottom := lipgloss.NewStyle().Foreground(c1).Render("▄") + lipgloss.NewStyle().Foreground(c2).Render("▀") + "  " + lipgloss.NewStyle().Foreground(c5).Render("▄") + lipgloss.NewStyle().Foreground(c6).Render("▀")

	// Text Components
	title := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Bold(true).Render("gekka-cli")
	version := lipgloss.NewStyle().Foreground(lipgloss.Color("#808080")).Render("v" + gekka.Version)

	// Line Assembly
	topLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconTop, "  ", title)
	bottomLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconBottom, "      ", version)

	header := headerBoxStyle.Render(
		lipgloss.JoinVertical(lipgloss.Left, topLine, bottomLine),
	)

	// Node List
	var nodes string
	if m.err != nil {
		nodes = nodeDownStyle.Render(fmt.Sprintf("Error: %v", m.err))
	} else if len(m.seeds) == 0 {
		nodes = infoStyle.Render("No nodes discovered yet...")
	} else {
		nodes = "Discovered Nodes:\n"
		for _, s := range m.seeds {
			nodes += nodeUpStyle.Render(fmt.Sprintf(" • %s", s)) + "\n"
		}
	}

	// Status Bar
	status := statusBarStyle.Render(
		fmt.Sprintf("Provider: %s | Last Update: %s",
			m.cfg.Discovery.Type,
			m.lastUpdate.Format("15:04:05")),
	)

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		nodes,
		"\n",
		status,
	)
}

// ── CLI Integration ─────────────────────────────────────────────────────────

func newDashboardCmd(root *rootState) *cobra.Command {
	var hoconPath string

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Launch interactive cluster monitoring dashboard",
		Long: `Starts a full-screen terminal UI for monitoring Gekka cluster status,
including discovery metrics and node health.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDashboard(hoconPath)
		},
	}

	cmd.Flags().StringVar(&hoconPath, "hocon", "application.conf",
		"Path to the HOCON configuration file")

	return cmd
}

func runDashboard(path string) error {
	cfg, err := gekka.LoadConfig(path)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if !cfg.Discovery.Enabled {
		return fmt.Errorf("discovery is not enabled in configuration")
	}

	provider, err := discovery.Get(cfg.Discovery.Type, cfg.Discovery.Config)
	if err != nil {
		return fmt.Errorf("initialize provider: %w", err)
	}

	m := dashboardModel{
		cfg:      cfg,
		provider: provider,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
