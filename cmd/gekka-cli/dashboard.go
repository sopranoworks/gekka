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
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka"
	_ "github.com/sopranoworks/gekka-extensions-cluster-k8s"
	"github.com/sopranoworks/gekka/management/client"
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

	rttRedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("1")) // Red

	rttOrangeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("208")) // Orange

	rttYellowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("11")) // Yellow

	rttGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10")) // Green
)

// ── Model ───────────────────────────────────────────────────────────────────

type state int

const (
	stateDashboard state = iota
	stateConfirmExit
)

type tickMsg time.Time
type timeoutMsg struct {
	id int
}

type membersMsg struct {
	members []client.MemberInfo
	err     error
}

type dashboardModel struct {
	mgmtURL       string
	mgmtClient    *client.Client
	members       []client.MemberInfo
	lastUpdate    time.Time
	err           error
	quitting      bool
	state         state
	confirmExitID int
	width         int
	height        int
}

func (m dashboardModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchMembers(),
		m.tick(),
	)
}

func (m dashboardModel) fetchMembers() tea.Cmd {
	return func() tea.Msg {
		members, err := m.mgmtClient.Members()
		return membersMsg{members: members, err: err}
	}
}

func (m dashboardModel) tick() tea.Cmd {
	return tea.Every(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m dashboardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch m.state {
		case stateDashboard:
			if msg.Type == tea.KeyEsc || msg.String() == "q" || msg.Type == tea.KeyCtrlC {
				m.state = stateConfirmExit
				m.confirmExitID++
				id := m.confirmExitID
				return m, tea.Tick(5*time.Second, func(_ time.Time) tea.Msg {
					return timeoutMsg{id: id}
				})
			}
		case stateConfirmExit:
			// Reset timer on any key press
			m.confirmExitID++
			id := m.confirmExitID
			resetCmd := tea.Tick(5*time.Second, func(_ time.Time) tea.Msg {
				return timeoutMsg{id: id}
			})

			switch strings.ToLower(msg.String()) {
			case "y":
				m.quitting = true
				return m, tea.Quit
			case "n", "esc":
				m.state = stateDashboard
				return m, nil
			}
			return m, resetCmd // swallow all other keys but reset timer
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		return m, m.fetchMembers()

	case membersMsg:
		m.members = msg.members
		m.err = msg.err
		if m.err == nil {
			// Sorting logic:
			// 1. Unreachable nodes at top.
			// 2. Then descending RTT.
			sort.Slice(m.members, func(i, j int) bool {
				mi, mj := m.members[i], m.members[j]
				if mi.Reachable != mj.Reachable {
					return !mi.Reachable // false (unreachable) comes first
				}
				return mi.LatencyMs > mj.LatencyMs
			})
		}
		m.lastUpdate = time.Now()
		return m, m.tick()

	case timeoutMsg:
		if m.state == stateConfirmExit && msg.id == m.confirmExitID {
			m.state = stateDashboard
		}
		return m, nil
	}

	return m, nil
}

func (m dashboardModel) View() string {
	if m.quitting {
		return "Shutting down dashboard...\n"
	}

	if m.width == 0 || m.height == 0 {
		return "Initialising..."
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

	// Member List
	var nodes string
	if m.err != nil {
		nodes = nodeDownStyle.Render(fmt.Sprintf("Error: %v", m.err))
	} else if len(m.members) == 0 {
		nodes = infoStyle.Render("No members found in cluster...")
	} else {
		headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("7"))
		nodes = fmt.Sprintf("%-45s  %-10s  %-10s  %s\n", 
			headerStyle.Render("ADDRESS"), 
			headerStyle.Render("STATUS"), 
			headerStyle.Render("REACHABLE"), 
			headerStyle.Render("RTT"))
		nodes += lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(fmt.Sprintf("%s\n", 
			"---------------------------------------------  ----------  ----------  ------"))
		
		for _, m := range m.members {
			status := m.Status
			if m.Status == "Up" {
				status = nodeUpStyle.Render(m.Status)
			}

			reachable := "yes"
			if !m.Reachable {
				reachable = nodeDownStyle.Render("NO")
			}

			rttStr := fmt.Sprintf("%dms", m.LatencyMs)
			var rttRendered string
			if !m.Reachable {
				rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("timeout")
			} else {
				// Color logic: Red (>=500ms), Orange (>=200ms), Yellow (>=50ms)
				if m.LatencyMs >= 500 {
					rttRendered = rttRedStyle.Render(rttStr)
				} else if m.LatencyMs >= 200 {
					rttRendered = rttOrangeStyle.Render(rttStr)
				} else if m.LatencyMs >= 50 {
					rttRendered = rttYellowStyle.Render(rttStr)
				} else {
					rttRendered = rttGreenStyle.Render(rttStr)
				}
			}

			nodes += fmt.Sprintf("%-45s  %-10s  %-10s  %s\n", 
				m.Address, status, reachable, rttRendered)
		}
	}

	// Status Bar
	status := statusBarStyle.Render(
		fmt.Sprintf("Management: %s | Last Update: %s",
			m.mgmtURL,
			m.lastUpdate.Format("15:04:05")),
	)

	ui := lipgloss.JoinVertical(lipgloss.Left,
		header,
		nodes,
		"\n",
		status,
	)

	if m.state == stateConfirmExit {
		overlayStyle := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("#FF0000")).
			Padding(1, 3).
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#880000"))

		overlay := overlayStyle.Render("Exit? (Y/n)")
		
		occupiedHeight := lipgloss.Height(header) + lipgloss.Height(status) + 1
		middleHeight := m.height - occupiedHeight
		if middleHeight < 0 {
			middleHeight = 0
		}

		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			lipgloss.Place(m.width, middleHeight,
				lipgloss.Center, lipgloss.Center,
				overlay,
				lipgloss.WithWhitespaceChars(" "),
				lipgloss.WithWhitespaceForeground(lipgloss.Color("0")),
			),
			"\n",
			status,
		)
	}

	return ui
}

// ── CLI Integration ─────────────────────────────────────────────────────────

func newDashboardCmd(root *rootState) *cobra.Command {
	var flagURL string

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Launch interactive cluster monitoring dashboard",
		Long: `Starts a full-screen terminal UI for monitoring Gekka cluster status,
including member reachability and heartbeat RTT latency.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			mgmtURL := root.resolveURL(flagURL)
			return runDashboard(mgmtURL)
		},
	}

	cmd.Flags().StringVar(&flagURL, "url", "",
		"Base URL of the management API (overrides config file)")

	return cmd
}

func runDashboard(mgmtURL string) error {
	mgmtClient := client.New(mgmtURL)

	m := dashboardModel{
		mgmtURL:    mgmtURL,
		mgmtClient: mgmtClient,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
