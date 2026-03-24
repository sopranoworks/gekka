/*
 * dashboard.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/management/client"
)

var (
	headerBoxStyle = lipgloss.NewStyle().
			Padding(1, 0).
			MarginBottom(1)

	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#6A4CFF")).
			Padding(0, 1).
			MarginTop(1)

	nodeUpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("10")) // Green

	nodeDownStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("9")) // Red

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("12")) // Blue

	rttRedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("9")) // Red

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
	frame         int // for marquee animation
}

func (m dashboardModel) Init() tea.Cmd {
	return tea.Batch(
		m.fetchMembers(),
		m.tick(),
	)
}

func (m *dashboardModel) fetchMembers() tea.Cmd {
	return func() tea.Msg {
		// Use a short timeout for API polling
		_, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		
		// The client.Members() method currently doesn't take a context, 
		// but we can add one if needed. For now, we use the default.
		members, err := m.mgmtClient.Members()
		return membersMsg{members: members, err: err}
	}
}

func (m dashboardModel) tick() tea.Cmd {
	return tea.Every(250*time.Millisecond, func(t time.Time) tea.Msg {
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
			return m, resetCmd
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		m.frame++
		var cmd tea.Cmd
		if m.frame%8 == 0 {
			cmd = m.fetchMembers()
		}
		return m, tea.Batch(cmd, m.tick())

	case membersMsg:
		m.members = nil
		if msg.err == nil {
			// Filter local node from member list using the Self field.
			for _, mem := range msg.members {
				if mem.Self {
					continue
				}
				m.members = append(m.members, mem)
			}

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
		m.err = msg.err
		m.lastUpdate = time.Now()
		return m, nil

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

	// Icon Segments (Restored stagger from gekka-metrics)
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
	var memberListBlock string
	if m.err != nil {
		memberListBlock = nodeDownStyle.Render(fmt.Sprintf("Error: %v", m.err))
	} else if len(m.members) == 0 {
		memberListBlock = infoStyle.Render("No remote members found in cluster...")
	} else {
		// Calculate dynamic column widths
		maxAddrLen := 7 // "ADDRESS"
		for _, mem := range m.members {
			if len(mem.Address) > maxAddrLen {
				maxAddrLen = len(mem.Address)
			}
		}

		headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("7"))
		// Styles with fixed widths for alignment. Width includes right padding.
		addrStyle := lipgloss.NewStyle().Width(maxAddrLen + 4)
		statusStyle := lipgloss.NewStyle().Width(14)
		roleStyle := lipgloss.NewStyle().Width(24)
		reachStyle := lipgloss.NewStyle().Width(14)
		rttStyle := lipgloss.NewStyle().Width(10)

		var rows []string

		// 1. Render Header Row
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top,
			headerStyle.Copy().Inherit(addrStyle).Render("ADDRESS"),
			headerStyle.Copy().Inherit(statusStyle).Render("STATUS"),
			headerStyle.Copy().Inherit(roleStyle).Render("ROLES"),
			headerStyle.Copy().Inherit(reachStyle).Render("REACHABLE"),
			headerStyle.Copy().Inherit(rttStyle).Render("RTT"),
		))

		// 2. Render Separator
		separator := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(
			strings.Repeat("-", maxAddrLen) + "    " + 
			strings.Repeat("-", 10) + "    " + 
			strings.Repeat("-", 20) + "    " + 
			strings.Repeat("-", 10) + "    " + 
			strings.Repeat("-", 6))
		rows = append(rows, separator)
		
		// 3. Render Member Rows
		for _, mem := range m.members {
			status := mem.Status
			if mem.Status == "Up" {
				status = nodeUpStyle.Render(mem.Status)
			}

			// Roles Marquee
			rolesStr := strings.Join(mem.Roles, ",")
			if rolesStr == "" {
				rolesStr = "-"
			}
			displayRoles := rolesStr
			if len(rolesStr) > 20 {
				padding := "    "
				marquee := rolesStr + padding
				shift := m.frame % len(marquee)
				displayRoles = marquee[shift:] + marquee[:shift]
				displayRoles = displayRoles[:20]
			}

			reachable := "yes"
			if !mem.Reachable {
				reachable = nodeDownStyle.Render("NO")
			}

			rttStr := fmt.Sprintf("%dms", mem.LatencyMs)
			var rttRendered string
			if !mem.Reachable {
				rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("timeout")
			} else {
				if mem.LatencyMs >= 500 {
					rttRendered = rttRedStyle.Render(rttStr)
				} else if mem.LatencyMs >= 200 {
					rttRendered = rttOrangeStyle.Render(rttStr)
				} else if mem.LatencyMs >= 50 {
					rttRendered = rttYellowStyle.Render(rttStr)
				} else {
					rttRendered = rttGreenStyle.Render(rttStr)
				}
			}

			rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top,
				addrStyle.Render(mem.Address),
				statusStyle.Render(status),
				roleStyle.Render(displayRoles),
				reachStyle.Render(reachable),
				rttStyle.Render(rttRendered),
			))
		}
		memberListBlock = lipgloss.JoinVertical(lipgloss.Left, rows...)
	}

	// Status Bar
	status := statusBarStyle.Render(
		fmt.Sprintf("Management: %s | Last Update: %s",
			m.mgmtURL,
			m.lastUpdate.Format("15:04:05")),
	)

	ui := lipgloss.JoinVertical(lipgloss.Left,
		header,
		memberListBlock,
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
		
		// Calculate available height for the middle section
		occupiedHeight := lipgloss.Height(header) + lipgloss.Height(memberListBlock) + 2
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

func newDashboardCmd(root *rootState) *cobra.Command {
	var url string
	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Launch interactive cluster monitoring dashboard",
		RunE: func(cmd *cobra.Command, args []string) error {
			mURL := root.resolveURL(url)
			return runDashboard(mURL)
		},
	}
	cmd.Flags().StringVar(&url, "url", "", "Management API base URL")
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
