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
	"encoding/json"
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
	stateStateExplorer
	stateConfirmDown
	stateConfirmLeave
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

type durableStateMsg struct {
	res *client.DurableStateResponse
	err error
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

	// Member selection
	selectedIndex int

	// State Explorer
	persistenceID string
	durableState  *client.DurableStateResponse
	explorerErr   error
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

		members, err := m.mgmtClient.Members()
		return membersMsg{members: members, err: err}
	}
}

func (m *dashboardModel) fetchDurableState() tea.Cmd {
	return func() tea.Msg {
		res, err := m.mgmtClient.DurableState(m.persistenceID)
		return durableStateMsg{res: res, err: err}
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
			if msg.String() == "s" {
				m.state = stateStateExplorer
				return m, nil
			}

			// Navigation
			if msg.String() == "j" || msg.Type == tea.KeyDown {
				if m.selectedIndex < len(m.members)-1 {
					m.selectedIndex++
				}
				return m, nil
			}
			if msg.String() == "k" || msg.Type == tea.KeyUp {
				if m.selectedIndex > 0 {
					m.selectedIndex--
				}
				return m, nil
			}

			// Cluster Ops
			if msg.String() == "d" {
				if len(m.members) > 0 {
					m.state = stateConfirmDown
				}
				return m, nil
			}
			if msg.String() == "l" {
				if len(m.members) > 0 {
					m.state = stateConfirmLeave
				}
				return m, nil
			}

		case stateStateExplorer:
			if msg.Type == tea.KeyEsc {
				m.state = stateDashboard
				return m, nil
			}
			if msg.Type == tea.KeyEnter {
				return m, m.fetchDurableState()
			}
			if msg.Type == tea.KeyBackspace {
				if len(m.persistenceID) > 0 {
					m.persistenceID = m.persistenceID[:len(m.persistenceID)-1]
				}
				return m, nil
			}
			if len(msg.String()) == 1 {
				m.persistenceID += msg.String()
				return m, nil
			}

		case stateConfirmDown, stateConfirmLeave:
			switch strings.ToLower(msg.String()) {
			case "y":
				var cmd tea.Cmd
				target := m.members[m.selectedIndex].Address
				if m.state == stateConfirmDown {
					cmd = func() tea.Msg {
						err := m.mgmtClient.DownMember(target)
						return membersMsg{err: err} // Reuse membersMsg to trigger refresh
					}
				} else {
					cmd = func() tea.Msg {
						err := m.mgmtClient.LeaveMember(target)
						return membersMsg{err: err}
					}
				}
				m.state = stateDashboard
				return m, tea.Batch(cmd, m.fetchMembers())
			case "n", "esc":
				m.state = stateDashboard
				return m, nil
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
		if m.frame%8 == 0 && m.state == stateDashboard {
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

			// Clamp selectedIndex
			if m.selectedIndex >= len(m.members) {
				m.selectedIndex = len(m.members) - 1
			}
			if m.selectedIndex < 0 {
				m.selectedIndex = 0
			}
		}
		m.err = msg.err
		m.lastUpdate = time.Now()
		return m, nil

	case durableStateMsg:
		m.durableState = msg.res
		m.explorerErr = msg.err
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

	// Main Content
	var mainBlock string
	if m.state == stateStateExplorer {
		titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("5")).MarginBottom(1)
		inputStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("7")).Background(lipgloss.Color("235")).Padding(0, 1)
		contentStyle := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("240")).Padding(1, 2).Width(m.width - 4)

		explorerTitle := titleStyle.Render("Durable State Explorer")
		inputField := "Persistence ID: " + inputStyle.Render(m.persistenceID+"█")

		var stateDisplay string
		if m.explorerErr != nil {
			stateDisplay = nodeDownStyle.Render(fmt.Sprintf("Error: %v", m.explorerErr))
		} else if m.durableState == nil {
			stateDisplay = infoStyle.Render("Enter a Persistence ID and press Enter to fetch state.")
		} else {
			stateJSON, _ := json.MarshalIndent(m.durableState.State, "", "  ")
			stateDisplay = fmt.Sprintf("Revision: %d\n\n%s", m.durableState.Revision, string(stateJSON))
		}

		mainBlock = lipgloss.JoinVertical(lipgloss.Left,
			explorerTitle,
			inputField,
			"\n",
			contentStyle.Render(stateDisplay),
			"\n",
			infoStyle.Render("Press ESC to return to Member List"),
		)
	} else {
		// Member List
		if m.err != nil {
			mainBlock = nodeDownStyle.Render(fmt.Sprintf("Error: %v", m.err))
		} else if len(m.members) == 0 {
			mainBlock = infoStyle.Render("No remote members found in cluster...")
		} else {
			// Calculate dynamic column widths
			maxAddrLen := 7 // "ADDRESS"
			for _, mem := range m.members {
				if len(mem.Address) > maxAddrLen {
					maxAddrLen = len(mem.Address)
				}
			}

			headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("7"))
			addrStyle := lipgloss.NewStyle().Width(maxAddrLen + 4)
			statusStyle := lipgloss.NewStyle().Width(14)
			roleStyle := lipgloss.NewStyle().Width(34)
			reachStyle := lipgloss.NewStyle().Width(14)
			rttStyle := lipgloss.NewStyle().Width(10)

			var rows []string
			rows = append(rows, "  "+lipgloss.JoinHorizontal(lipgloss.Top,
				headerStyle.Copy().Inherit(addrStyle).Render("ADDRESS"),
				headerStyle.Copy().Inherit(statusStyle).Render("STATUS"),
				headerStyle.Copy().Inherit(roleStyle).Render("ROLES"),
				headerStyle.Copy().Inherit(reachStyle).Render("REACHABLE"),
				headerStyle.Copy().Inherit(rttStyle).Render("RTT"),
			))

			separator := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(
				"  "+strings.Repeat("-", maxAddrLen) + "    " +
					strings.Repeat("-", 10) + "    " +
					strings.Repeat("-", 30) + "    " +
					strings.Repeat("-", 10) + "    " +
					strings.Repeat("-", 6))
			rows = append(rows, separator)

			for i, mem := range m.members {
				status := mem.Status
				if mem.Status == "Up" {
					status = nodeUpStyle.Render(mem.Status)
				}

				rolesStr := strings.Join(mem.Roles, ",")
				if rolesStr == "" {
					rolesStr = "-"
				}
				displayRoles := rolesStr
				if len(rolesStr) > 30 {
					padding := "    "
					marquee := rolesStr + padding
					shift := m.frame % len(marquee)
					displayRoles = marquee[shift:] + marquee[:shift]
					displayRoles = displayRoles[:30]
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

				row := lipgloss.JoinHorizontal(lipgloss.Top,
					addrStyle.Render(mem.Address),
					statusStyle.Render(status),
					roleStyle.Render(displayRoles),
					reachStyle.Render(reachable),
					rttStyle.Render(rttRendered),
				)

				if i == m.selectedIndex {
					row = lipgloss.NewStyle().Background(lipgloss.Color("236")).Foreground(lipgloss.Color("255")).Render("> " + row)
				} else {
					row = "  " + row
				}
				rows = append(rows, row)
			}
			mainBlock = lipgloss.JoinVertical(lipgloss.Left, rows...)
		}
	}

	// Status Bar
	status := statusBarStyle.Render(
		fmt.Sprintf("Management: %s | Mode: %s | Last Update: %s",
			m.mgmtURL,
			func() string {
				if m.state == stateStateExplorer {
					return "Explorer"
				}
				return "Dashboard"
			}(),
			m.lastUpdate.Format("15:04:05")),
	)

	ui := lipgloss.JoinVertical(lipgloss.Left,
		header,
		mainBlock,
		"\n",
		status,
	)

	// Help Text
	helpText := "q:quit | s:explorer | d:down | l:leave | j/k:select"
	if m.state == stateStateExplorer {
		helpText = "esc:back | enter:fetch | backspace:edit"
	}
	ui = lipgloss.JoinVertical(lipgloss.Left,
		ui,
		lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(helpText),
	)

	if m.state == stateConfirmExit || m.state == stateConfirmDown || m.state == stateConfirmLeave {
		overlayStyle := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			Padding(1, 3).
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF"))

		var msg string
		switch m.state {
		case stateConfirmExit:
			overlayStyle = overlayStyle.BorderForeground(lipgloss.Color("#FF0000")).Background(lipgloss.Color("#880000"))
			msg = "Exit? (Y/n)"
		case stateConfirmDown:
			overlayStyle = overlayStyle.BorderForeground(lipgloss.Color("#FF0000")).Background(lipgloss.Color("#880000"))
			msg = fmt.Sprintf("DOWN node %s? (Y/n)", m.members[m.selectedIndex].Address)
		case stateConfirmLeave:
			overlayStyle = overlayStyle.BorderForeground(lipgloss.Color("#FFA500")).Background(lipgloss.Color("#884400"))
			msg = fmt.Sprintf("LEAVE node %s? (Y/n)", m.members[m.selectedIndex].Address)
		}

		overlay := overlayStyle.Render(msg)

		occupiedHeight := lipgloss.Height(header) + lipgloss.Height(mainBlock) + 2
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
