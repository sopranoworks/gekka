/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	gekka "github.com/sopranoworks/gekka"
	gcluster "github.com/sopranoworks/gekka/cluster"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ── Model ───────────────────────────────────────────────────────────────────

type state int

const (
	stateMetrics state = iota
	stateConfirmExit
)

type tickMsg time.Time
type timeoutMsg struct{}
type logMsg string

type model struct {
	cm           *gcluster.ClusterManager
	otlpEndpoint string
	lastUpdate   time.Time
	upCount      int
	totalCount   int
	state        state
	viewport     viewport.Model
	logs         []string
	width        int
	height       int
}

func (m model) Init() tea.Cmd {
	return m.tick()
}

func (m model) tick() tea.Cmd {
	return tea.Every(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *model) updateStats() {
	m.cm.Mu.RLock()
	gossip := m.cm.State
	m.cm.Mu.RUnlock()

	if gossip == nil {
		return
	}

	m.totalCount = len(gossip.GetMembers())
	m.upCount = 0
	for _, member := range gossip.GetMembers() {
		if member.GetStatus().String() == "Up" {
			m.upCount++
		}
	}
	m.lastUpdate = time.Now()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch m.state {
		case stateMetrics:
			if msg.String() == "q" || msg.Type == tea.KeyEsc {
				m.state = stateConfirmExit
				return m, tea.Tick(30*time.Second, func(_ time.Time) tea.Msg {
					return timeoutMsg{}
				})
			}
		case stateConfirmExit:
			switch strings.ToLower(msg.String()) {
			case "y":
				return m, tea.Quit
			case "n", "esc":
				m.state = stateMetrics
				return m, nil
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		headerHeight := 4
		footerHeight := 2
		m.viewport.Width = msg.Width
		m.viewport.Height = msg.Height - headerHeight - footerHeight
		if m.viewport.Height < 0 {
			m.viewport.Height = 0
		}

	case tickMsg:
		m.updateStats()
		return m, m.tick()

	case logMsg:
		m.logs = append(m.logs, string(msg))
		if len(m.logs) > 500 {
			m.logs = m.logs[1:]
		}
		m.viewport.SetContent(strings.Join(m.logs, "\n"))
		m.viewport.GotoBottom()

	case timeoutMsg:
		if m.state == stateConfirmExit {
			m.state = stateMetrics
		}
		return m, nil
	}

	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
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

	// Header Text
	title := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Bold(true).Render("gekka-metrics")
	version := lipgloss.NewStyle().Foreground(lipgloss.Color("#808080")).Render("v0.9.0")

	// Header Assembly
	topLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconTop, "  ", title)
	bottomLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconBottom, "      ", version)
	header := lipgloss.JoinVertical(lipgloss.Left, topLine, bottomLine)

	if m.state == stateConfirmExit {
		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			"\n",
			lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFF00")).Render("Are you sure you want to quit? (Y/n)"),
		)
	}

	// Metrics Info
	metrics := lipgloss.NewStyle().Foreground(lipgloss.Color("#00897B")).Render(
		fmt.Sprintf("OTLP: %s | %d Up / %d Total | Last Update: %s",
			m.otlpEndpoint, m.upCount, m.totalCount, m.lastUpdate.Format("15:04:05")),
	)

	// Log Viewport Area
	logView := m.viewport.View()

	// Footer with Muted Teal Horizontal Line
	footerBorder := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#00695C")).
		Render(strings.Repeat("─", m.width))

	hint := lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")).Render("Press 'q' or 'ESC' to quit")

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		metrics,
		logView,
		footerBorder,
		hint,
	)
}

// ── Custom Log Writer ───────────────────────────────────────────────────────

type teaWriter struct {
	program *tea.Program
}

func (w *teaWriter) Write(p []byte) (n int, err error) {
	s := strings.TrimSpace(string(p))
	if s != "" {
		w.program.Send(logMsg(s))
	}
	return len(p), nil
}

// ── Main ────────────────────────────────────────────────────────────────────

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (required)")
	flagOtlp := flag.String("otlp", "", "OTLP/HTTP collector endpoint (overrides config)")
	flag.Parse()

	if *flagConfig == "" {
		fmt.Fprintln(os.Stderr, "--config is required")
		os.Exit(1)
	}

	cfg, err := gekka.LoadConfig(*flagConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	cfg.Roles = appendIfMissing(cfg.Roles, "metrics-exporter")

	otlpEndpoint := cfg.Telemetry.OtlpEndpoint
	if *flagOtlp != "" {
		otlpEndpoint = *flagOtlp
	}

	// ── OTEL SDK initialisation ───────────────────────────────────────────────

	ctx := context.Background()
	mp, shutdown, err := initMeterProvider(ctx, otlpEndpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init meter provider: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(shutCtx)
	}()
	otel.SetMeterProvider(mp)

	// ── Join the cluster ──────────────────────────────────────────────────────

	node, err := gekka.NewCluster(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create cluster node: %v\n", err)
		os.Exit(1)
	}
	defer node.Shutdown()

	if err := node.JoinSeeds(); err != nil {
		fmt.Fprintf(os.Stderr, "join cluster: %v\n", err)
		os.Exit(1)
	}

	// ── Register OTEL gauge ───────────────────────────────────────────────────

	meter := mp.Meter("github.com/sopranoworks/gekka/cmd/gekka-metrics")
	cm := node.ClusterManager()

	_, _ = meter.Int64ObservableGauge(
		"gekka.cluster.members",
		otelmetric.WithDescription("Number of cluster members in each status/dc combination"),
		otelmetric.WithUnit("{members}"),
		otelmetric.WithInt64Callback(func(_ context.Context, obs otelmetric.Int64Observer) error {
			cm.Mu.RLock()
			gossip := cm.State
			cm.Mu.RUnlock()

			if gossip == nil {
				return nil
			}

			type groupKey struct{ status, dc string }
			counts := make(map[groupKey]int64)
			for _, m := range gossip.GetMembers() {
				status := statusString(m.GetStatus().String())
				dc := gcluster.DataCenterForMember(gossip, m)
				counts[groupKey{status, dc}]++
			}
			for k, n := range counts {
				obs.Observe(n,
					otelmetric.WithAttributes(
						attribute.String("status", k.status),
						attribute.String("dc", k.dc),
					),
				)
			}
			return nil
		}),
	)

	// ── Start TUI ─────────────────────────────────────────────────────────────

	m := model{
		cm:           cm,
		otlpEndpoint: otlpEndpoint,
		viewport:     viewport.New(0, 0),
	}
	m.updateStats()

	p := tea.NewProgram(m, tea.WithAltScreen())

	// Redirect standard log and slog to Bubble Tea program
	writer := &teaWriter{program: p}
	log.SetOutput(writer)
	slog.SetDefault(slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "run program: %v\n", err)
		os.Exit(1)
	}
}

func statusString(s string) string {
	return strings.TrimPrefix(s, "MemberStatus_")
}

func appendIfMissing(roles []string, role string) []string {
	for _, r := range roles {
		if r == role {
			return roles
		}
	}
	return append(roles, role)
}

func initMeterProvider(ctx context.Context, otlpEndpoint string) (*sdkmetric.MeterProvider, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("gekka-metrics")),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, nil, err
	}

	var readerOpt sdkmetric.Option
	if otlpEndpoint != "" {
		exp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(otlpEndpoint),
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			return nil, nil, err
		}
		readerOpt = sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp))
	} else {
		readerOpt = sdkmetric.WithReader(sdkmetric.NewManualReader())
	}

	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), readerOpt)
	return mp, mp.Shutdown, nil
}
