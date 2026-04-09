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
	gekkaotel "github.com/sopranoworks/gekka-extensions-telemetry-otel"

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
type timeoutMsg struct {
	id int
}
type logMsg string

type model struct {
	cm            *gcluster.ClusterManager
	otlpEndpoint  string
	lastUpdate    time.Time
	upCount       int
	totalCount    int
	state         state
	confirmExitID int
	viewport      viewport.Model
	logs          []string
	width         int
	height        int
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
			if msg.Type == tea.KeyEsc || msg.String() == "q" {
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
				return m, tea.Quit
			case "n", "esc":
				m.state = stateMetrics
				return m, nil
			}
			return m, resetCmd // swallow all other keys but reset timer
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
		if m.state == stateConfirmExit && msg.id == m.confirmExitID {
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
	version := lipgloss.NewStyle().Foreground(lipgloss.Color("#808080")).Render("v" + gekka.Version)

	// Header Assembly
	topLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconTop, "  ", title)
	bottomLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconBottom, "      ", version)
	header := lipgloss.JoinVertical(lipgloss.Left, topLine, bottomLine)

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

	ui := lipgloss.JoinVertical(lipgloss.Left,
		header,
		metrics,
		logView,
		footerBorder,
		hint,
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
		occupiedHeight := lipgloss.Height(header) + lipgloss.Height(metrics)
		middleHeight := m.height - occupiedHeight
		if middleHeight < 0 {
			middleHeight = 0
		}

		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			metrics,
			lipgloss.Place(m.width, middleHeight,
				lipgloss.Center, lipgloss.Center,
				overlay,
				lipgloss.WithWhitespaceChars(" "),
				lipgloss.WithWhitespaceForeground(lipgloss.Color("0")),
			),
		)
	}

	return ui
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

// ── Management API Auto-Enable ──────────────────────────────────────────────

// applyManagementDefaults flips the HTTP management API on by default unless
// disable is true.  Hostname and Port are set to gekka-metrics defaults
// (127.0.0.1:8559) whenever they hold their zero value OR the upstream
// gekka.LoadConfig default (8558) — the latter case matters because
// gekka.LoadConfig pre-fills the default at parse time, so Port == 0 is
// never observed by this helper after a successful load.
//
// Explicit non-default HOCON values (e.g. port = 9090, hostname = 0.0.0.0)
// are preserved.  Operators who specifically want to collide with the seed
// on :8558 must pass --disable-management and set it in their own HOCON.
//
// The "disable=true" path is the --disable-management escape hatch for
// operators who do not want gekka-metrics to bind a management port.
func applyManagementDefaults(cfg *gekka.ClusterConfig, disable bool) {
	if disable {
		return
	}
	cfg.Management.Enabled = true
	if cfg.Management.Hostname == "" || cfg.Management.Hostname == "127.0.0.1" {
		cfg.Management.Hostname = "127.0.0.1"
	}
	if cfg.Management.Port == 0 || cfg.Management.Port == 8558 {
		cfg.Management.Port = 8559
	}
}

// ── Main ────────────────────────────────────────────────────────────────────

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (required)")
	flagOtlp := flag.String("otlp", "", "OTLP/HTTP collector endpoint (overrides config)")
	flagDisableManagement := flag.Bool("disable-management", false,
		"Do not auto-enable the HTTP management API (opt out of the default port 8559 binding)")
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

	applyManagementDefaults(&cfg, *flagDisableManagement)

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

	var level slog.Level
	switch strings.ToUpper(cfg.LogLevel) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{
		Level: level,
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
		host, secure := gekkaotel.ParseEndpoint(otlpEndpoint)

		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(host),
		}
		if !secure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}

		exp, err := otlpmetrichttp.New(ctx, opts...)
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
