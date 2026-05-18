// cmd/showcase-runner/phases.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	exitOK             = 0
	exitSetupFailed    = 2
	exitGate1Failed    = 3
	exitGate2LogFailed = 4
	exitGate2MemFailed = 5
	exitInternalError  = 10
)

type topoNode struct {
	label    string
	host     string
	port     int
	mgmtPort int
	kind     string // "scala" or "gekka"
}

func defaultTopology() []topoNode {
	return []topoNode{
		{"s1", "127.0.0.1", 2551, 8551, "scala"},
		{"s2", "127.0.0.1", 2552, 8552, "scala"},
		{"s3", "127.0.0.1", 2553, 8553, "scala"},
		{"s4", "127.0.0.1", 2554, 8554, "scala"},
		{"s5", "127.0.0.1", 2555, 8555, "scala"},
		{"g1", "127.0.0.1", 2541, 9541, "gekka"},
		{"g2", "127.0.0.1", 2542, 9542, "gekka"},
		{"g3", "127.0.0.1", 2543, 9543, "gekka"},
	}
}

func seedsHOCONList() []string {
	return []string{
		"pekko://ShowcaseCluster@127.0.0.1:2551",
		"pekko://ShowcaseCluster@127.0.0.1:2552",
	}
}

func peersFor(label string, topo []topoNode) []string {
	var out []string
	for _, n := range topo {
		if n.label == label {
			continue
		}
		out = append(out, fmt.Sprintf("pekko://ShowcaseCluster@%s:%d", n.host, n.port))
	}
	return out
}

func buildScalaCmd(cfg runnerCfg, n topoNode, topo []topoNode) *exec.Cmd {
	args := []string{
		"-Dshowcase.node=" + n.label,
		"-Dshowcase.port=" + fmt.Sprint(n.port),
		"-Dshowcase.mgmt-port=" + fmt.Sprint(n.mgmtPort),
		"-Dshowcase.roles-list.0=showcase-member",
		"-Dshowcase.roles-list.1=singleton-" + n.label,
	}
	for i, s := range seedsHOCONList() {
		args = append(args, fmt.Sprintf("-Dshowcase.seeds-list.%d=%s", i, s))
	}
	for i, p := range peersFor(n.label, topo) {
		args = append(args, fmt.Sprintf("-Dshowcase.peers.%d=%s", i, p))
	}
	args = append(args, "-jar", cfg.ScalaJar)
	return exec.Command("java", args...)
}

func buildGekkaCmd(cfg runnerCfg, n topoNode, topo []topoNode) *exec.Cmd {
	args := []string{
		"--node", n.label,
		"--port", fmt.Sprint(n.port),
		"--mgmt-port", fmt.Sprint(n.mgmtPort),
		"--seeds", "127.0.0.1:2551,127.0.0.1:2552",
		"--roles", "showcase-member,singleton-" + n.label,
		"--peers", strings.Join(peersFor(n.label, topo), ","),
	}
	return exec.Command(cfg.GekkaBin, args...)
}

// runPhases is the entry point called from main.go.
func runPhases(ctx context.Context, cfg runnerCfg) int {
	topo := defaultTopology()
	allowlist, err := loadAllowlist("docs/showcase-allowed-warnings.md")
	if err != nil {
		cfg.Logger.Error("allowlist load failed", "err", err)
		return exitInternalError
	}
	cfg.Logger.Info("allowlist loaded", "count", len(allowlist))

	var children []*child
	stopAll := func(grace time.Duration) {
		for _, c := range children {
			c.stop(grace)
		}
	}
	defer stopAll(teardownBudget)

	// ---- Phase A: Setup ----
	setupCtx, cancel := context.WithTimeout(ctx, setupBudget)
	defer cancel()

	for _, n := range topo {
		if n.kind != "scala" {
			continue
		}
		c, err := spawnChild(setupCtx, n.label, buildScalaCmd(cfg, n, topo), cfg.ArtifactDir)
		if err != nil {
			cfg.Logger.Error("spawn scala", "node", n.label, "err", err)
			return exitSetupFailed
		}
		children = append(children, c)
		if err := awaitReady(setupCtx, c, n.label); err != nil {
			cfg.Logger.Error("setup ready timeout", "node", n.label, "err", err)
			return exitSetupFailed
		}
	}
	for _, n := range topo {
		if n.kind != "gekka" {
			continue
		}
		c, err := spawnChild(setupCtx, n.label, buildGekkaCmd(cfg, n, topo), cfg.ArtifactDir)
		if err != nil {
			cfg.Logger.Error("spawn gekka", "node", n.label, "err", err)
			return exitSetupFailed
		}
		children = append(children, c)
		if err := awaitReady(setupCtx, c, n.label); err != nil {
			cfg.Logger.Error("setup ready timeout", "node", n.label, "err", err)
			return exitSetupFailed
		}
	}

	// ---- Phase B: Gate 1 ----
	endpoints := make([]nodeEndpoint, 0, len(topo))
	for _, n := range topo {
		endpoints = append(endpoints, nodeEndpoint{label: n.label, host: n.host, mgmtPort: n.mgmtPort})
	}
	gate1Deadline := time.Now().Add(gate1Budget)
	for time.Now().Before(gate1Deadline) {
		if err := pollAll(ctx, endpoints, len(topo)); err == nil {
			cfg.Logger.Info("gate 1 PASS — all 8 Up")
			goto gate2
		}
		select {
		case <-ctx.Done():
			return exitInternalError
		case <-time.After(2 * time.Second):
		}
	}
	cfg.Logger.Error("Gate 1 timed out")
	return exitGate1Failed

gate2:
	// ---- Phase C: Gate 2 ----
	gate2Ctx, gate2Cancel := context.WithTimeout(ctx, gateWindow)
	defer gate2Cancel()

	// Membership invariant goroutine
	membFail := make(chan error, 1)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-gate2Ctx.Done():
				return
			case <-t.C:
				if err := pollAll(gate2Ctx, endpoints, len(topo)); err != nil {
					select {
					case membFail <- err:
					default:
					}
					return
				}
			}
		}
	}()

	// Log invariant: fan-in every child's line channel through classifyLine.
	logFail := make(chan classification, 1)
	for _, c := range children {
		go func(c *child) {
			for ln := range c.lines {
				cls := classifyLine(ln.text)
				if cls.level == "" {
					continue
				}
				if cls.level == "ERROR" || !isAllowed(cls, allowlist) {
					select {
					case logFail <- cls:
					default:
					}
					return
				}
			}
		}(c)
	}

	select {
	case <-gate2Ctx.Done():
		cfg.Logger.Info("gate 2 PASS — 10-minute window completed clean")
		return exitOK
	case err := <-membFail:
		cfg.Logger.Error("gate 2 FAIL (membership)", "err", err)
		return exitGate2MemFailed
	case cls := <-logFail:
		cfg.Logger.Error("gate 2 FAIL (log)", "level", cls.level, "text", cls.text)
		return exitGate2LogFailed
	case <-ctx.Done():
		return exitInternalError
	}
}

func awaitReady(ctx context.Context, c *child, label string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case ln, ok := <-c.lines:
			if !ok {
				return errors.New("child exited before ready")
			}
			if strings.Contains(ln.text, "--- SHOWCASE NODE READY: "+label+" ---") {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("ready timeout")
}
