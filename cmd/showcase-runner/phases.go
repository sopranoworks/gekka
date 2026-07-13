// cmd/showcase-runner/phases.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
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
	// Diagnostic escape hatch: SHOWCASE_GEKKA_VERBOSE=1 turns on the gekka
	// nodes' verbose cluster logging (gossip/heartbeat/phi debug lines).
	if os.Getenv("SHOWCASE_GEKKA_VERBOSE") == "1" {
		args = append(args, "--verbose")
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

	// Every child's lines channel must be consumed CONTINUOUSLY from spawn
	// to process exit. The channel holds 256 lines; once it fills, the
	// pipeReaders stop reading the OS pipe, the pipe buffer fills, and the
	// child's own stdout writes BLOCK — freezing whatever goroutine inside
	// the child is logging (observed: a gekka node's heartbeat and inbound
	// control-stream goroutines wedged mid-Gate-1 for 72s, so every JVM
	// peer's failure detector flagged it and SBR downed the cluster).
	// Each pump discards lines until Gate 2 arms classification, then
	// applies the log invariant to lines within the gate window only.
	var gate2Active atomic.Bool
	logFail := make(chan classification, 1)
	pump := func(c *child) {
		go func() {
			for ln := range c.lines {
				if !gate2Active.Load() {
					continue
				}
				cls := classifyLine(ln.text)
				if cls.level == "" {
					continue
				}
				if cls.level == "ERROR" || !isAllowed(cls, allowlist) {
					select {
					case logFail <- cls:
					default:
					}
					// Keep draining so the child never wedges; classification
					// stops after the first violation is reported.
					for range c.lines {
					}
					return
				}
			}
		}()
	}

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
		pump(c)
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
		pump(c)
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
		err := pollAll(ctx, endpoints, len(topo))
		if err == nil {
			cfg.Logger.Info("gate 1 PASS — all 8 Up")
			goto gate2
		}
		cfg.Logger.Info("gate 1 poll not converged yet", "err", err.Error())
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

	// Log invariant: the per-child pumps have been discarding lines since
	// spawn; arming gate2Active scopes classification to the 10-minute
	// steady-state window (spec §5). Setup-phase warmup noise (e.g. ask
	// timeouts to peers that had not reached Up yet) predates the window
	// and must not fail it.
	gate2Active.Store(true)

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

// awaitReady waits for the child's READY sentinel on its dedicated tap
// (`c.ready`, closed by pipeReader). It concurrently drains `c.lines` so the
// producer can keep reading the OS pipe — setup-phase lines are not
// classified, mirroring the prior behaviour where awaitReady discarded them.
//
// The `label` parameter is kept for diagnostic context; the actual sentinel
// match is performed inline by pipeReader (responsibility split).
func awaitReady(ctx context.Context, c *child, label string) error {
	_ = label
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-c.ready:
			return nil
		case _, ok := <-c.lines:
			if !ok {
				return errors.New("child exited before ready")
			}
		case <-c.done:
			return errors.New("child exited before ready")
		case <-timeout:
			return errors.New("ready timeout")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
