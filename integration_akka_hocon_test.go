//go:build integration

/*
 * integration_akka_hocon_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — strict-HOCON JOIN reproducer against Akka 2.6.x.
//
// The previous version of these tests asserted on the joiner's LOCAL
// cluster state ("gekka thinks it's Up").  That self-report is unreliable:
// when the joiner's outbound messages are dropped server-side (e.g. Artery
// compression-id desync), gekka happily counts pre-existing members as Up
// while the SERVER still has the joiner pinned at Joining and is
// rejecting every subsequent message.  TestCluster's logs proved that exact
// failure mode.
//
// This rewrite asserts on the SERVER's authoritative view, surfaced via
// the AkkaStrictHoconJoinNode scenario which prints
//
//	STRICT_FOREIGN_MEMBER_UP:<host>:<port>
//
// to stdout when (and only when) a non-self member is promoted past
// Joining.  The Go test waits for that signal — gekka's local member list
// is logged for debugging but is NOT load-bearing.
//
// Run with:
//
//	go test -v -tags integration -run TestAkkaHoconJoin -timeout 300s .
package gekka

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/test/jvmproc"
)

// allocateFreeTCPPort returns a TCP port that was free at the moment of
// the call.  There is a small race between Close and the caller's bind,
// but for in-test single-host use the failure rate is negligible.
func allocateFreeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocateFreeTCPPort: listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("allocateFreeTCPPort: close: %v", err)
	}
	return port
}

// renderStrictHoconConf reads testdata/akka_strict_hocon.conf.tmpl,
// substitutes the two placeholders, writes the result to t.TempDir(),
// and returns the absolute path.
func renderStrictHoconConf(t *testing.T, localPort, seedPort int) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/akka_strict_hocon.conf.tmpl")
	if err != nil {
		t.Fatalf("renderStrictHoconConf: read template: %v", err)
	}
	rendered := strings.ReplaceAll(string(raw), "__LOCAL_PORT__", strconv.Itoa(localPort))
	rendered = strings.ReplaceAll(rendered, "__SEED_PORT__", strconv.Itoa(seedPort))
	out := filepath.Join(t.TempDir(), "cluster.conf")
	if err := os.WriteFile(out, []byte(rendered), 0o600); err != nil {
		t.Fatalf("renderStrictHoconConf: write %s: %v", out, err)
	}
	return out
}

// jvmSignals collects parseable lines emitted by AkkaStrictHoconJoinNode.
type jvmSignals struct {
	ready               chan struct{}
	foreignMemberUp     chan string // payload: "<host>:<port>"
	foreignMemberUpSeen sync.Map    // dedupe by host:port
	stdoutTail          []string    // last N lines, kept for failure diagnostics
	stdoutMu            sync.Mutex
	stdoutTailMaxLines  int
}

func newJvmSignals() *jvmSignals {
	return &jvmSignals{
		ready:              make(chan struct{}),
		foreignMemberUp:    make(chan string, 16),
		stdoutTailMaxLines: 200,
	}
}

func (s *jvmSignals) appendTail(line string) {
	s.stdoutMu.Lock()
	defer s.stdoutMu.Unlock()
	s.stdoutTail = append(s.stdoutTail, line)
	if len(s.stdoutTail) > s.stdoutTailMaxLines {
		s.stdoutTail = s.stdoutTail[len(s.stdoutTail)-s.stdoutTailMaxLines:]
	}
}

func (s *jvmSignals) snapshotTail() []string {
	s.stdoutMu.Lock()
	defer s.stdoutMu.Unlock()
	out := make([]string, len(s.stdoutTail))
	copy(out, s.stdoutTail)
	return out
}

// runStrictHoconJoin is the body shared by both the 2.6.21 and 2.6.14
// tests.  Spins up a single-member Akka cluster of the given assembly,
// drives gekka through NewClusterFromConfig + JoinSeeds, and asserts
// that the SERVER promotes gekka past Joining.
func runStrictHoconJoin(t *testing.T, assembly jvmproc.AssemblyProject) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	seedPort := allocateFreeTCPPort(t)
	localPort := allocateFreeTCPPort(t)

	jar := jvmproc.EnsureAssembly(t, assembly)
	p, err := jvmproc.SpawnJava(t, ctx, jar,
		"com.example.AkkaStrictHoconJoinNode", nil, jvmproc.Options{
			Dir:           "scala-server",
			JVMFlags:      []string{"-Dnode.port=" + strconv.Itoa(seedPort)},
			PortToRelease: seedPort,
		})
	if err != nil {
		t.Fatalf("SpawnJava: %v", err)
	}

	sig := newJvmSignals()
	go func() {
		scanner := bufio.NewScanner(p.Stdout)
		// Some Akka log lines are long; raise the buffer cap.
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		readyOnce := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			sig.appendTail(line)

			if !readyOnce && strings.Contains(line, "STRICT_NODE_READY") {
				readyOnce = true
				close(sig.ready)
			}
			if strings.HasPrefix(line, "STRICT_FOREIGN_MEMBER_UP:") {
				payload := strings.TrimPrefix(line, "STRICT_FOREIGN_MEMBER_UP:")
				if _, dup := sig.foreignMemberUpSeen.LoadOrStore(payload, true); !dup {
					select {
					case sig.foreignMemberUp <- payload:
					default:
					}
				}
			}
		}
	}()

	// 1. Wait for the seed to reach Up (cluster of 1, multi-DC).
	select {
	case <-sig.ready:
		t.Logf("[scala] STRICT_NODE_READY received (seedPort=%d, dc=test)", seedPort)
	case <-time.After(60 * time.Second):
		dumpJvmTail(t, sig,
			"Akka strict node did not print STRICT_NODE_READY within 60s")
		t.FailNow()
	}

	// 2. Build the gekka HOCON, drive Join.
	confPath := renderStrictHoconConf(t, localPort, seedPort)
	t.Logf("[gekka] HOCON written to %s (localPort=%d, seedPort=%d)",
		confPath, localPort, seedPort)

	cluster, err := NewClusterFromConfig(confPath)
	if err != nil {
		t.Fatalf("NewClusterFromConfig(%s): %v", confPath, err)
	}
	t.Cleanup(func() { _ = cluster.Shutdown() })

	if err := cluster.JoinSeeds(); err != nil {
		t.Fatalf("JoinSeeds: %v", err)
	}

	hsCtx, hsCancel := context.WithTimeout(ctx, 60*time.Second)
	if err := cluster.WaitForHandshake(hsCtx, "127.0.0.1", uint32(seedPort)); err != nil {
		hsCancel()
		dumpJvmTail(t, sig, fmt.Sprintf("WaitForHandshake: %v", err))
		t.FailNow()
	}
	hsCancel()
	t.Logf("[gekka] handshake ASSOCIATED at 127.0.0.1:%d", seedPort)

	// 3. THE assertion: wait for the SERVER to print STRICT_FOREIGN_MEMBER_UP
	//    naming gekka's bind port.  This is the only authoritative signal
	//    that gekka was actually accepted into the cluster — gekka's local
	//    view is logged below for diagnostics but is not load-bearing.
	want := fmt.Sprintf("127.0.0.1:%d", localPort)
	deadline := time.After(90 * time.Second)
	gossipTicker := time.NewTicker(5 * time.Second)
	defer gossipTicker.Stop()

	for {
		select {
		case payload := <-sig.foreignMemberUp:
			if payload == want {
				t.Logf("[scala] SERVER promoted joiner to Up: %s — PASS", payload)
				return
			}
			t.Logf("[scala] STRICT_FOREIGN_MEMBER_UP received for %q (waiting for %q)",
				payload, want)

		case <-gossipTicker.C:
			logGekkaView(t, cluster, seedPort, localPort)

		case <-deadline:
			logGekkaView(t, cluster, seedPort, localPort)
			dumpJvmTail(t, sig, fmt.Sprintf(
				"server never promoted joiner %s to Up within 90s after handshake — JOIN rejected server-side",
				want))
			t.FailNow()
		}
	}
}

// logGekkaView prints gekka's local cluster state for diagnostics.  Do not
// use this as an assertion — gekka's local view can claim members are Up
// even when the server has dropped every message from gekka.
func logGekkaView(t *testing.T, cluster *Cluster, seedPort, localPort int) {
	t.Helper()
	state := cluster.cm.GetState()
	members := state.GetMembers()
	allAddrs := state.GetAllAddresses()
	parts := make([]string, 0, len(members))
	for _, m := range members {
		idx := int(m.GetAddressIndex())
		if idx >= len(allAddrs) {
			continue
		}
		port := allAddrs[idx].GetAddress().GetPort()
		role := ""
		switch port {
		case uint32(seedPort):
			role = "seed"
		case uint32(localPort):
			role = "self"
		default:
			role = "?"
		}
		parts = append(parts, fmt.Sprintf("%s:%d=%s",
			role, port, friendlyStatus(m.GetStatus())))
	}
	t.Logf("[gekka-local-view] %s", strings.Join(parts, " "))
}

func friendlyStatus(s gproto_cluster.MemberStatus) string {
	switch s {
	case gproto_cluster.MemberStatus_Joining:
		return "Joining"
	case gproto_cluster.MemberStatus_Up:
		return "Up"
	case gproto_cluster.MemberStatus_Leaving:
		return "Leaving"
	case gproto_cluster.MemberStatus_Exiting:
		return "Exiting"
	case gproto_cluster.MemberStatus_Down:
		return "Down"
	case gproto_cluster.MemberStatus_Removed:
		return "Removed"
	case gproto_cluster.MemberStatus_WeaklyUp:
		return "WeaklyUp"
	default:
		return s.String()
	}
}

// dumpJvmTail prints the last N stdout lines from the JVM scenario before
// failing.  This is what surfaces the actual server-side rejection
// (e.g. compression desync, JoinConfigCompatChecker rejection) that the
// previous "trust gekka's local view" assertion was hiding.
func dumpJvmTail(t *testing.T, sig *jvmSignals, msg string) {
	t.Helper()
	t.Errorf("%s", msg)
	tail := sig.snapshotTail()
	t.Logf("---- last %d JVM stdout lines ----", len(tail))
	for _, l := range tail {
		t.Logf("  %s", l)
	}
	t.Logf("---- end JVM stdout ----")
}

// TestAkkaHoconJoin_2_6_21_Strict drives gekka against a strict, prod-shaped
// Akka 2.6.21 cluster via the dashboard's HOCON code path.  Asserts on the
// SERVER's MemberUp event for the joiner.
func TestAkkaHoconJoin_2_6_21_Strict(t *testing.T) {
	runStrictHoconJoin(t, jvmproc.AkkaAssembly)
}

// TestAkkaHoconJoin_2_6_14_Strict mirrors the 2.6.21 test against the Akka
// version the production dashboard's cluster runs.
func TestAkkaHoconJoin_2_6_14_Strict(t *testing.T) {
	runStrictHoconJoin(t, jvmproc.Akka2614Assembly)
}
