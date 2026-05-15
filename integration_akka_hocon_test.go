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
// These tests reproduce the dashboard's "infinite InitJoin" failure by
// driving NewClusterFromConfig (the dashboard's code path) against a
// production-shaped single-member Akka cluster (configuration-compatibility-
// check.enforce-on-join = on, generic ActorSystem name "TestSystem", random
// free port).
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
// substitutes the two placeholders, writes the result to t.TempDir(), and
// returns the absolute path.
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

// runStrictHoconJoin is the body shared by both the 2.6.21 and 2.6.14
// tests.  It spins up a single-member Akka cluster of the given assembly,
// renders the HOCON template, drives gekka through NewClusterFromConfig +
// JoinSeeds, and asserts that both members reach Up within the budget.
func runStrictHoconJoin(t *testing.T, assembly jvmproc.AssemblyProject) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	seedPort := allocateFreeTCPPort(t)
	localPort := allocateFreeTCPPort(t)

	// 1. Build (or reuse) the fat JAR and spawn the strict node.
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

	// 2. Wait for STRICT_NODE_READY (60 s budget).  Stream the rest of
	//    stdout to the test log so a failure later shows JVM logs.
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(p.Stdout)
		readySent := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			if !readySent && strings.Contains(line, "STRICT_NODE_READY") {
				readySent = true
				close(ready)
			}
		}
	}()

	select {
	case <-ready:
		t.Logf("[scala] STRICT_NODE_READY received (seedPort=%d)", seedPort)
	case <-time.After(60 * time.Second):
		t.Fatalf("Akka strict node did not print STRICT_NODE_READY within 60s")
	}

	// 3. Render HOCON and drive gekka.
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
	defer hsCancel()
	if err := cluster.WaitForHandshake(hsCtx, "127.0.0.1", uint32(seedPort)); err != nil {
		t.Fatalf("WaitForHandshake: %v", err)
	}

	// 4. Poll for both members Up.  This is the assertion the InitJoin
	//    loop will fail.
	deadline := time.After(60 * time.Second)
	for {
		state := cluster.cm.GetState()
		members := state.GetMembers()
		allAddrs := state.GetAllAddresses()

		seedUp := false
		localUp := false
		for _, m := range members {
			if m.GetStatus() != gproto_cluster.MemberStatus_Up {
				continue
			}
			idx := int(m.GetAddressIndex())
			if idx >= len(allAddrs) {
				continue
			}
			port := allAddrs[idx].GetAddress().GetPort()
			switch port {
			case uint32(seedPort):
				seedUp = true
			case uint32(localPort):
				localUp = true
			}
		}

		if seedUp && localUp {
			t.Logf("[gekka] both members Up: seed=%d local=%d", seedPort, localPort)
			return
		}

		select {
		case <-deadline:
			t.Fatalf("members never reached Up within 60s: seedUp=%v localUp=%v "+
				"members=%d (seedPort=%d localPort=%d)",
				seedUp, localUp, len(members), seedPort, localPort)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestAkkaHoconJoin_2_6_21_Strict is the baseline: gekka against a strict,
// production-shaped Akka 2.6.21 cluster via the dashboard's HOCON code
// path.  Expected to PASS — its passing is what proves the harness works.
func TestAkkaHoconJoin_2_6_21_Strict(t *testing.T) {
	runStrictHoconJoin(t, jvmproc.AkkaAssembly)
}

// TestAkkaHoconJoin_2_6_14_Strict is the reproducer: same scenario as the
// 2.6.21 test but pinned to the Akka version the dashboard's production
// cluster runs.  Expected to FAIL until the underlying gekka bug is fixed.
func TestAkkaHoconJoin_2_6_14_Strict(t *testing.T) {
	runStrictHoconJoin(t, jvmproc.Akka2614Assembly)
}
