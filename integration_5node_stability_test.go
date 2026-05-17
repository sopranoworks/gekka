//go:build integration

/*
 * integration_5node_stability_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// TestFiveNodeStability3Min — five Scala/Pekko cluster ActorSystems
// inside one JVM, gekka joins as the 6th member, then NOTHING is allowed
// to fail for three minutes.  Any ERROR-prefixed line from the Scala
// stdout (which the in-JVM watcher emits for MemberDowned, MemberRemoved,
// MemberLeft, MemberExited, UnreachableMember) fails the test; any
// transition out of Up in gekka's own local state also fails the test.
//
// This test exists to make the dashboard self-down regression
// reproducible from a clean automated environment, without relying on the
// user's production cluster.  If the gossip-ingress filter (and other
// stability fixes) are intact, this test passes deterministically.  If a
// regression reintroduces collateral downing or self-down, this test
// fails within seconds.

package gekka

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/test/jvmproc"
)

// stabilitySignals is the read-only channel set the Scala-stdout scanner
// publishes events to.  Test goroutines select on these to detect both
// readiness and any failure signal.
type stabilitySignals struct {
	ready     chan struct{} // closed once FIVE_NODE_READY arrives
	foreignUp chan string   // host:port payloads for FOREIGN_MEMBER_UP
	errored   chan string   // ANY "ERROR:" stdout line; first wins

	readyOnce atomic.Bool
	errOnce   atomic.Bool

	tailMu sync.Mutex
	tail   []string
}

func newStabilitySignals() *stabilitySignals {
	return &stabilitySignals{
		ready:     make(chan struct{}),
		foreignUp: make(chan string, 32),
		errored:   make(chan string, 1),
	}
}

func (s *stabilitySignals) appendTail(line string) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	s.tail = append(s.tail, line)
	if len(s.tail) > 200 {
		s.tail = s.tail[len(s.tail)-200:]
	}
}

func (s *stabilitySignals) dumpTail(t *testing.T, reason string) {
	t.Helper()
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	t.Logf("[scala] %s — last %d log lines:", reason, len(s.tail))
	for _, l := range s.tail {
		t.Logf("  %s", l)
	}
}

func TestFiveNodeStability3Min(t *testing.T) {
	// 4-minute test context: 60 s for JVM startup + cluster convergence,
	// then 180 s of hold, plus margin for the gekka-join handshake.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute+30*time.Second)
	t.Cleanup(cancel)

	const numScalaNodes = 5
	scalaPorts := make([]int, numScalaNodes)
	for i := range scalaPorts {
		scalaPorts[i] = allocateFreeTCPPort(t)
	}
	gekkaPort := allocateFreeTCPPort(t)

	jar := jvmproc.EnsureAssembly(t, jvmproc.PekkoAssembly)
	// holdEndSignal is the file Scala's StabilityWatcher polls per cluster
	// event to decide whether MemberLeft/Exited/Downed/Removed transitions
	// should be reported as "ERROR:" (real hold-window failure) or
	// "POST_HOLD:" (expected during gekka's teardown after PASS).  Written
	// once, just before the PASS return; absent means "still in hold".
	tempDir := t.TempDir()
	holdEndSignal := tempDir + "/hold-ended"
	jvmFlags := make([]string, 0, numScalaNodes+1)
	for i, port := range scalaPorts {
		jvmFlags = append(jvmFlags, fmt.Sprintf("-Dnode.port.%d=%d", i+1, port))
	}
	jvmFlags = append(jvmFlags, "-Dgekka.hold.signal="+holdEndSignal)
	p, err := jvmproc.SpawnJava(t, ctx, jar, "com.example.FiveNodeStableCluster", nil, jvmproc.Options{
		Dir:           "scala-server",
		JVMFlags:      jvmFlags,
		PortToRelease: scalaPorts[0],
	})
	if err != nil {
		t.Fatalf("SpawnJava: %v", err)
	}

	sig := newStabilitySignals()
	go func() {
		scanner := bufio.NewScanner(p.Stdout)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			sig.appendTail(line)

			switch {
			case strings.HasPrefix(line, "FIVE_NODE_READY:"):
				if sig.readyOnce.CompareAndSwap(false, true) {
					close(sig.ready)
				}
			case strings.HasPrefix(line, "FOREIGN_MEMBER_UP:"):
				select {
				case sig.foreignUp <- strings.TrimPrefix(line, "FOREIGN_MEMBER_UP:"):
				default:
				}
			case strings.HasPrefix(line, "ERROR:"):
				if sig.errOnce.CompareAndSwap(false, true) {
					select {
					case sig.errored <- strings.TrimPrefix(line, "ERROR:"):
					default:
					}
				}
			}
		}
	}()

	// ── 1. Wait for all five Scala nodes to be Up ────────────────────────
	select {
	case <-sig.ready:
		t.Logf("[scala] FIVE_NODE_READY (ports=%v)", scalaPorts)
	case errMsg := <-sig.errored:
		sig.dumpTail(t, fmt.Sprintf("ERROR during startup: %s", errMsg))
		t.Fatalf("Scala emitted ERROR before READY: %s", errMsg)
	case <-time.After(90 * time.Second):
		sig.dumpTail(t, "FIVE_NODE_READY not seen within 90s")
		t.FailNow()
	}

	// ── 2. Build gekka, point it at all five Scala seeds ─────────────────
	seedNodes := make([]string, numScalaNodes)
	for i, port := range scalaPorts {
		seedNodes[i] = fmt.Sprintf("\"pekko://StableCluster@127.0.0.1:%d\"", port)
	}
	conf := fmt.Sprintf(`pekko {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = %d
  }
  cluster {
    seed-nodes = [%s]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver {
      active-strategy = "keep-oldest"
      stable-after = 30s
    }
  }
}
`, gekkaPort, strings.Join(seedNodes, ","))
	confPath := fmt.Sprintf("%s/cluster.conf", tempDir)
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		t.Fatalf("write conf: %v", err)
	}

	cluster, err := NewClusterFromConfig(confPath)
	if err != nil {
		t.Fatalf("NewClusterFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Shutdown() })

	if err := cluster.JoinSeeds(); err != nil {
		t.Fatalf("JoinSeeds: %v", err)
	}

	hsCtx, hsCancel := context.WithTimeout(ctx, 60*time.Second)
	if err := cluster.WaitForHandshake(hsCtx, "127.0.0.1", uint32(scalaPorts[0])); err != nil {
		hsCancel()
		sig.dumpTail(t, fmt.Sprintf("WaitForHandshake: %v", err))
		t.FailNow()
	}
	hsCancel()
	t.Logf("[gekka] handshake ASSOCIATED at 127.0.0.1:%d", scalaPorts[0])

	// ── 3. Wait for the cluster to confirm gekka is Up ───────────────────
	want := fmt.Sprintf("127.0.0.1:%d", gekkaPort)
	gotUp := false
	upDeadline := time.After(60 * time.Second)
	for !gotUp {
		select {
		case payload := <-sig.foreignUp:
			if payload == want {
				gotUp = true
				t.Logf("[scala] FOREIGN_MEMBER_UP for gekka:%d — promoted to Up", gekkaPort)
			} else {
				t.Logf("[scala] FOREIGN_MEMBER_UP for other (%s), waiting for %s", payload, want)
			}
		case errMsg := <-sig.errored:
			sig.dumpTail(t, fmt.Sprintf("ERROR during gekka join: %s", errMsg))
			t.Fatalf("Scala emitted ERROR before gekka reached Up: %s", errMsg)
		case <-upDeadline:
			sig.dumpTail(t, fmt.Sprintf("Scala never promoted gekka:%d to Up within 60s", gekkaPort))
			t.FailNow()
		}
	}

	// ── 4. THE ASSERTION: hold steady for 3 minutes ──────────────────────
	const holdWindow = 3 * time.Minute
	stableUntil := time.After(holdWindow)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	t.Logf("[gekka] holding %v for any error or non-Up local transition ...", holdWindow)
	for {
		select {
		case errMsg := <-sig.errored:
			sig.dumpTail(t, fmt.Sprintf("ERROR during stability hold: %s", errMsg))
			logGekkaSelfState(t, cluster, gekkaPort)
			t.Fatalf("Scala emitted ERROR during 3-min hold: %s", errMsg)
		case <-tick.C:
			status := cluster.selfMemberStatus()
			switch status {
			case gproto_cluster.MemberStatus_Down,
				gproto_cluster.MemberStatus_Removed,
				gproto_cluster.MemberStatus_Leaving,
				gproto_cluster.MemberStatus_Exiting:
				logGekkaSelfState(t, cluster, gekkaPort)
				t.Fatalf("gekka self-status transitioned to %s during 3-min hold — dashboard self-down regressed", status)
			}
		case <-stableUntil:
			// Signal Scala-side StabilityWatcher that the hold window has
			// elapsed BEFORE letting t.Cleanup run cluster.Shutdown() — once
			// the file exists, watcher emits POST_HOLD: instead of ERROR:
			// for the imminent MemberLeft/Exited/Removed sequence triggered
			// by gekka's graceful departure.  fs.WriteFile is durable enough
			// (returns after pagecache update) for the next Pekko event tick.
			if werr := os.WriteFile(holdEndSignal, []byte("ended"), 0o600); werr != nil {
				t.Logf("warn: write hold-end signal: %v", werr)
			}
			t.Logf("[PASS] held Up for %v with no errors from any of %d Scala nodes + gekka", holdWindow, numScalaNodes)
			return
		case <-ctx.Done():
			t.Fatalf("test context expired during hold: %v", ctx.Err())
		}
	}
}
