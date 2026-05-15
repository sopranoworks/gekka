//go:build integration

/*
 * integration_akka_multimember_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// Package gekka — multi-member Akka 2.6.x JOIN reproducer.
//
// Boots a 2-ActorSystem Akka cluster in one JVM (AkkaMultiMemberJoinNode)
// configured to mirror the real TestCluster HOCON shape: SBR enabled with
// defaults, multi-DC=JP, no other overrides.  Drives gekka in as a 3rd
// member and asserts that gekka stays Up for at least 30 seconds after
// being promoted — i.e. the seed does NOT decide to Down gekka after a
// successful Join.
//
// The failing case (gekka observes self-Down within ~1s of being Up)
// is exactly the dashboard / TestCluster symptom that the previous session
// could not root-cause.
package gekka

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gproto_cluster "github.com/sopranoworks/gekka/internal/proto/cluster"
	"github.com/sopranoworks/gekka/test/jvmproc"
)

// multiMemberSignals is the channel set used by the Scala-stdout reader.
type multiMemberSignals struct {
	ready          chan struct{}    // closed once MULTI_NODE_READY arrives
	foreignDowned  chan string      // host:port payloads for FOREIGN_MEMBER_DOWNED
	foreignLeaving chan string      // host:port payloads for FOREIGN_MEMBER_LEFT
	foreignUp      chan string      // host:port payloads for FOREIGN_MEMBER_UP
	tail           []string         // most recent 200 log lines for failure reporting
	tailMu         sync.Mutex       //
	readyOnce      atomic.Bool      // guards readyClose
	readyPayload   atomic.Pointer[string]
}

func newMultiMemberSignals() *multiMemberSignals {
	return &multiMemberSignals{
		ready:          make(chan struct{}),
		foreignDowned:  make(chan string, 32),
		foreignLeaving: make(chan string, 32),
		foreignUp:      make(chan string, 32),
	}
}

func (s *multiMemberSignals) appendTail(line string) {
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	s.tail = append(s.tail, line)
	if len(s.tail) > 200 {
		s.tail = s.tail[len(s.tail)-200:]
	}
}

func (s *multiMemberSignals) dumpTail(t *testing.T, reason string) {
	t.Helper()
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	t.Logf("[scala] %s — last %d log lines:", reason, len(s.tail))
	for _, l := range s.tail {
		t.Logf("  %s", l)
	}
}

// TestAkkaMultiMemberJoin asserts that a gekka node joining a multi-member
// Akka 2.6.x cluster (mirroring TestCluster) reaches Up and STAYS Up for at
// least 30s after promotion.  The bug being reproduced is: gekka is
// promoted to Up, then ~1s later the seed gossips self=Down and gekka
// self-shuts-down via CoordinatedShutdown.
func TestAkkaMultiMemberJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)

	portA := allocateFreeTCPPort(t)
	portB := allocateFreeTCPPort(t)
	gekkaPort := allocateFreeTCPPort(t)

	jar := jvmproc.EnsureAssembly(t, jvmproc.AkkaAssembly)
	p, err := jvmproc.SpawnJava(t, ctx, jar,
		"com.example.AkkaMultiMemberJoinNode", nil, jvmproc.Options{
			Dir: "scala-server",
			JVMFlags: []string{
				"-Dnode.port.a=" + strconv.Itoa(portA),
				"-Dnode.port.b=" + strconv.Itoa(portB),
			},
			PortToRelease: portA,
		})
	if err != nil {
		t.Fatalf("SpawnJava: %v", err)
	}

	sig := newMultiMemberSignals()
	go func() {
		scanner := bufio.NewScanner(p.Stdout)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[SCALA] %s\n", line)
			sig.appendTail(line)

			switch {
			case strings.HasPrefix(line, "MULTI_NODE_READY:"):
				payload := strings.TrimPrefix(line, "MULTI_NODE_READY:")
				sig.readyPayload.Store(&payload)
				if sig.readyOnce.CompareAndSwap(false, true) {
					close(sig.ready)
				}
			case strings.HasPrefix(line, "FOREIGN_MEMBER_UP:"):
				select {
				case sig.foreignUp <- strings.TrimPrefix(line, "FOREIGN_MEMBER_UP:"):
				default:
				}
			case strings.HasPrefix(line, "FOREIGN_MEMBER_LEFT:"):
				select {
				case sig.foreignLeaving <- strings.TrimPrefix(line, "FOREIGN_MEMBER_LEFT:"):
				default:
				}
			case strings.HasPrefix(line, "FOREIGN_MEMBER_DOWNED:"):
				select {
				case sig.foreignDowned <- strings.TrimPrefix(line, "FOREIGN_MEMBER_DOWNED:"):
				default:
				}
			}
		}
	}()

	// 1. Wait for both Akka systems to reach Up.
	select {
	case <-sig.ready:
		t.Logf("[scala] MULTI_NODE_READY (portA=%d, portB=%d)", portA, portB)
	case <-time.After(90 * time.Second):
		sig.dumpTail(t, "MULTI_NODE_READY not seen within 90s")
		t.FailNow()
	}

	// 2. Build gekka pointed at BOTH seeds.  Mirror TestCluster HOCON shape:
	//    multi-DC=JP, SBR provider, no other overrides.
	conf := fmt.Sprintf(`akka {
  remote.artery.canonical {
    hostname = "127.0.0.1"
    port = %d
  }
  cluster {
    seed-nodes = [
      "akka://TestCluster@127.0.0.1:%d",
      "akka://TestCluster@127.0.0.1:%d"
    ]
    multi-data-center.self-data-center = "JP"
    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
  }
}
`, gekkaPort, portA, portB)
	confPath := fmt.Sprintf("%s/cluster.conf", t.TempDir())
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
	if err := cluster.WaitForHandshake(hsCtx, "127.0.0.1", uint32(portA)); err != nil {
		hsCancel()
		sig.dumpTail(t, fmt.Sprintf("WaitForHandshake: %v", err))
		t.FailNow()
	}
	hsCancel()
	t.Logf("[gekka] handshake ASSOCIATED at 127.0.0.1:%d", portA)

	// 3. Wait for the SERVER to confirm gekka is Up.
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
		case <-upDeadline:
			sig.dumpTail(t, fmt.Sprintf("server never promoted gekka:%d to Up within 60s", gekkaPort))
			logGekkaSelfState(t, cluster, gekkaPort)
			t.FailNow()
		}
	}

	// 4. THE ASSERTION: gekka must STAY Up for 30 seconds.  We watch BOTH
	//    sides: any FOREIGN_MEMBER_DOWNED for gekka's port from either
	//    seed AND any local transition out of Up are failures.
	stayUpDeadline := time.After(30 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	t.Logf("[gekka] watching self-status for 30s ...")
	for {
		select {
		case payload := <-sig.foreignDowned:
			if payload == want {
				sig.dumpTail(t, "server downed gekka")
				logGekkaSelfState(t, cluster, gekkaPort)
				t.Fatalf("seed gossiped FOREIGN_MEMBER_DOWNED:%s — bug reproduced", want)
			}
		case payload := <-sig.foreignLeaving:
			if payload == want {
				sig.dumpTail(t, "server observed gekka Leaving")
				logGekkaSelfState(t, cluster, gekkaPort)
				t.Fatalf("seed gossiped FOREIGN_MEMBER_LEFT:%s — gekka left the cluster", want)
			}
		case <-tick.C:
			// Bug-only check: only flag terminal-or-leaving transitions.
			// gekka's local member view legitimately lags the seed's
			// promotion to Up (it may still read Joining after the seed
			// has gossiped Up), so requiring local Up here is over-strict
			// and unrelated to the dashboard self-down bug.
			status := cluster.selfMemberStatus()
			switch status {
			case gproto_cluster.MemberStatus_Down,
				gproto_cluster.MemberStatus_Removed,
				gproto_cluster.MemberStatus_Leaving,
				gproto_cluster.MemberStatus_Exiting:
				logGekkaSelfState(t, cluster, gekkaPort)
				t.Fatalf("gekka self-status transitioned to terminal/leaving state: %s", status)
			}
		case <-stayUpDeadline:
			t.Logf("[gekka] PASS — stayed Up for 30s")
			return
		case <-ctx.Done():
			t.Fatalf("test context expired: %v", ctx.Err())
		}
	}
}

func logGekkaSelfState(t *testing.T, cluster *Cluster, gekkaPort int) {
	t.Helper()
	state := cluster.cm.GetState()
	t.Logf("[gekka] selfMemberStatus()=%s", cluster.selfMemberStatus())
	t.Logf("[gekka] localUid=%d", cluster.cm.LocalAddress.GetUid())
	for i, m := range state.GetMembers() {
		ai := int(m.GetAddressIndex())
		if ai < 0 || ai >= len(state.GetAllAddresses()) {
			continue
		}
		ua := state.GetAllAddresses()[ai]
		a := ua.GetAddress()
		t.Logf("[gekka]   Members[%d]: addrIdx=%d host=%s port=%d uid=%d status=%s upNo=%d",
			i, ai, a.GetHostname(), a.GetPort(), ua.GetUid(), m.GetStatus(), m.GetUpNumber())
	}
}
