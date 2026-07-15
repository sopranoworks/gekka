//go:build integration

/*
 * cluster_persistence_proxy_two_process_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package gekka

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	hocon "github.com/sopranoworks/gekka-config"
	"github.com/sopranoworks/gekka/persistence"
)

// proxyJournalChildEnv, when set, tells the re-exec'd test binary to run
// as the journal-hosting *child* process rather than as the test itself.
const proxyJournalChildEnv = "GEKKA_PROXY_JOURNAL_TWO_PROC_CHILD"

// twoProcAcctCmd is the command type for the persistent actor driven in
// the two-process proof.  Report=true asks the (recovered) actor to
// publish its current summed balance; otherwise Amount is persisted as
// an event.
type twoProcAcctCmd struct {
	Amount int
	Report bool
}

// twoProcAcctState is the persistent actor's rebuilt-on-replay state.
type twoProcAcctState struct {
	Sum int
}

// TestPersistenceProxyJournal_TwoOSProcesses is the headline correctness
// proof for cross-process Persistence journal forwarding, mirroring the
// bbolt provider's real-process-boundary restart test in spirit: the real
// journal lives in a genuinely separate OS process, and a real
// PersistentActor in *this* process writes events and — via a second,
// freshly-spawned PersistentActor — replays them, entirely over Artery
// remoting.
//
// Why this is a real proof and not a mock:
//   - The journal (persistence.InMemoryJournal) exists ONLY in the child
//     process; the parent never holds a local copy.
//   - The writer actor and the recoverer actor run in two DIFFERENT local
//     actor systems in the parent, neither of which has any local journal.
//     Their sole journal is the off-mode proxy (RemoteJournal) whose bytes
//     travel over the real cluster transport to the child.
//   - Therefore the recoverer observing Sum==42 is only possible if the
//     three events physically crossed the process boundary into the child's
//     journal on write and came back on replay. A broken forwarding path
//     would surface as Sum==0, not a false pass.
func TestPersistenceProxyJournal_TwoOSProcesses(t *testing.T) {
	if os.Getenv(proxyJournalChildEnv) != "" {
		runProxyJournalTargetChild()
		return // unreachable; runProxyJournalTargetChild calls os.Exit
	}

	// ── Launch the journal-hosting child process ────────────────────────────────
	cmd := exec.Command(os.Args[0], "-test.run", "^TestPersistenceProxyJournal_TwoOSProcesses$")
	cmd.Env = append(os.Environ(), proxyJournalChildEnv+"=1")
	cmd.Stderr = os.Stderr // surface child cluster errors in the test log

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	// Closing stdin releases the child from its io.Copy block so it shuts
	// down cleanly; Wait then reaps it.
	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()

	// The child advertises the address of its hosted journal on stdout as
	// "ADDR <system> <host> <port>". Read it (and keep draining stdout so a
	// full pipe can never deadlock the child).
	type childAddr struct {
		system, host string
		port         uint32
	}
	addrCh := make(chan childAddr, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			if !sent && strings.HasPrefix(line, "ADDR ") {
				var s, h string
				var p uint32
				if _, err := fmt.Sscanf(line, "ADDR %s %s %d", &s, &h, &p); err == nil {
					addrCh <- childAddr{system: s, host: h, port: p}
					sent = true
				}
			}
		}
	}()

	var target childAddr
	select {
	case target = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for child to advertise its journal address")
	}

	// ── Parent side: connect to the child over Artery ───────────────────────────
	client, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster client: %v", err)
	}
	defer func() { _ = client.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Join(target.host, target.port); err != nil {
		t.Fatalf("Join child: %v", err)
	}
	if err := client.WaitForHandshake(ctx, target.host, target.port); err != nil {
		t.Fatalf("WaitForHandshake child: %v", err)
	}

	// Build the off-mode proxy journal pointing at the child's hosted
	// target. NewCluster already registered the cluster-backed
	// ProxyTransport, so the "proxy" provider picks it up automatically.
	targetURI := fmt.Sprintf("pekko://%s@%s:%d%s",
		target.system, target.host, target.port, persistence.RemoteJournalPathSuffix)
	cfg, err := hocon.ParseString(fmt.Sprintf(`
target-journal-plugin-id = ignored
start-target-journal = off
target-journal-address = "%s"
init-timeout = 1s
request-timeout = 10s
`, targetURI))
	if err != nil {
		t.Fatalf("parse proxy cfg: %v", err)
	}
	proxyJournal, err := persistence.NewJournal("proxy", *cfg)
	if err != nil {
		t.Fatalf("NewJournal proxy off-mode: %v", err)
	}

	const persistenceID = "acct-42"

	// ── Write phase: a real PersistentActor persists three events across ─────────
	// the process boundary into the child's journal.
	writerSys, err := NewActorSystem("proxy-writer")
	if err != nil {
		t.Fatalf("NewActorSystem writer: %v", err)
	}
	defer writerSys.Terminate()

	persistedCh := make(chan int, 8)
	writeBehavior := &EventSourcedBehavior[twoProcAcctCmd, int, twoProcAcctState]{
		PersistenceID: persistenceID,
		Journal:       proxyJournal,
		InitialState:  twoProcAcctState{},
		CommandHandler: func(_ TypedContext[twoProcAcctCmd], _ twoProcAcctState, c twoProcAcctCmd) Effect[int, twoProcAcctState] {
			return PersistThen[int, twoProcAcctState](func(ns twoProcAcctState) {
				persistedCh <- ns.Sum
			}, c.Amount)
		},
		EventHandler: func(s twoProcAcctState, e int) twoProcAcctState {
			return twoProcAcctState{Sum: s.Sum + e}
		},
	}
	writerRef, err := SpawnPersistent(writerSys, writeBehavior, "acct-42-writer")
	if err != nil {
		t.Fatalf("SpawnPersistent writer: %v", err)
	}

	writerRef.Tell(twoProcAcctCmd{Amount: 10})
	writerRef.Tell(twoProcAcctCmd{Amount: 20})
	writerRef.Tell(twoProcAcctCmd{Amount: 12})

	var lastSum int
	for i := 0; i < 3; i++ {
		select {
		case lastSum = <-persistedCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("persist #%d timed out (event did not reach the child journal)", i+1)
		}
	}
	if lastSum != 42 {
		t.Fatalf("writer final sum = %d, want 42", lastSum)
	}

	// ── Recovery phase: a fresh PersistentActor in a DIFFERENT local system ──────
	// replays the events back out of the child's journal to rebuild its state.
	recoverSys, err := NewActorSystem("proxy-recoverer")
	if err != nil {
		t.Fatalf("NewActorSystem recoverer: %v", err)
	}
	defer recoverSys.Terminate()

	reportCh := make(chan int, 1)
	recoverBehavior := &EventSourcedBehavior[twoProcAcctCmd, int, twoProcAcctState]{
		PersistenceID: persistenceID, // same ID → same journal stream in the child
		Journal:       proxyJournal,  // same remote journal
		InitialState:  twoProcAcctState{},
		CommandHandler: func(_ TypedContext[twoProcAcctCmd], s twoProcAcctState, c twoProcAcctCmd) Effect[int, twoProcAcctState] {
			if c.Report {
				reportCh <- s.Sum
				return None[int, twoProcAcctState]()
			}
			return PersistThen[int, twoProcAcctState](func(twoProcAcctState) {}, c.Amount)
		},
		EventHandler: func(s twoProcAcctState, e int) twoProcAcctState {
			return twoProcAcctState{Sum: s.Sum + e}
		},
	}
	recoverRef, err := SpawnPersistent(recoverSys, recoverBehavior, "acct-42-recoverer")
	if err != nil {
		t.Fatalf("SpawnPersistent recoverer: %v", err)
	}

	// The Report command is stashed until recovery (remote replay) completes,
	// then answered with the rebuilt state.
	recoverRef.Tell(twoProcAcctCmd{Report: true})

	select {
	case got := <-reportCh:
		if got != 42 {
			t.Fatalf("recovered sum = %d, want 42 — events must be replayed from the child process's journal over Artery", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("recovery report timed out (replay from the child journal did not complete)")
	}
}

// runProxyJournalTargetChild is the child half of the two-process proof.
// It stands up a cluster node hosting an in-memory journal at the
// well-known proxy path, advertises its address on stdout, and stays
// alive until the parent closes its stdin — at which point it shuts the
// node down and exits cleanly.
func runProxyJournalTargetChild() {
	target, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child NewCluster: %v\n", err)
		os.Exit(3)
	}
	ServePersistenceProxyJournal(target, persistence.NewInMemoryJournal())

	fmt.Printf("ADDR %s %s %d\n",
		target.localAddr.GetSystem(), target.localAddr.GetHostname(), target.localAddr.GetPort())
	_ = os.Stdout.Sync()

	// Block until the parent signals completion by closing our stdin.
	_, _ = io.Copy(io.Discard, os.Stdin)

	_ = target.Shutdown()
	os.Exit(0)
}

// proxySnapshotChildEnv, when set, tells the re-exec'd test binary to run
// as the snapshot-store-hosting *child* process rather than as the test
// itself.
const proxySnapshotChildEnv = "GEKKA_PROXY_SNAPSHOT_TWO_PROC_CHILD"

// twoProcSnapPayload is the snapshot payload carried across the process
// boundary. Its exact value is asserted on load, so a match is only
// possible if these bytes physically reached the child's store on save
// and came back on load.
const twoProcSnapPayload = "snapshot-crossed-two-processes-7f3a"

// TestPersistenceProxySnapshot_TwoOSProcesses is the SnapshotStore
// counterpart of TestPersistenceProxyJournal_TwoOSProcesses, with the
// identical structure and rigor: the real snapshot-store lives in a
// genuinely separate OS process, the parent saves through an off-mode
// proxy, and a SEPARATE off-mode proxy client in the parent loads it back
// — entirely over Artery remoting.
//
// Why this is a real proof and not a mock:
//   - The snapshot-store (persistence.InMemorySnapshotStore) exists ONLY
//     in the child process; the parent never holds a local copy.
//   - The saving proxy and the loading proxy are two DIFFERENT
//     RemoteSnapshotStore instances in the parent, each of which is a
//     pure forwarder with no local snapshot state. Their sole store is
//     the off-mode proxy whose bytes travel over the real cluster
//     transport to the child.
//   - Therefore the loader observing the exact saved payload + metadata is
//     only possible if the snapshot physically crossed the process
//     boundary into the child's store on save and came back on load. A
//     broken forwarding path would surface as a nil/empty snapshot, not a
//     false positive from local state.
func TestPersistenceProxySnapshot_TwoOSProcesses(t *testing.T) {
	if os.Getenv(proxySnapshotChildEnv) != "" {
		runProxySnapshotTargetChild()
		return // unreachable; runProxySnapshotTargetChild calls os.Exit
	}

	// ── Launch the snapshot-store-hosting child process ─────────────────────────
	cmd := exec.Command(os.Args[0], "-test.run", "^TestPersistenceProxySnapshot_TwoOSProcesses$")
	cmd.Env = append(os.Environ(), proxySnapshotChildEnv+"=1")
	cmd.Stderr = os.Stderr // surface child cluster errors in the test log

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	// Closing stdin releases the child from its io.Copy block so it shuts
	// down cleanly; Wait then reaps it.
	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()

	// The child advertises the address of its hosted snapshot-store on
	// stdout as "ADDR <system> <host> <port>". Read it (and keep draining
	// stdout so a full pipe can never deadlock the child).
	type childAddr struct {
		system, host string
		port         uint32
	}
	addrCh := make(chan childAddr, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			if !sent && strings.HasPrefix(line, "ADDR ") {
				var s, h string
				var p uint32
				if _, err := fmt.Sscanf(line, "ADDR %s %s %d", &s, &h, &p); err == nil {
					addrCh <- childAddr{system: s, host: h, port: p}
					sent = true
				}
			}
		}
	}()

	var target childAddr
	select {
	case target = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for child to advertise its snapshot-store address")
	}

	// ── Parent side: connect to the child over Artery ───────────────────────────
	client, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("NewCluster client: %v", err)
	}
	defer func() { _ = client.Shutdown() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Join(target.host, target.port); err != nil {
		t.Fatalf("Join child: %v", err)
	}
	if err := client.WaitForHandshake(ctx, target.host, target.port); err != nil {
		t.Fatalf("WaitForHandshake child: %v", err)
	}

	targetURI := fmt.Sprintf("pekko://%s@%s:%d%s",
		target.system, target.host, target.port, persistence.RemoteSnapshotPathSuffix)

	// newProxyStore builds a fresh off-mode proxy SnapshotStore pointing at
	// the child. NewCluster already registered the cluster-backed
	// ProxyTransport, so the "proxy" provider picks it up automatically.
	newProxyStore := func(label string) persistence.SnapshotStore {
		cfg, err := hocon.ParseString(fmt.Sprintf(`
target-snapshot-store-plugin-id = ignored
start-target-snapshot-store = off
target-snapshot-store-address = "%s"
init-timeout = 1s
request-timeout = 10s
`, targetURI))
		if err != nil {
			t.Fatalf("parse proxy cfg (%s): %v", label, err)
		}
		store, err := persistence.NewSnapshotStore("proxy", *cfg)
		if err != nil {
			t.Fatalf("NewSnapshotStore proxy off-mode (%s): %v", label, err)
		}
		return store
	}

	const persistenceID = "snap-acct-99"
	meta := persistence.SnapshotMetadata{PersistenceID: persistenceID, SequenceNr: 7, Timestamp: 990099}

	// ── Save phase: the saver proxy pushes a snapshot across the process ─────────
	// boundary into the child's store.
	saver := newProxyStore("saver")
	if err := saver.SaveSnapshot(ctx, meta, twoProcSnapPayload); err != nil {
		t.Fatalf("SaveSnapshot through proxy: %v", err)
	}

	// ── Load phase: a SEPARATE proxy client pulls the snapshot back out of ───────
	// the child's store. Neither proxy holds local snapshot state, so a
	// correct load proves genuine cross-process forwarding.
	loader := newProxyStore("loader")
	loaded, err := loader.LoadSnapshot(ctx, persistenceID, persistence.LatestSnapshotCriteria())
	if err != nil {
		t.Fatalf("LoadSnapshot through separate proxy: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadSnapshot returned nil — snapshot did not survive the round-trip to the child process")
	}
	if loaded.Snapshot != twoProcSnapPayload {
		t.Fatalf("loaded snapshot = %v, want %q — payload must be forwarded to and from the child process over Artery",
			loaded.Snapshot, twoProcSnapPayload)
	}
	if loaded.Metadata.SequenceNr != 7 || loaded.Metadata.Timestamp != 990099 {
		t.Fatalf("loaded metadata = %+v, want seq=7 ts=990099", loaded.Metadata)
	}
}

// runProxySnapshotTargetChild is the child half of the two-process
// snapshot proof. It stands up a cluster node hosting an in-memory
// snapshot-store at the well-known proxy path, advertises its address on
// stdout, and stays alive until the parent closes its stdin — at which
// point it shuts the node down and exits cleanly.
func runProxySnapshotTargetChild() {
	target, err := NewCluster(ClusterConfig{SystemName: "ProxyTgt", Host: "127.0.0.1", Port: 0})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child NewCluster: %v\n", err)
		os.Exit(3)
	}
	ServePersistenceProxySnapshot(target, persistence.NewInMemorySnapshotStore())

	fmt.Printf("ADDR %s %s %d\n",
		target.localAddr.GetSystem(), target.localAddr.GetHostname(), target.localAddr.GetPort())
	_ = os.Stdout.Sync()

	// Block until the parent signals completion by closing our stdin.
	_, _ = io.Copy(io.Discard, os.Stdin)

	_ = target.Shutdown()
	os.Exit(0)
}
