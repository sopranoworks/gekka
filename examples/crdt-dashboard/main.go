// Package main demonstrates Gekka's Replicator (CRDT engine) with a live
// terminal dashboard.
//
// Two CRDTs are maintained cluster-wide:
//
//   - GCounter "total_requests" — incremented by every Go node on each tick;
//     global value = sum of all nodes' contributions (pairwise-max merge).
//
//   - ORSet "active_nodes" — each node adds its own address on startup and
//     removes it on graceful shutdown; the set reflects current membership.
//
// The dashboard clears and repaints the terminal every 2 s.  Eventual
// consistency is visible: run two instances of this program and watch the
// GCounter and ORSet converge on both terminals after a gossip round-trip.
//
// Topology:
//
//	Go dashboard-1 ─┐
//	Go dashboard-2 ─┼──► Scala GoReplicator (/user/goReplicator) ─► merge + reply
//	Go dashboard-N ─┘
//
// Run alongside a Pekko GoReplicator server — see README.md.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sopranoworks/gekka"
)

const (
	// dashInner is the printable width inside the box borders.
	dashInner = 54
	// refreshInterval controls how often the dashboard repaints.
	refreshInterval = 2 * time.Second
)

func main() {
	// Route all library log output to stderr so the dashboard owns stdout.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn ─────────────────────────────────────────────────────
	//
	// SpawnFromConfig reads application.conf.  Port 0 lets the OS assign a
	// free port, so you can run multiple instances simultaneously.
	node, err := gekka.NewClusterFromConfig("application.conf")
	if err != nil {
		log.Fatalf("SpawnFromConfig: %v", err)
	}
	defer node.Shutdown()

	self := node.SelfAddress()
	myAddr := fmt.Sprintf("%s:%d", self.Host, self.Port)
	log.Printf("node up at %s (system=%s)", myAddr, self.System)

	seeds := node.Seeds()
	if len(seeds) == 0 {
		log.Fatal("no seed-nodes in application.conf")
	}
	seed := seeds[0]

	// ── Step 2: Wire the Replicator ───────────────────────────────────────
	//
	// AddPeer registers the Scala GoReplicator as a gossip target.
	// The Scala actor merges incoming state and gossips the union back,
	// acting as a hub that bridges all Go instances without requiring
	// direct Go-to-Go connections.
	repl := node.Replicator()
	repl.GossipInterval = refreshInterval

	goReplPath := seed.WithRoot("user").Child("goReplicator")
	repl.AddPeer(goReplPath.String())
	log.Printf("gossip peer: %s", goReplPath)

	// ── Step 3: Route incoming gossip to the Replicator ───────────────────
	//
	// OnMessage forwards raw JSON payloads (identified by leading '{') to
	// HandleIncoming.  That merges the received CRDT snapshot into local
	// state and logs the new value.
	node.OnMessage(func(ctx context.Context, msg *gekka.IncomingMessage) error {
		if len(msg.Payload) > 0 && msg.Payload[0] == '{' {
			if err := repl.HandleIncoming(msg.Payload); err != nil {
				log.Printf("HandleIncoming: %v", err)
			}
		}
		return nil
	})

	// ── Step 4: Join the cluster ─────────────────────────────────────────
	log.Printf("joining cluster via %s …", seed)
	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("JoinSeeds: %v", err)
	}
	if err := node.WaitForHandshake(ctx, seed.Host, uint32(seed.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("Artery handshake complete.")

	// ── Step 5: Initialise CRDT state ────────────────────────────────────
	//
	// Add this node's address to the active_nodes ORSet so all peers know
	// it is online.  WriteAll pushes the update immediately in addition to
	// the periodic gossip tick.
	repl.AddToSet("active_nodes", myAddr, gekka.WriteAll)

	// Seed the GCounter key so it appears in the dashboard immediately.
	repl.GCounter("total_requests")

	// ── Step 6: Start the gossip engine ──────────────────────────────────
	//
	// Start spawns a background goroutine that calls gossipAll every
	// GossipInterval.  The goroutine exits when ctx is cancelled or
	// Stop() is called.
	repl.Start(ctx)
	defer repl.Stop()

	// ── Step 7: Dashboard loop ────────────────────────────────────────────
	//
	// Every refreshInterval:
	//   a) Increment total_requests (this node's contribution).
	//   b) Repaint the terminal with current CRDT state.
	//
	// Between repaints, incoming gossip from Scala merges peer contributions
	// into local state — eventual consistency becomes visible in real time.
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	var myRequests uint64

	// Print an initial empty dashboard while waiting for the first tick.
	printDashboard(repl, myAddr, self.System, myRequests)

	for {
		select {
		case <-ticker.C:
			myRequests++
			repl.IncrementCounter("total_requests", 1, gekka.WriteLocal)
			printDashboard(repl, myAddr, self.System, myRequests)

		// ── Step 8: Graceful shutdown ─────────────────────────────────
		//
		// Remove this node from the active_nodes ORSet before leaving so
		// peers stop advertising it immediately.  WriteAll pushes the
		// remove to all registered gossip peers synchronously.
		case <-ctx.Done():
			fmt.Println("\n[crdt] removing self from active_nodes …")
			repl.RemoveFromSet("active_nodes", myAddr, gekka.WriteAll)
			time.Sleep(500 * time.Millisecond) // let WriteAll propagate

			fmt.Println("[crdt] leaving cluster …")
			if err := node.Leave(); err != nil {
				log.Printf("Leave: %v", err)
			}
			time.Sleep(2 * time.Second)
			return
		}
	}
}

// ── Dashboard rendering ───────────────────────────────────────────────────────

// printDashboard clears the terminal and draws the current CRDT state.
func printDashboard(repl *gekka.Replicator, myAddr, system string, myRequests uint64) {
	counter := repl.GCounter("total_requests")
	nodeSet := repl.ORSet("active_nodes")
	members := nodeSet.Elements()

	top := "┌" + strings.Repeat("─", dashInner+2) + "┐"
	sep := "├" + strings.Repeat("─", dashInner+2) + "┤"
	bot := "└" + strings.Repeat("─", dashInner+2) + "┘"

	// Clear terminal and move cursor to top-left.
	fmt.Print("\033[H\033[2J")

	fmt.Println(top)
	printLine(center("Cluster CRDT Dashboard", dashInner))
	printLine(center("system: "+system, dashInner))
	printLine(center("node:   "+myAddr, dashInner))
	fmt.Println(sep)

	// G-Counter section.
	printLine("  GCounter — total_requests")
	printLine(fmt.Sprintf("    Global value : %d", counter.Value()))
	printLine(fmt.Sprintf("    My increment : %d", myRequests))
	fmt.Println(sep)

	// OR-Set section.
	printLine("  ORSet — active_nodes")
	if len(members) == 0 {
		printLine("    (empty — waiting for gossip convergence)")
	}
	for _, m := range members {
		label := "    " + m
		if m == myAddr {
			label += "  ← this node"
		}
		printLine(label)
	}
	fmt.Println(sep)

	// Footer.
	printLine("  Updated : " + time.Now().Format("2006-01-02 15:04:05"))
	printLine("  Gossip  : every " + repl.GossipInterval.String())
	fmt.Println(bot)
	fmt.Println("  Press Ctrl-C to leave gracefully.")
}

// printLine renders a single row inside the box, padding to dashInner width.
func printLine(s string) {
	// Trim if the string is too long to fit.
	if len(s) > dashInner {
		s = s[:dashInner-1] + "…"
	}
	padding := dashInner - len(s)
	fmt.Printf("│ %s%s │\n", s, strings.Repeat(" ", padding))
}

// center returns s horizontally centred within width characters.
func center(s string, width int) string {
	if len(s) >= width {
		return s
	}
	pad := (width - len(s)) / 2
	return strings.Repeat(" ", pad) + s
}
