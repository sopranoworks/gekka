// Package main is the "monitoring" Gekka example.
//
// It demonstrates the built-in health-check and metrics endpoints:
//
//  1. Spawn a node with MonitoringPort set (or EnableMonitoring + port).
//  2. Query /healthz — returns 200 OK once the node has joined and received
//     a cluster Welcome, or 503 with a detail field while still connecting.
//  3. Query /metrics — JSON snapshot of all internal counters.
//     Add ?fmt=prom for Prometheus text exposition format.
//
// # Quick start (standalone — no Pekko seed required)
//
//	go run ./examples/monitoring
//
// The node starts immediately and the monitoring server is reachable:
//
//	curl http://localhost:9090/healthz
//	# → 503 {"status":"not_ready","detail":"waiting for Artery handshake"}
//
//	curl http://localhost:9090/metrics
//	# → {"messages_sent":0,"messages_received":0,...}
//
//	curl "http://localhost:9090/metrics?fmt=prom"
//	# → # HELP gekka_messages_sent_total ...
//
// # With a Pekko seed node
//
// Start the Scala seed (see scala-server/ in the repo), then run:
//
//	SEED_HOST=127.0.0.1 SEED_PORT=2552 go run ./examples/monitoring
//
// After ~5 s the node joins and /healthz switches to 200:
//
//	curl http://localhost:9090/healthz
//	# → 200 {"status":"ok"}
//
// # In-process metrics access
//
// Call node.MetricsSnapshot() from any goroutine to obtain a MetricsSnapshot:
//
//	snap := node.MetricsSnapshot()
//	fmt.Printf("sent=%d recv=%d gossips=%d\n",
//	    snap.MessagesSent, snap.MessagesReceived, snap.GossipsReceived)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sopranoworks/gekka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn with monitoring enabled ────────────────────────────────
	//
	// MonitoringPort is the TCP port for the built-in HTTP server.
	// Setting it to a non-zero value implies EnableMonitoring = true.
	// Use 0 to let the OS pick a free port (see node.MonitoringAddr() below).
	node, err := gekka.Spawn(gekka.ClusterConfig{
		SystemName:     "ClusterSystem",
		Host:           "127.0.0.1",
		Port:           0, // OS-assigned Artery port
		MonitoringPort: monitoringPort(),
	})
	if err != nil {
		log.Fatalf("Spawn: %v", err)
	}
	defer node.Shutdown()

	log.Printf("[gekka] Artery listening on    %s", node.Addr())
	log.Printf("[gekka] Monitoring server on   http://%s", node.MonitoringAddr())
	log.Printf("")
	log.Printf("  /healthz  → readiness probe (200 OK once joined, 503 otherwise)")
	log.Printf("  /metrics  → JSON counter snapshot")
	log.Printf("  /metrics?fmt=prom → Prometheus text format")
	log.Printf("")

	// ── Step 2: Optionally join a seed node ──────────────────────────────────
	seedHost := envOr("SEED_HOST", "")
	seedPort := uint32(envInt("SEED_PORT", 0))

	if seedHost != "" && seedPort != 0 {
		log.Printf("[gekka] joining seed %s:%d …", seedHost, seedPort)
		node.Join(seedHost, seedPort)

		// Wait up to 30 s for the handshake to complete before continuing.
		hsCtx, hsCancel := context.WithTimeout(ctx, 30*time.Second)
		defer hsCancel()
		if err := node.WaitForHandshake(hsCtx, seedHost, seedPort); err != nil {
			log.Printf("[gekka] handshake warning: %v (node still running)", err)
		} else {
			log.Printf("[gekka] Artery handshake complete")
		}
	} else {
		log.Printf("[gekka] no SEED_HOST/SEED_PORT set — running standalone")
		log.Printf("[gekka] /healthz will return 503 until the node joins a cluster")
	}

	// ── Step 3: Periodic in-process metrics dump (every 10 s) ────────────────
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap := node.MetricsSnapshot()
				b, _ := json.MarshalIndent(snap, "  ", "  ")
				log.Printf("[metrics]\n  %s", b)
			}
		}
	}()

	// ── Step 4: Simple demo HTTP frontend (port 8080) ────────────────────────
	//
	// GET /status  — prints a human-readable summary.
	//
	// This is separate from the built-in monitoring server and shows how to
	// combine node.MetricsSnapshot() with application-level endpoints.
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		snap := node.MetricsSnapshot()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "Artery node: %s\n", node.Addr())
		fmt.Fprintf(w, "Monitoring:  http://%s\n\n", node.MonitoringAddr())
		fmt.Fprintf(w, "Messages sent:        %d\n", snap.MessagesSent)
		fmt.Fprintf(w, "Messages received:    %d\n", snap.MessagesReceived)
		fmt.Fprintf(w, "Bytes sent:           %d\n", snap.BytesSent)
		fmt.Fprintf(w, "Bytes received:       %d\n", snap.BytesReceived)
		fmt.Fprintf(w, "Active associations:  %d\n", snap.ActiveAssociations)
		fmt.Fprintf(w, "Gossips received:     %d\n", snap.GossipsReceived)
		fmt.Fprintf(w, "Last convergence:     %s\n", snap.LastConvergenceTime)
		fmt.Fprintf(w, "MemberUp events:      %d\n", snap.MemberUpEvents)
		fmt.Fprintf(w, "MemberRemoved events: %d\n", snap.MemberRemovedEvents)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Printf("[http]  status page on  http://localhost:8080/status")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[http] error: %v", err)
		}
	}()

	// ── Step 5: Graceful shutdown ─────────────────────────────────────────────
	<-ctx.Done()
	log.Println("[gekka] shutting down …")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}

// monitoringPort returns the port for the built-in monitoring server.
// Override with MONITORING_PORT env var; defaults to 9090.
func monitoringPort() int {
	return envInt("MONITORING_PORT", 9090)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
