// Package main is the "ask-pattern" Gekka example.
//
// It demonstrates the Ask (request-response) API:
//
//  1. Spawn a node and connect to a remote Pekko actor system.
//  2. Call node.Ask — which blocks until the remote actor replies or the
//     context deadline is exceeded.
//  3. Expose the replies over HTTP so any browser or curl can trigger an Ask.
//
// Prerequisites:
//   - A Pekko RemoteSystem running at 127.0.0.1:2552 with a /user/echo actor
//     that echoes Array[Byte] messages (see the scala-server in the repo).
//
// Run:
//
//	go run ./examples/ask-pattern
//	curl "http://localhost:8080/ask?msg=hello"
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gekka"
	"gekka/actor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Step 1: Spawn ────────────────────────────────────────────────────────
	node, err := gekka.SpawnFromConfig("application.conf")
	if err != nil {
		log.Fatalf("SpawnFromConfig: %v", err)
	}
	defer node.Shutdown()
	log.Printf("[gekka] node listening on %s", node.Addr())

	// ── Step 2: Connect to the remote echo actor ─────────────────────────────
	remote := actor.Address{Protocol: "pekko", System: "RemoteSystem", Host: "127.0.0.1", Port: 2552}
	echoPath := remote.WithRoot("user").Child("echo")

	// Initiate the Artery handshake (non-blocking probe).
	go func() {
		if err := node.Send(ctx, echoPath, []byte("probe")); err != nil {
			log.Printf("[gekka] probe send error: %v", err)
		}
	}()

	log.Printf("[gekka] waiting for Artery handshake with %s …", remote)
	if err := node.WaitForHandshake(ctx, remote.Host, uint32(remote.Port)); err != nil {
		log.Fatalf("WaitForHandshake: %v", err)
	}
	log.Printf("[gekka] handshake complete — ready to Ask")

	// ── Step 3: HTTP server ──────────────────────────────────────────────────
	//
	// GET /ask?msg=<text>
	//   Sends <text> to the Pekko echo actor via Ask and returns the reply.
	//
	// The handler creates a 5-second deadline for each Ask so slow or
	// unreachable remote actors do not stall the HTTP client forever.
	mux := http.NewServeMux()
	mux.HandleFunc("/ask", func(w http.ResponseWriter, r *http.Request) {
		msg := r.URL.Query().Get("msg")
		if msg == "" {
			http.Error(w, "missing ?msg= parameter", http.StatusBadRequest)
			return
		}

		askCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		reply, err := node.Ask(askCtx, echoPath, []byte(msg))
		if err != nil {
			http.Error(w, fmt.Sprintf("Ask error: %v", err), http.StatusGatewayTimeout)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "%s\n", reply.Payload)
		log.Printf("[http] Ask(%q) → %q", msg, reply.Payload)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		log.Printf("[http] listening on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[http] server error: %v", err)
		}
	}()

	// ── Step 4: Graceful shutdown ────────────────────────────────────────────
	<-ctx.Done()
	log.Println("[gekka] shutting down …")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}
