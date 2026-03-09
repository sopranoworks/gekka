package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gekka/gekka"
	"gekka/gekka/actor"
)

// EchoActor simply logs received messages and replies to the sender.
type EchoActor struct {
	actor.BaseActor
}

func (a *EchoActor) Receive(msg any) {
	switch m := msg.(type) {
	case *gekka.IncomingMessage:
		text := string(m.Payload)
		a.Log().Info("Node A received remote message", "payload", text, "sender", m.Sender.Path())

		if s := a.Sender(); s != nil {
			reply := []byte("Ack: " + text)
			a.Log().Info("Node A replying to", "target", s.Path())
			s.Tell(reply, a.Self())
		}
	case string:
		a.Log().Info("Node A received local message", "msg", m)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [a|b]")
		return
	}

	mode := os.Args[1]
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if mode == "a" {
		runNodeA(ctx)
	} else {
		runNodeB(ctx)
	}
}

func runNodeA(ctx context.Context) {
	// Node A: Starts on port 2552, no seed-nodes.
	cfg := gekka.NodeConfig{
		SystemName: "RemoteSys",
		Host:       "127.0.0.1",
		Port:       2552,
	}

	node, err := gekka.Spawn(cfg)
	if err != nil {
		log.Fatalf("Node A Spawn: %v", err)
	}
	defer node.Shutdown()

	log.Printf("[Node A] started at %s", node.SelfAddress())

	// Register EchoActor at /user/echo
	node.System.ActorOf(actor.Props{
		New: func() actor.Actor {
			return &EchoActor{BaseActor: actor.NewBaseActor()}
		},
	}, "echo")

	<-ctx.Done()
	log.Println("[Node A] shutting down")
}

func runNodeB(ctx context.Context) {
	// Node B: Starts on a random port.
	cfg := gekka.NodeConfig{
		SystemName: "SenderSys",
		Host:       "127.0.0.1",
		Port:       0,
	}

	node, err := gekka.Spawn(cfg)
	if err != nil {
		log.Fatalf("Node B Spawn: %v", err)
	}
	defer node.Shutdown()

	log.Printf("[Node B] started at %s", node.SelfAddress())

	// Node B sends a message to Node A using a direct URI.
	// No JoinSeeds or Cluster membership required.
	nodeARef, err := node.ActorSelection("pekko://RemoteSys@127.0.0.1:2552/user/echo").Resolve(ctx)
	if err != nil {
		log.Fatalf("Node B Resolve Node A: %v", err)
	}

	log.Printf("[Node B] resolved Node A: %s", nodeARef.Path())

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			payload := []byte(fmt.Sprintf("Hello from Node B at %s", time.Now().Format(time.TimeOnly)))
			log.Printf("[Node B] -> Tell Node A")
			nodeARef.Tell(payload)
		case <-ctx.Done():
			log.Println("[Node B] shutting down")
			return
		}
	}
}
