// cmd/showcase-gekka/main.go — gekka node binary for gekka_showcase_test.
// SPDX-License-Identifier: MIT
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gekka "github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/actor"
	"github.com/sopranoworks/gekka/internal/core"
)

func main() {
	systemName := flag.String("system", "ShowcaseCluster", "actor system name")
	nodeLabel := flag.String("node", "", "node label, e.g. g1")
	port := flag.Int("port", 0, "remoting port")
	mgmtPort := flag.Int("mgmt-port", 0, "management HTTP port")
	seeds := flag.String("seeds", "", "comma-separated seed list host:port,host:port")
	rolesCSV := flag.String("roles", "showcase-member", "comma-separated cluster roles")
	flag.Parse()

	if *nodeLabel == "" || *port == 0 || *mgmtPort == 0 || *seeds == "" {
		fmt.Fprintln(os.Stderr, "usage: showcase-gekka --node g1 --port 2541 --mgmt-port 9541 --seeds host:port,...")
		os.Exit(2)
	}

	roles := strings.Split(*rolesCSV, ",")

	cfg := gekka.ClusterConfig{
		SystemName: *systemName,
		Address: actor.Address{
			Protocol: "pekko",
			System:   *systemName,
			Host:     "127.0.0.1",
			Port:     *port,
		},
		Roles: roles,
		Management: core.ManagementConfig{
			Enabled:  true,
			Hostname: "127.0.0.1",
			Port:     *mgmtPort,
		},
	}

	cluster, err := gekka.NewCluster(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewCluster: %v\n", err)
		os.Exit(1)
	}

	// Join the first seed.
	first := strings.SplitN(strings.Split(*seeds, ",")[0], ":", 2)
	if len(first) != 2 {
		fmt.Fprintf(os.Stderr, "bad --seeds: %q\n", *seeds)
		os.Exit(2)
	}
	seedHost := first[0]
	var seedPort int
	if _, err := fmt.Sscanf(first[1], "%d", &seedPort); err != nil {
		fmt.Fprintf(os.Stderr, "bad seed port in %q: %v\n", *seeds, err)
		os.Exit(2)
	}
	if err := cluster.Join(seedHost, uint32(seedPort)); err != nil {
		fmt.Fprintf(os.Stderr, "Join: %v\n", err)
		os.Exit(1)
	}

	// Wait until the cluster reports this node Up.
	waitCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if cluster.IsLocalNodeUp() {
			break
		}
		select {
		case <-waitCtx.Done():
			fmt.Fprintln(os.Stderr, "timed out waiting for self-Up")
			os.Exit(1)
		case <-time.After(200 * time.Millisecond):
		}
	}
	fmt.Printf("--- SHOWCASE NODE READY: %s ---\n", *nodeLabel)
	os.Stdout.Sync()

	// Block on SIGTERM/SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	_ = cluster.Shutdown()
}
