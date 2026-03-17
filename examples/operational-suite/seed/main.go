/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// seed is a minimal cluster seed node used by the operational-suite example.
// It loads a HOCON config file specified via --config and starts the cluster
// with the HTTP Management API enabled.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	gekka "github.com/sopranoworks/gekka"
)

func main() {
	flagConfig := flag.String("config", "", "Path to HOCON application.conf (required)")
	flag.Parse()

	if *flagConfig == "" {
		log.Fatal("--config is required")
	}

	cfg, err := gekka.LoadConfig(*flagConfig)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	node, err := gekka.NewCluster(cfg)
	if err != nil {
		log.Fatalf("new cluster: %v", err)
	}
	defer func() { _ = node.Shutdown() }()

	if err := node.JoinSeeds(); err != nil {
		log.Fatalf("join seeds: %v", err)
	}

	log.Printf("seed node up — management API on %s:%d", cfg.Management.Hostname, cfg.Management.Port)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("seed node shutting down")
}
