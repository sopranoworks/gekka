/*
 * discovery.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/discovery"
	_ "github.com/sopranoworks/gekka/discovery/kubernetes"
	"github.com/spf13/cobra"
)

func newDiscoveryCheckCmd(root *rootState) *cobra.Command {
	var hoconPath string

	cmd := &cobra.Command{
		Use:   "discovery-check",
		Short: "Test cluster discovery using local HOCON configuration",
		Long: `Initializes the discovery provider specified in the HOCON file and
executes a fetch operation to verify that seed nodes can be discovered.
This is a diagnostic tool for validating Kubernetes API/DNS settings.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDiscoveryCheck(hoconPath)
		},
	}

	cmd.Flags().StringVar(&hoconPath, "hocon", "application.conf",
		"Path to the HOCON configuration file to test")

	return cmd
}

func runDiscoveryCheck(path string) error {
	cfg, err := gekka.LoadConfig(path)
	if err != nil {
		return fmt.Errorf("load config %q: %w", path, err)
	}

	if !cfg.Discovery.Enabled {
		return fmt.Errorf("discovery is not enabled in %q (gekka.cluster.discovery.enabled = off)", path)
	}

	fmt.Printf("Configured Provider: %s\n", cfg.Discovery.Type)
	fmt.Printf("Configuration:       %+v\n", cfg.Discovery.Config.Config)
	fmt.Println("------------------------------------------------------------")

	provider, err := discovery.Get(cfg.Discovery.Type, cfg.Discovery.Config)
	if err != nil {
		return fmt.Errorf("initialize provider: %w", err)
	}

	fmt.Println("[WAIT] Fetching seed nodes...")
	seeds, err := provider.FetchSeedNodes()
	if err != nil {
		return fmt.Errorf("fetch failed: %w", err)
	}

	if len(seeds) == 0 {
		fmt.Println("No seed nodes discovered.")
		return nil
	}

	fmt.Printf("SUCCESS: Discovered %d seed node(s):\n\n", len(seeds))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "INDEX\tADDRESS")
	fmt.Fprintln(w, "-----\t-------")
	for i, s := range seeds {
		fmt.Fprintf(w, "%d\t%s\n", i+1, s)
	}
	return w.Flush()
}
