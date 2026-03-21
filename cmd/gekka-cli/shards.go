/*
 * shards.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/sopranoworks/gekka/internal/cli"
	"github.com/sopranoworks/gekka/internal/management/client"
	"github.com/spf13/cobra"
)

func newShardsCmd(root *rootState) *cobra.Command {
	var flagURL string

	cmd := &cobra.Command{
		Use:   "shards",
		Short: "Inspect and manage cluster shard distribution",
		Long: `Commands for inspecting and managing Cluster Sharding state.

The management URL is resolved in this order:
  1. --url flag
  2. --profile flag → profile entry in the config file
  3. default_profile in the config file
  4. management_url in the config file
  5. built-in default (http://127.0.0.1:8558)`,
	}

	cmd.PersistentFlags().StringVar(&flagURL, "url", "",
		"Base URL of the management API (overrides config file)")

	cmd.AddCommand(newShardsListCmd(root, &flagURL))
	cmd.AddCommand(newShardsRebalanceCmd(root, &flagURL))

	return cmd
}

// ── shards list ───────────────────────────────────────────────────────────────

func newShardsListCmd(root *rootState, parentURL *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list <typeName>",
		Short: "Show current shard-to-region mapping for an entity type",
		Long: `Calls GET /cluster/sharding/{typeName} and prints the current
shard allocation map: which shard ID lives on which ShardRegion actor path.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := root.resolveURL(*parentURL)
			return runShardsList(baseURL, args[0], root.jsonOutput)
		},
	}
}

func runShardsList(baseURL, typeName string, jsonOut bool) error {
	c := client.New(baseURL)
	dist, err := c.ShardDistribution(typeName)
	if err != nil {
		return fmt.Errorf("shards list: %w", err)
	}

	if jsonOut {
		b, _ := json.Marshal(dist)
		fmt.Fprintln(os.Stdout, string(b))
		return nil
	}

	if len(dist) == 0 {
		fmt.Printf("No shards allocated for entity type %q.\n", typeName)
		return nil
	}

	// Sort shard IDs for deterministic output.
	ids := make([]string, 0, len(dist))
	for id := range dist {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	header := cli.HeaderStyle.Render
	border := cli.BorderStyle.Render

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s\t%s\n", header("SHARD ID"), header("REGION PATH"))
	fmt.Fprintf(w, "%s\t%s\n", border("--------"), border("-----------"))
	for _, id := range ids {
		fmt.Fprintf(w, "%s\t%s\n", id, dist[id])
	}
	return w.Flush()
}

// ── shards rebalance ──────────────────────────────────────────────────────────

func newShardsRebalanceCmd(root *rootState, parentURL *string) *cobra.Command {
	return &cobra.Command{
		Use:   "rebalance <typeName> <shardID> <targetRegion>",
		Short: "Move a specific shard to a target region",
		Long: `Calls POST /cluster/sharding/{typeName}/rebalance to initiate a
manual handoff of <shardID> to <targetRegion>.

<targetRegion> must be the full actor path of a registered ShardRegion
(e.g. /user/Cart-region).  The coordinator will acknowledge the request
and begin the BeginHandOff → HandOff → ShardStopped sequence.`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := root.resolveURL(*parentURL)
			typeName, shardID, targetRegion := args[0], args[1], args[2]
			return runShardsRebalance(baseURL, typeName, shardID, targetRegion)
		},
	}
}

func runShardsRebalance(baseURL, typeName, shardID, targetRegion string) error {
	c := client.New(baseURL)
	if err := c.RebalanceShard(typeName, shardID, targetRegion); err != nil {
		return fmt.Errorf("shards rebalance: %w", err)
	}
	fmt.Printf("Rebalance initiated: shard %q → %q (type: %s)\n",
		shardID, targetRegion, typeName)
	return nil
}
