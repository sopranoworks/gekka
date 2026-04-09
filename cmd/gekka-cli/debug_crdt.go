/*
 * debug_crdt.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/cmd/internal/cli"
	"github.com/sopranoworks/gekka/management/client"
	"github.com/spf13/cobra"
)

func newDebugCRDTCmd(root *rootState) *cobra.Command {
	var flagURL string
	cmd := &cobra.Command{
		Use:   "crdt [key]",
		Short: "List CRDTs or show one CRDT's current value",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if root.quiet {
				return fmt.Errorf("--quiet is not supported by 'debug crdt'")
			}
			baseURL := root.resolveURL(flagURL)
			if len(args) == 0 {
				return runDebugCRDTList(baseURL, root.jsonOutput, os.Stdout)
			}
			return runDebugCRDT(baseURL, args[0], root.jsonOutput, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	return cmd
}

func runDebugCRDTList(baseURL string, jsonOut bool, out io.Writer) error {
	c := client.New(baseURL)
	entries, err := c.DebugCRDTList()
	if err != nil {
		return fmt.Errorf("debug crdt: %w", err)
	}

	if jsonOut {
		b, _ := json.MarshalIndent(entries, "", "  ")
		fmt.Fprintln(out, string(b))
		return nil
	}

	if len(entries) == 0 {
		fmt.Fprintln(out, "(no CRDTs registered)")
		return nil
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s\t%s\n",
		cli.HeaderStyle.Render("KEY"),
		cli.HeaderStyle.Render("TYPE"))
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\n", e.Key, e.Type)
	}
	return w.Flush()
}

func runDebugCRDT(baseURL, key string, jsonOut bool, out io.Writer) error {
	c := client.New(baseURL)
	val, err := c.DebugCRDT(key)
	if err != nil {
		return fmt.Errorf("debug crdt: %w", err)
	}
	if val == nil {
		return fmt.Errorf("debug crdt: key %q not found", key)
	}

	if jsonOut {
		b, _ := json.MarshalIndent(val, "", "  ")
		fmt.Fprintln(out, string(b))
		return nil
	}

	bold := lipgloss.NewStyle().Bold(true).Render
	fmt.Fprintf(out, "%s %s\n", bold("Key: "), val.Key)
	fmt.Fprintf(out, "%s %s\n", bold("Type:"), val.Type)
	fmt.Fprintln(out)

	valueBytes, _ := json.MarshalIndent(val.Value, "", "  ")
	fmt.Fprintln(out, string(valueBytes))
	return nil
}
