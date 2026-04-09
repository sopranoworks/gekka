/*
 * durable_state.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/management/client"
	"github.com/spf13/cobra"
)

func newDurableStateCmd(root *rootState) *cobra.Command {
	var flagURL string
	cmd := &cobra.Command{
		Use:   "durable-state <persistenceID>",
		Short: "Fetch and display a durable-state entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := root.resolveURL(flagURL)
			return runDurableState(baseURL, args[0], root.jsonOutput, root.quiet, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	return cmd
}

func runDurableState(baseURL, persistenceID string, jsonOut, quiet bool, out io.Writer) error {
	c := client.New(baseURL)
	resp, err := c.DurableState(persistenceID)
	if err != nil {
		return fmt.Errorf("durable-state: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("durable-state: %s not found", persistenceID)
	}

	bodyBytes, _ := json.Marshal(resp.State)

	if jsonOut {
		b, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Fprintln(out, string(b))
		return nil
	}
	if quiet {
		out.Write(bodyBytes)
		fmt.Fprintln(out)
		return nil
	}

	bold := lipgloss.NewStyle().Bold(true).Render
	fmt.Fprintf(out, "%s %s\n", bold("Persistence ID:"), resp.PersistenceID)
	fmt.Fprintf(out, "%s %d\n", bold("Revision:       "), resp.Revision)
	fmt.Fprintf(out, "%s %d bytes\n", bold("Size:           "), len(bodyBytes))
	fmt.Fprintln(out)

	if isLikelyJSON(bodyBytes) {
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, bodyBytes, "", "  "); err == nil {
			out.Write(pretty.Bytes())
			fmt.Fprintln(out)
			return nil
		}
	}
	writeHexDump(out, bodyBytes)
	return nil
}

// isLikelyJSON returns true if b (after trimming leading whitespace) starts
// with '{', '[', or '"' AND passes json.Valid.
func isLikelyJSON(b []byte) bool {
	trimmed := bytes.TrimLeft(b, " \t\r\n")
	if len(trimmed) == 0 {
		return false
	}
	first := trimmed[0]
	if first != '{' && first != '[' && first != '"' {
		return false
	}
	return json.Valid(b)
}

// writeHexDump writes `hexdump -C`-style output (16 bytes per row).
func writeHexDump(out io.Writer, data []byte) {
	const cols = 16
	for i := 0; i < len(data); i += cols {
		end := i + cols
		if end > len(data) {
			end = len(data)
		}
		row := data[i:end]
		hx := hex.EncodeToString(row)
		var spaced bytes.Buffer
		for j := 0; j < len(hx); j += 2 {
			if j > 0 {
				spaced.WriteByte(' ')
			}
			spaced.WriteString(hx[j : j+2])
		}
		ascii := make([]byte, len(row))
		for j, b := range row {
			if b >= 32 && b < 127 {
				ascii[j] = b
			} else {
				ascii[j] = '.'
			}
		}
		fmt.Fprintf(out, "%08x  %-47s  |%s|\n", i, spaced.String(), ascii)
	}
}
