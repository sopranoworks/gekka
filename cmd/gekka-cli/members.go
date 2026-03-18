/*
 * members.go
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
	"text/tabwriter"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/internal/cli"
	"github.com/sopranoworks/gekka/internal/management/client"
	"github.com/spf13/cobra"
)

func newMembersCmd(root *rootState) *cobra.Command {
	var flagURL string

	cmd := &cobra.Command{
		Use:   "members",
		Short: "List all cluster members and their current status",
		Long: `Calls GET /cluster/members on the Cluster HTTP Management API and
prints a human-readable table of all known members, their status,
roles, data-center, and reachability.

The management URL is resolved in this order:
  1. --url flag
  2. --profile flag → profile entry in the config file
  3. default_profile in the config file
  4. management_url in the config file
  5. built-in default (http://127.0.0.1:8558)`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			url := root.resolveURL(flagURL)
			return runMembers(url, root.jsonOutput)
		},
	}

	cmd.Flags().StringVar(&flagURL, "url", "",
		"Base URL of the management API (overrides config file)")

	return cmd
}

func runMembers(baseURL string, jsonOut bool) error {
	c := client.New(baseURL)
	members, err := c.Members()
	if err != nil {
		return fmt.Errorf("members: %w", err)
	}

	if jsonOut {
		b, err := json.Marshal(members)
		if err != nil {
			return fmt.Errorf("members: marshal: %w", err)
		}
		fmt.Fprintln(os.Stdout, string(b))
		return nil
	}

	if len(members) == 0 {
		fmt.Println("No cluster members found.")
		return nil
	}

	header := cli.HeaderStyle.Render
	border := cli.BorderStyle.Render

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", header("ADDRESS"), header("STATUS"), header("DC"), header("ROLES"), header("REACHABLE"))
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", border("-------"), border("------"), border("--"), border("-----"), border("---------"))
	for _, m := range members {
		roles := "-"
		if len(m.Roles) > 0 {
			roles = ""
			for i, r := range m.Roles {
				if i > 0 {
					roles += ","
				}
				roles += r
			}
		}

		status := m.Status
		if status == "Up" || status == "Joining" {
			status = cli.SuccessStyle.Render(status)
		}

		var reachable string
		if !m.Reachable {
			reachable = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render("NO")
		} else {
			reachable = cli.SuccessStyle.Render("yes")
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			m.Address, status, m.DataCenter, roles, reachable)
	}
	return w.Flush()
}
