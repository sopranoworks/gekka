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
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/cmd/internal/cli"
	"github.com/sopranoworks/gekka/management/client"
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
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", header("ADDRESS"), header("STATUS"), header("DC"), header("ROLES"), header("REACHABLE"), header("RTT"))
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", border("-------"), border("------"), border("--"), border("-----"), border("---------"), border("---"))
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

		rttStr := fmt.Sprintf("%dms", m.LatencyMs)
		var rttRendered string
		if !m.Reachable {
			rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("timeout")
		} else {
			if m.LatencyMs >= 500 {
				rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render(rttStr)
			} else if m.LatencyMs >= 200 {
				rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("208")).Render(rttStr)
			} else if m.LatencyMs >= 50 {
				rttRendered = lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Render(rttStr)
			} else {
				rttRendered = cli.SuccessStyle.Render(rttStr)
			}
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			m.Address, status, m.DataCenter, roles, reachable, rttRendered)
	}
	return w.Flush()
}

type memberAction int

const (
	memberActionDown memberAction = iota
	memberActionLeave
)

func (a memberAction) verb() string {
	if a == memberActionDown {
		return "DOWN"
	}
	return "LEAVE"
}

func (a memberAction) do(c *client.Client, addr string) error {
	if a == memberActionDown {
		return c.DownMember(addr)
	}
	return c.LeaveMember(addr)
}

// runMemberAction is the testable entry point for `members down` and
// `members leave`.  It prompts, calls the client, and writes output.
func runMemberAction(action memberAction, baseURL, address string,
	autoYes, jsonOut, quiet bool,
	in io.Reader, out io.Writer) error {

	if !autoYes {
		ok, err := PromptYesNo(in, out,
			fmt.Sprintf("About to %s %s. Continue?", action.verb(), address),
			false)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	c := client.New(baseURL)
	if err := action.do(c, address); err != nil {
		return fmt.Errorf("%s: %w", strings.ToLower(action.verb()), err)
	}

	if quiet {
		return nil
	}
	if jsonOut {
		payload := map[string]string{
			"action":  strings.ToLower(action.verb()),
			"address": address,
			"status":  "ok",
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(b))
		return nil
	}
	fmt.Fprintf(out, "%s %s %s\n", cli.SuccessStyle.Render("✓"), action.verb(), address)
	return nil
}

func newMemberDownCmd(root *rootState) *cobra.Command {
	var (
		flagURL string
		autoYes bool
	)
	cmd := &cobra.Command{
		Use:   "down <address>",
		Short: "Mark a cluster member as Down (immediate removal)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := root.resolveURL(flagURL)
			if !autoYes && !isTerminal(os.Stdin) {
				return fmt.Errorf("refusing to prompt: stdin is not a tty (use --yes)")
			}
			return runMemberAction(memberActionDown, baseURL, args[0],
				autoYes, root.jsonOutput, root.quiet,
				os.Stdin, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	cmd.Flags().BoolVarP(&autoYes, "yes", "y", false, "Skip confirmation prompt")
	return cmd
}

// isTerminal reports whether f is connected to a terminal.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func newMemberLeaveCmd(root *rootState) *cobra.Command {
	var (
		flagURL string
		autoYes bool
	)
	cmd := &cobra.Command{
		Use:   "leave <address>",
		Short: "Initiate a graceful Leave for a cluster member",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			baseURL := root.resolveURL(flagURL)
			if !autoYes && !isTerminal(os.Stdin) {
				return fmt.Errorf("refusing to prompt: stdin is not a tty (use --yes)")
			}
			return runMemberAction(memberActionLeave, baseURL, args[0],
				autoYes, root.jsonOutput, root.quiet,
				os.Stdin, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	cmd.Flags().BoolVarP(&autoYes, "yes", "y", false, "Skip confirmation prompt")
	return cmd
}
