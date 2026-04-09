/*
 * health.go
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
	"sync"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka/cmd/internal/cli"
	"github.com/sopranoworks/gekka/management/client"
	"github.com/spf13/cobra"
)

type probeResult struct {
	ok      bool
	message string
	err     error
}

func newHealthCmd(root *rootState) *cobra.Command {
	var (
		flagURL string
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Check /health/alive and /health/ready",
		RunE: func(cmd *cobra.Command, _ []string) error {
			baseURL := root.resolveURL(flagURL)
			exit := runHealth(baseURL, timeout, root.jsonOutput, root.quiet, os.Stdout)
			if exit != 0 {
				os.Exit(exit)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&flagURL, "url", "", "Base URL of the management API")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "Per-probe timeout")
	return cmd
}

// runHealth executes both probes in parallel and writes output to out.
// Returns the desired process exit code (0=ok, 1=probe failed, 2=unreachable/misuse).
func runHealth(baseURL string, timeout time.Duration, jsonOut, quiet bool, out io.Writer) int {
	c := client.New(baseURL)
	c.SetTimeout(timeout)

	var (
		wg               sync.WaitGroup
		aliveRes, readyR probeResult
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		ok, msg, err := c.Alive()
		aliveRes = probeResult{ok: ok, message: msg, err: err}
	}()
	go func() {
		defer wg.Done()
		ok, msg, err := c.Ready()
		readyR = probeResult{ok: ok, message: msg, err: err}
	}()
	wg.Wait()

	if aliveRes.err != nil || readyR.err != nil {
		if !quiet {
			if aliveRes.err != nil {
				fmt.Fprintln(os.Stderr, "health: alive:", aliveRes.err)
			}
			if readyR.err != nil {
				fmt.Fprintln(os.Stderr, "health: ready:", readyR.err)
			}
		}
		return 2
	}

	if quiet {
		if aliveRes.ok && readyR.ok {
			return 0
		}
		return 1
	}

	if jsonOut {
		payload := map[string]map[string]any{
			"alive": {"ok": aliveRes.ok, "message": aliveRes.message},
			"ready": {"ok": readyR.ok, "message": readyR.message},
		}
		b, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Fprintln(out, string(b))
	} else {
		w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			cli.HeaderStyle.Render("PROBE"),
			cli.HeaderStyle.Render("STATUS"),
			cli.HeaderStyle.Render("MESSAGE"))
		fmt.Fprintf(w, "alive\t%s\t%s\n", renderStatus(aliveRes.ok), aliveRes.message)
		fmt.Fprintf(w, "ready\t%s\t%s\n", renderStatus(readyR.ok), readyR.message)
		w.Flush()
	}

	if aliveRes.ok && readyR.ok {
		return 0
	}
	return 1
}

func renderStatus(ok bool) string {
	if ok {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Render("OK")
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render("FAIL")
}
