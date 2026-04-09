/*
 * main.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"fmt"
	"os"

	"github.com/charmbracelet/lipgloss"
	"github.com/sopranoworks/gekka"
	"github.com/sopranoworks/gekka/cmd/internal/cli"
	"github.com/spf13/cobra"
)

type rootState struct {
	configPath string
	profile    string
	jsonOutput bool
	quiet      bool
	cfg        cli.Config
}

func (r *rootState) resolveURL(flagURL string) string {
	return cli.ResolveManagementURL(r.cfg, flagURL, r.profile)
}

func main() {
	root := &rootState{}

	// Nebula Parallel-Slash Icon Colors
	c1 := lipgloss.Color("#6A4CFF")
	c2 := lipgloss.Color("#8265FF")
	c3 := lipgloss.Color("#9B7FFF")
	c4 := lipgloss.Color("#B399FF")
	c5 := lipgloss.Color("#C678FF")
	c6 := lipgloss.Color("#DD94FF")
	c7 := lipgloss.Color("#F2AEFF")
	c8 := lipgloss.Color("#FFC9FF")

	// Icon Segments
	iconTop := "  " + lipgloss.NewStyle().Foreground(c3).Render("▄") + lipgloss.NewStyle().Foreground(c4).Render("▀") + "  " + lipgloss.NewStyle().Foreground(c7).Render("▄") + lipgloss.NewStyle().Foreground(c8).Render("▀")
	iconBottom := lipgloss.NewStyle().Foreground(c1).Render("▄") + lipgloss.NewStyle().Foreground(c2).Render("▀") + "  " + lipgloss.NewStyle().Foreground(c5).Render("▄") + lipgloss.NewStyle().Foreground(c6).Render("▀")

	// Text Components
	title := lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Bold(true).Render("gekka-cli")
	version := lipgloss.NewStyle().Foreground(lipgloss.Color("#808080")).Render("v" + gekka.Version)

	// Line Assembly
	topLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconTop, "  ", title)
	bottomLine := lipgloss.JoinHorizontal(lipgloss.Bottom, iconBottom, "      ", version)

	logo := lipgloss.JoinVertical(lipgloss.Left, topLine, bottomLine)

	rootCmd := &cobra.Command{
		Use:   "gekka-cli",
		Short: "CLI tool for managing a Gekka / Pekko cluster",
		Long: logo + "\n\n" + `gekka-cli is a command-line interface for inspecting and managing
a Gekka (or Apache Pekko) cluster via its HTTP Management API.`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := cli.Load(root.configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			root.cfg = cfg
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&root.configPath, "config", cli.DefaultConfigPath(),
		"Path to the gekka-cli config file")
	rootCmd.PersistentFlags().StringVar(&root.profile, "profile", "",
		"Named profile to use from the config file")
	rootCmd.PersistentFlags().BoolVar(&root.jsonOutput, "json", false,
		"Output raw JSON instead of a formatted table")
	rootCmd.PersistentFlags().BoolVarP(&root.quiet, "quiet", "q", false,
		"Suppress non-essential output (errors still go to stderr)")

	membersCmd := newMembersCmd(root)
	membersCmd.AddCommand(newMemberDownCmd(root))
	membersCmd.AddCommand(newMemberLeaveCmd(root))
	rootCmd.AddCommand(membersCmd)
	rootCmd.AddCommand(newDiscoveryCheckCmd(root))
	rootCmd.AddCommand(newDashboardCmd(root))
	rootCmd.AddCommand(newShardsCmd(root))

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
