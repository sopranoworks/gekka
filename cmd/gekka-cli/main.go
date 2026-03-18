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
	"github.com/sopranoworks/gekka/internal/cli"
	"github.com/spf13/cobra"
)

type rootState struct {
	configPath string
	profile    string
	jsonOutput bool
	cfg        cli.Config
}

func (r *rootState) resolveURL(flagURL string) string {
	return cli.ResolveManagementURL(r.cfg, flagURL, r.profile)
}

func main() {
	root := &rootState{}

	// High-fidelity Logo: >_❀ gekka-cli v0.9.0
	logo := lipgloss.JoinHorizontal(lipgloss.Center,
		lipgloss.NewStyle().Foreground(lipgloss.Color("14")).Render(">"),
		lipgloss.NewStyle().Foreground(lipgloss.Color("13")).Render("_"),
		lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render("❀"),
		lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true).Render(" gekka-cli"),
		lipgloss.NewStyle().Foreground(lipgloss.Color("242")).Render(" v0.9.0"),
	)

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

	rootCmd.AddCommand(newMembersCmd(root))
	rootCmd.AddCommand(newDiscoveryCheckCmd(root))
	rootCmd.AddCommand(newDashboardCmd(root))

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
