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

	rootCmd := &cobra.Command{
		Use:   "gekka-cli",
		Short: "CLI tool for managing a Gekka / Pekko cluster",
		Long: `gekka-cli is a command-line interface for inspecting and managing
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

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
