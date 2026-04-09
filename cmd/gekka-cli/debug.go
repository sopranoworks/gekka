/*
 * debug.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"github.com/spf13/cobra"
)

func newDebugCmd(root *rootState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Introspection endpoints (requires gekka.management.debug.enabled=true on the server)",
	}
	cmd.AddCommand(newDebugCRDTCmd(root))
	cmd.AddCommand(newDebugActorsCmd(root))
	return cmd
}

