/*
 * style.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package cli

import "github.com/charmbracelet/lipgloss"

var (
	// ForestTeal is the primary accent color (#00897B).
	ForestTeal = lipgloss.Color("#00897B")

	// ForestGreen is the success/active accent color (#43A047).
	ForestGreen = lipgloss.Color("#43A047")

	// Styles
	HeaderStyle = lipgloss.NewStyle().Foreground(ForestTeal).Bold(true)
	SuccessStyle = lipgloss.NewStyle().Foreground(ForestGreen)
	BorderStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#00695C")) // Muted teal for borders
)
