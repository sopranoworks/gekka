/*
 * confirm.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// PromptYesNo writes a "prompt [y/N]" line to out and reads a single line from in.
// If autoYes is true, it returns (true, nil) without touching in or out.
// Accepts "y" or "yes" (case-insensitive) as yes; everything else (including
// empty input) is treated as no.
func PromptYesNo(in io.Reader, out io.Writer, prompt string, autoYes bool) (bool, error) {
	if autoYes {
		return true, nil
	}
	if _, err := fmt.Fprintf(out, "%s [y/N] ", prompt); err != nil {
		return false, err
	}
	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}
