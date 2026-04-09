/*
 * confirm_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestPromptYesNo_AutoYes(t *testing.T) {
	ok, err := PromptYesNo(&bytes.Buffer{}, &bytes.Buffer{}, "go?", true)
	if err != nil || !ok {
		t.Errorf("autoYes: want (true,nil), got (%v,%v)", ok, err)
	}
}

func TestPromptYesNo_Yes(t *testing.T) {
	in := strings.NewReader("y\n")
	var out bytes.Buffer
	ok, err := PromptYesNo(in, &out, "go?", false)
	if err != nil || !ok {
		t.Errorf("y: want (true,nil), got (%v,%v)", ok, err)
	}
	if !strings.Contains(out.String(), "go?") {
		t.Errorf("prompt text missing from output: %q", out.String())
	}
}

func TestPromptYesNo_No(t *testing.T) {
	in := strings.NewReader("n\n")
	ok, err := PromptYesNo(in, &bytes.Buffer{}, "go?", false)
	if err != nil || ok {
		t.Errorf("n: want (false,nil), got (%v,%v)", ok, err)
	}
}

func TestPromptYesNo_EmptyDefaultsNo(t *testing.T) {
	in := strings.NewReader("\n")
	ok, err := PromptYesNo(in, &bytes.Buffer{}, "go?", false)
	if err != nil || ok {
		t.Errorf("empty: want (false,nil), got (%v,%v)", ok, err)
	}
}
