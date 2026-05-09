/*
 * assembly_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindRepoRoot_FindsGoWorkInParent(t *testing.T) {
	// Build a tree:
	//   <tmp>/go.work
	//   <tmp>/a/b/c/  ← chdir here
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "go.work"), []byte("go 1.26\n"), 0o644); err != nil {
		t.Fatalf("write go.work: %v", err)
	}
	deep := filepath.Join(tmp, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Chdir(deep)

	got, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot: %v", err)
	}
	want, _ := filepath.EvalSymlinks(tmp)
	gotEval, _ := filepath.EvalSymlinks(got)
	if gotEval != want {
		t.Errorf("got %q, want %q", gotEval, want)
	}
}

func TestFindRepoRoot_NoGoWorkReturnsError(t *testing.T) {
	tmp := t.TempDir()
	t.Chdir(tmp)
	if _, err := findRepoRoot(); err == nil {
		t.Fatal("expected error when no go.work is present")
	}
}
