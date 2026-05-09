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
	"time"
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

func TestIsAssemblyFresh_MissingJarReturnsFalse(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	jar := filepath.Join(tmp, "missing.jar")

	fresh, err := isAssemblyFresh(jar, []string{src})
	if err != nil {
		t.Fatalf("isAssemblyFresh: %v", err)
	}
	if fresh {
		t.Error("expected fresh=false when JAR is missing")
	}
}

func TestIsAssemblyFresh_FreshJar(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	srcFile := filepath.Join(src, "Main.scala")
	if err := os.WriteFile(srcFile, []byte("// stub\n"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	// Source mtime in the past, jar mtime now.
	past := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(srcFile, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	jar := filepath.Join(tmp, "out.jar")
	if err := os.WriteFile(jar, []byte("PK\x03\x04"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}

	fresh, err := isAssemblyFresh(jar, []string{src})
	if err != nil {
		t.Fatalf("isAssemblyFresh: %v", err)
	}
	if !fresh {
		t.Error("expected fresh=true when jar mtime > src mtime")
	}
}

func TestIsAssemblyFresh_StaleJar(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	jar := filepath.Join(tmp, "out.jar")
	if err := os.WriteFile(jar, []byte("PK\x03\x04"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}
	// Make the jar 1 hour old so a freshly-written src file is newer.
	past := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(jar, past, past); err != nil {
		t.Fatalf("chtimes jar: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "Main.scala"), []byte("// new\n"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	fresh, err := isAssemblyFresh(jar, []string{src})
	if err != nil {
		t.Fatalf("isAssemblyFresh: %v", err)
	}
	if fresh {
		t.Error("expected fresh=false when src mtime > jar mtime")
	}
}

func TestIsAssemblyFresh_MissingInputRootIgnored(t *testing.T) {
	tmp := t.TempDir()
	jar := filepath.Join(tmp, "out.jar")
	if err := os.WriteFile(jar, []byte("PK\x03\x04"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}
	// Pass a non-existent input root.
	fresh, err := isAssemblyFresh(jar, []string{filepath.Join(tmp, "nope")})
	if err != nil {
		t.Fatalf("isAssemblyFresh: %v", err)
	}
	if !fresh {
		t.Error("expected fresh=true when no inputs exist")
	}
}
