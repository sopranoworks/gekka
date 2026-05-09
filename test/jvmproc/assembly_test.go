/*
 * assembly_test.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

package jvmproc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubAssemblyProject scaffolds a temp-dir project that mimics the
// AssemblyProject layout: <tmp>/go.work + <tmp>/proj/build.sbt +
// <tmp>/proj/project + <tmp>/proj/src. The test's cwd is changed to
// <tmp> so findRepoRoot finds the stub go.work.
func stubAssemblyProject(t *testing.T) (repo string, p AssemblyProject, jarPath string) {
	t.Helper()
	repo = t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "go.work"), []byte("go 1.26\n"), 0o644); err != nil {
		t.Fatalf("write go.work: %v", err)
	}
	for _, sub := range []string{"proj", "proj/project", "proj/src", "proj/target/scala-2.13"} {
		if err := os.MkdirAll(filepath.Join(repo, sub), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", sub, err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "proj", "build.sbt"), []byte("// stub\n"), 0o644); err != nil {
		t.Fatalf("write build.sbt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "proj", "src", "Main.scala"), []byte("// stub\n"), 0o644); err != nil {
		t.Fatalf("write Main.scala: %v", err)
	}
	jarPath = filepath.Join(repo, "proj", "target", "scala-2.13", "out.jar")
	p = AssemblyProject{
		Dir:        "proj",
		SbtProject: "",
		ProjectSrc: "proj",
		JarPath:    "proj/target/scala-2.13/out.jar",
	}
	t.Chdir(repo)
	resetAssemblyCacheForTest(t)
	return repo, p, jarPath
}

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

func TestTryEnsureAssembly_FreshSkipsBuilder(t *testing.T) {
	_, p, jarPath := stubAssemblyProject(t)
	// Pre-create JAR with future mtime so it's fresh.
	if err := os.WriteFile(jarPath, []byte("PK"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}
	future := time.Now().Add(1 * time.Hour)
	if err := os.Chtimes(jarPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	var calls int32
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	got, err := tryEnsureAssembly(t, p)
	if err != nil {
		t.Fatalf("tryEnsureAssembly: %v", err)
	}
	if got != jarPath {
		t.Errorf("path: got %q, want %q", got, jarPath)
	}
	if calls != 0 {
		t.Errorf("builder called %d times, want 0 (fresh JAR should skip)", calls)
	}
}

func TestTryEnsureAssembly_StaleRebuilds(t *testing.T) {
	_, p, jarPath := stubAssemblyProject(t)
	// JAR exists but is older than src.
	if err := os.WriteFile(jarPath, []byte("PK"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}
	past := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(jarPath, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	var calls int32
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		atomic.AddInt32(&calls, 1)
		// Simulate sbt by touching the JAR after the build.
		now := time.Now()
		return os.Chtimes(jarPath, now, now)
	})

	if _, err := tryEnsureAssembly(t, p); err != nil {
		t.Fatalf("tryEnsureAssembly: %v", err)
	}
	if calls != 1 {
		t.Errorf("builder calls = %d, want 1", calls)
	}
}

func TestTryEnsureAssembly_MissingBuilds(t *testing.T) {
	_, p, jarPath := stubAssemblyProject(t)
	// No JAR pre-created.

	var calls int32
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		atomic.AddInt32(&calls, 1)
		return os.WriteFile(jarPath, []byte("PK"), 0o644)
	})

	if _, err := tryEnsureAssembly(t, p); err != nil {
		t.Fatalf("tryEnsureAssembly: %v", err)
	}
	if calls != 1 {
		t.Errorf("builder calls = %d, want 1", calls)
	}
}

func TestTryEnsureAssembly_MemoizesAcrossCalls(t *testing.T) {
	_, p, jarPath := stubAssemblyProject(t)
	var calls int32
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		atomic.AddInt32(&calls, 1)
		return os.WriteFile(jarPath, []byte("PK"), 0o644)
	})

	for i := 0; i < 5; i++ {
		if _, err := tryEnsureAssembly(t, p); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if calls != 1 {
		t.Errorf("builder calls = %d across 5 invocations, want 1", calls)
	}
}

func TestTryEnsureAssembly_ForceRebuildEnv(t *testing.T) {
	_, p, jarPath := stubAssemblyProject(t)
	if err := os.WriteFile(jarPath, []byte("PK"), 0o644); err != nil {
		t.Fatalf("write jar: %v", err)
	}
	future := time.Now().Add(1 * time.Hour)
	if err := os.Chtimes(jarPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	t.Setenv("GEKKA_FORCE_ASSEMBLY", "1")

	var calls int32
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	if _, err := tryEnsureAssembly(t, p); err != nil {
		t.Fatalf("tryEnsureAssembly: %v", err)
	}
	if calls != 1 {
		t.Errorf("builder calls = %d under GEKKA_FORCE_ASSEMBLY=1, want 1", calls)
	}
}

func TestTryEnsureAssembly_BuildFailureSurfacesError(t *testing.T) {
	_, p, _ := stubAssemblyProject(t)
	setAssemblyBuilderForTest(t, func(testing.TB, context.Context, AssemblyProject) error {
		return fmt.Errorf("synthetic build failure")
	})

	_, err := tryEnsureAssembly(t, p)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "synthetic build failure") {
		t.Errorf("error = %v, want it to wrap 'synthetic build failure'", err)
	}
}
