/*
 * assembly.go
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

// File provides EnsureAssembly + supporting helpers so integration tests can
// run pre-built fat JARs (java -cp <jar> <Main>) instead of `sbt runMain`.
// The motivation is to eliminate sbt's bg-jobs/sbt_<hash> leak that
// accumulates when jvmproc SIGKILLs the sbt process group before sbt's
// shutdown hook runs.

package jvmproc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// findRepoRoot walks up from the current working directory looking for
// go.work, the gekka workspace marker. Returns the absolute path of the
// directory that contains it. Errors out after 10 ancestors so a stray
// invocation outside the repo fails loudly instead of silently walking
// up past `/`.
func findRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			abs, err := filepath.Abs(dir)
			if err != nil {
				return "", err
			}
			return abs, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("repo root not found (no go.work) starting from %s", cwd)
}

// isAssemblyFresh reports whether jarPath exists and its mtime is at
// least as recent as the newest file under any of inputRoots. A missing
// jar is never fresh. A missing input root is treated as having no
// inputs (the project simply contributes nothing). Errors other than
// fs.ErrNotExist are surfaced.
func isAssemblyFresh(jarPath string, inputRoots []string) (bool, error) {
	jarStat, err := os.Stat(jarPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	var newest time.Time
	for _, root := range inputRoots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if info.ModTime().After(newest) {
				newest = info.ModTime()
			}
			return nil
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
	}
	return !newest.After(jarStat.ModTime()), nil
}

// AssemblyProject identifies a fat JAR target produced by sbt-assembly.
// All paths are relative to the repository root (the directory that
// contains go.work).
type AssemblyProject struct {
	// Dir is the sbt project root that contains build.sbt.
	Dir string

	// SbtProject is the sbt sub-project name. Empty string for the root
	// project. The build command is `sbt assembly` for root or
	// `sbt <SbtProject>/assembly` for a sub-project.
	SbtProject string

	// ProjectSrc is the directory containing this sub-project's src/
	// tree. For root projects it equals Dir; for sub-projects it is the
	// sub-project's directory (e.g. "scala-server/akka-server").
	ProjectSrc string

	// JarPath is where sbt-assembly emits the fat JAR, relative to the
	// repository root.
	JarPath string
}

var (
	// PekkoAssembly produces the fat JAR for every Pekko-side main
	// (PekkoServer, PekkoIntegrationNode, SBRTestNode, ClusterSeedNode,
	// ClusterSingletonServer, ScalaClusterNode, …).
	PekkoAssembly = AssemblyProject{
		Dir:        "scala-server",
		SbtProject: "",
		ProjectSrc: "scala-server",
		JarPath:    "scala-server/target/scala-2.13/pekko-mains-assembly.jar",
	}

	// AkkaAssembly produces the fat JAR for every Akka 2.6.x main
	// living under the akkaServer sub-project.
	AkkaAssembly = AssemblyProject{
		Dir:        "scala-server",
		SbtProject: "akkaServer",
		ProjectSrc: "scala-server/akka-server",
		JarPath:    "scala-server/akka-server/target/scala-2.13/akka-mains-assembly.jar",
	}
)

// assemblyState carries the result of a single (Dir, SbtProject) build.
type assemblyState struct {
	once sync.Once
	path string
	err  error
}

// assemblyCache memoizes EnsureAssembly results within one test binary.
// Keyed by "<Dir>::<SbtProject>". Cleared between tests via
// resetAssemblyCacheForTest.
var assemblyCache sync.Map

// assemblyBuilder is the function that actually invokes sbt to produce
// the JAR. Tests swap this via setAssemblyBuilderForTest.
var assemblyBuilder = realBuildAssembly

// EnsureAssembly returns the absolute path to a fresh fat JAR for p,
// rebuilding via `sbt <SbtProject>/assembly` if any input file is newer
// than the JAR. Memoized per process. Calls t.Fatalf on build failure.
func EnsureAssembly(t testing.TB, p AssemblyProject) string {
	t.Helper()
	path, err := tryEnsureAssembly(t, p)
	if err != nil {
		t.Fatalf("EnsureAssembly(%s::%s): %v", p.Dir, p.SbtProject, err)
	}
	return path
}

// tryEnsureAssembly is the error-returning core of EnsureAssembly so
// tests can assert on failure modes without their TB instance Fatalf-ing
// out. Production code should use the public EnsureAssembly wrapper.
func tryEnsureAssembly(t testing.TB, p AssemblyProject) (string, error) {
	t.Helper()
	repo, err := findRepoRoot()
	if err != nil {
		return "", fmt.Errorf("repo root: %w", err)
	}
	absJar := filepath.Join(repo, p.JarPath)

	key := p.Dir + "::" + p.SbtProject
	v, _ := assemblyCache.LoadOrStore(key, &assemblyState{})
	s := v.(*assemblyState)
	s.once.Do(func() {
		force := os.Getenv("GEKKA_FORCE_ASSEMBLY") == "1"
		if !force {
			fresh, ferr := isAssemblyFresh(absJar, assemblyInputRoots(repo, p))
			if ferr != nil {
				s.err = fmt.Errorf("freshness: %w", ferr)
				return
			}
			if fresh {
				s.path = absJar
				return
			}
		}
		if berr := assemblyBuilder(t, context.Background(), p); berr != nil {
			s.err = fmt.Errorf("build: %w", berr)
			return
		}
		if _, statErr := os.Stat(absJar); statErr != nil {
			s.err = fmt.Errorf("post-build: jar not at %s: %w", absJar, statErr)
			return
		}
		s.path = absJar
	})
	return s.path, s.err
}

// assemblyInputRoots returns the absolute paths whose mtimes invalidate
// the fat JAR for p.
func assemblyInputRoots(repo string, p AssemblyProject) []string {
	return []string{
		filepath.Join(repo, p.Dir, "build.sbt"),
		filepath.Join(repo, p.Dir, "project"),
		filepath.Join(repo, p.ProjectSrc, "src"),
	}
}

// realBuildAssembly invokes sbt via jvmproc.Spawn and waits for the
// build task to exit. The build runs as a Task inside sbt's own JVM
// (assembly does NOT bgRun-fork), so it does not create a bg-jobs
// entry of its own.
func realBuildAssembly(t testing.TB, ctx context.Context, p AssemblyProject) error {
	t.Helper()
	cmd := "assembly"
	if p.SbtProject != "" {
		cmd = p.SbtProject + "/assembly"
	}
	proc, err := Spawn(t, ctx, "sbt", []string{cmd}, Options{
		Dir:         p.Dir,
		KillTimeout: 30 * time.Second,
	})
	if err != nil {
		return err
	}
	code, werr := proc.WaitForExit(ctx)
	if werr != nil {
		return werr
	}
	if code != 0 {
		return fmt.Errorf("sbt %s exited with code %d", cmd, code)
	}
	return nil
}

// setAssemblyBuilderForTest swaps the package-level builder for the
// duration of a test. Restored via t.Cleanup.
func setAssemblyBuilderForTest(t testing.TB, fn func(testing.TB, context.Context, AssemblyProject) error) {
	t.Helper()
	prev := assemblyBuilder
	assemblyBuilder = fn
	t.Cleanup(func() { assemblyBuilder = prev })
}

// resetAssemblyCacheForTest clears the per-project sync.Once cache so
// each test starts with a cold cache.
func resetAssemblyCacheForTest(t testing.TB) {
	t.Helper()
	assemblyCache = sync.Map{}
}
