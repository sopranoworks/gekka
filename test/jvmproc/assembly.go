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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
