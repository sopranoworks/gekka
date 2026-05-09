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
