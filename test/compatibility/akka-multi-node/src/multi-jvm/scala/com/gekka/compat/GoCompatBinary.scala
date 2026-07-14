/*
 * GoCompatBinary.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.gekka.compat

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
 * Shared logic for locating — or, if entirely absent, building — the Go
 * compat-test binaries: `gekka-compat-test` (used by `FourNodeClusterSpec`,
 * `FiveNodeClusterSpec`, and `GekkaCompatSpec`) and
 * `gekka-aeron-compat-test` (used by `AeronClusterSpec`).
 *
 * Building on demand (rather than only ever checking for a pre-built
 * artifact) mirrors this project's own precedent for the scala-server jar:
 * `test/jvmproc/assembly.go`'s `EnsureAssembly` already builds missing/stale
 * JVM-side artifacts as part of Go integration test setup, so doing the
 * same for the Go side here keeps both halves of this cross-language suite
 * consistent, rather than requiring a human to pre-build one side by hand.
 *
 * Unlike `EnsureAssembly` — which discards the underlying `sbt` build
 * output entirely and surfaces only `"exit status 1"` on failure, making a
 * genuine compile error indistinguishable from any other build problem —
 * this helper captures the actual `go build` stdout/stderr and returns it
 * distinctly from the "binary genuinely absent, no source to build either"
 * case, so a compile error (like the `runAeronMode` undefined-reference
 * incident) is immediately visible as a compile error.
 */
object GoCompatBinary {

  sealed trait Result
  final case class Found(path: String) extends Result
  final case class BuildFailed(command: String, output: String) extends Result
  final case class NotFoundNoSource(searchedFrom: String) extends Result

  private def relativeCandidates(binaryName: String) = Seq(
    s"../../../bin/$binaryName",
    s"../../bin/$binaryName",
    binaryName,
  )

  private def isUsable(p: String): Boolean = {
    val f = new java.io.File(p)
    f.canExecute || (!f.isAbsolute && Try(s"which $p".!!).isSuccess)
  }

  /**
   * Walk upward from the current working directory looking for the repo
   * root, identified by the presence of `go.work` (present only at the
   * gekka repo root). Bounded to defend against an unexpected working
   * directory rather than looping forever.
   */
  private def findRepoRoot(): Option[java.io.File] = {
    var dir = new java.io.File(".").getCanonicalFile
    var depth = 0
    while (dir != null && depth < 10) {
      if (new java.io.File(dir, "go.work").isFile) return Some(dir)
      dir = dir.getParentFile
      depth += 1
    }
    None
  }

  /**
   * Locate a usable `gekka-compat-test` binary: an explicit env var, then
   * relative candidate paths, then PATH. If none are usable, attempt to
   * build it from source (`test/compat-bin/gekka-compat-test`) rather than
   * only reporting it missing.
   */
  def locate(envVarName: String = "GEKKA_COMPAT_TEST_BIN"): Result =
    locateOrBuild("gekka-compat-test",
      sys.env.getOrElse(envVarName, "") +: relativeCandidates("gekka-compat-test"))

  /**
   * Locate a usable `gekka-aeron-compat-test` binary for `AeronClusterSpec`.
   * Preserves that spec's historical candidate order — the dedicated Aeron
   * binary first, then `gekka-compat-test` (which retains an `aeron-udp`
   * transport mode) as a fallback — and, when none are usable, builds the
   * dedicated Aeron binary from `test/compat-bin/gekka-aeron-compat-test`.
   */
  def locateAeron(): Result =
    locateOrBuild("gekka-aeron-compat-test",
      Seq(
        sys.env.getOrElse("GEKKA_AERON_COMPAT_TEST_BIN", ""),
        sys.env.getOrElse("GEKKA_COMPAT_TEST_BIN", ""),
      ) ++ relativeCandidates("gekka-aeron-compat-test")
        ++ relativeCandidates("gekka-compat-test"))

  private def locateOrBuild(binaryName: String, candidates: Seq[String]): Result =
    candidates.filter(_.nonEmpty).find(isUsable) match {
      case Some(p) => Found(p)
      case None    => attemptBuild(binaryName)
    }

  private def attemptBuild(binaryName: String): Result = {
    findRepoRoot() match {
      case None =>
        NotFoundNoSource(new java.io.File(".").getCanonicalPath)
      case Some(repoRoot) =>
        val srcDir = new java.io.File(repoRoot, s"test/compat-bin/$binaryName")
        if (!srcDir.isDirectory) {
          NotFoundNoSource(srcDir.getPath)
        } else {
          val target = new java.io.File(repoRoot, s"test/compatibility/akka-multi-node/bin/$binaryName")
          target.getParentFile.mkdirs()
          val cmd = Seq("go", "build", "-o", target.getAbsolutePath, s"./test/compat-bin/$binaryName")
          val commandDescription = cmd.mkString(" ") + " (cwd=" + repoRoot.getPath + ")"
          val output = new ArrayBuffer[String]()
          val logger = ProcessLogger(l => output += l, l => output += l)
          Try((Process(cmd, repoRoot) ! logger)) match {
            case Success(0) => Found(target.getAbsolutePath)
            case Success(_) => BuildFailed(commandDescription, output.mkString("\n"))
            case Failure(e) => BuildFailed(commandDescription, (output :+ e.toString).mkString("\n"))
          }
        }
    }
  }
}
