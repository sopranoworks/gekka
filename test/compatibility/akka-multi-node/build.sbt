/*
 * build.sbt
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */

val AkkaVersion = "2.6.21"

lazy val root = (project in file("."))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .settings(
    name         := "akka-multi-node-compat",
    version      := "0.1",
    scalaVersion := "2.13.12",
    organization := "com.gekka",

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor"              % AkkaVersion,
      "com.typesafe.akka" %% "akka-remote"             % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster"            % AkkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % "multi-jvm,test",
      "com.typesafe.akka" %% "akka-testkit"            % AkkaVersion % "multi-jvm,test",
      "org.scalatest"     %% "scalatest"               % "3.2.17"    % "multi-jvm,test",
      "com.typesafe"       % "config"                  % "1.4.3",
    ),

    // Multi-JVM source directory
    MultiJvm / sourceDirectory := baseDirectory.value / "src" / "multi-jvm",

    // Prevent parallel test runs — multi-node tests are order-sensitive
    Test / parallelExecution := false,
    MultiJvm / parallelExecution := false,

    // Pass env variables through to the forked JVMs
    MultiJvm / jvmOptions ++= Seq(
      "-Xmx512m",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
  )
