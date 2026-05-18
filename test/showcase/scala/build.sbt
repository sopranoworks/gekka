/*
 * build.sbt — gekka_showcase_test Scala side
 * SPDX-License-Identifier: MIT
 */

val PekkoVersion = "1.1.2"

lazy val root = (project in file("."))
  .settings(
    name         := "showcase-scala",
    version      := "0.1",
    scalaVersion := "2.13.12",
    organization := "com.gekka",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor"                 % PekkoVersion,
      "org.apache.pekko" %% "pekko-actor-typed"           % PekkoVersion,
      "org.apache.pekko" %% "pekko-remote"                % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster"               % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-tools"         % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed"         % PekkoVersion,
      "org.apache.pekko" %% "pekko-distributed-data"      % PekkoVersion,
      "org.apache.pekko" %% "pekko-protobuf-v3"           % PekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % PekkoVersion,
      "org.apache.pekko" %% "pekko-management"            % "1.0.0",
      "org.apache.pekko" %% "pekko-management-cluster-http" % "1.0.0",
      "com.typesafe"      % "config"                       % "1.4.3",
      "ch.qos.logback"    % "logback-classic"              % "1.4.14",
    ),
    Compile / mainClass       := Some("com.gekka.showcase.Main"),
    Compile / run / mainClass := Some("com.gekka.showcase.Main"),
    assembly / assemblyJarName := "showcase-scala-assembly.jar",
    assembly / mainClass       := Some("com.gekka.showcase.Main"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*)       => MergeStrategy.last
      case "reference.conf"                     => MergeStrategy.concat
      case "application.conf"                   => MergeStrategy.concat
      case "logback.xml"                        => MergeStrategy.last
      case _                                    => MergeStrategy.first
    },
  )
