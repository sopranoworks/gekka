val PekkoVersion = "1.1.2"
val AkkaVersion  = "2.6.21"

// ---------------------------------------------------------------------------
// Root project — Pekko-based server
// ---------------------------------------------------------------------------
lazy val root = (project in file("."))
  .settings(
    name         := "scala-server",
    version      := "0.1",
    scalaVersion := "2.13.12",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor"                 % PekkoVersion,
      "org.apache.pekko" %% "pekko-actor-typed"           % PekkoVersion,
      "org.apache.pekko" %% "pekko-remote"                % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster"               % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-tools"         % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-typed"         % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding"      % PekkoVersion,
      "org.apache.pekko" %% "pekko-distributed-data"      % PekkoVersion,
      "org.apache.pekko" %% "pekko-protobuf-v3"           % PekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson" % PekkoVersion,
      "com.typesafe"      % "config"                       % "1.4.3",
      "ch.qos.logback"    % "logback-classic"              % "1.4.14"
    ),
    Compile / mainClass       := Some("com.example.PekkoServer"),
    Compile / run / mainClass := Some("com.example.PekkoServer"),
    assembly / assemblyJarName := "pekko-mains-assembly.jar",
    assembly / mainClass       := None,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties")                       => MergeStrategy.first
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF"))   => MergeStrategy.discard
      case PathList("reference.conf")                                                  => MergeStrategy.concat
      case PathList("application.conf")                                                => MergeStrategy.concat
      case PathList(ps @ _*) if ps.lastOption.contains("module-info.class")            => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value(x)
    }
  )
  .aggregate(akkaServer, akka2614Server)

// ---------------------------------------------------------------------------
// akka-server sub-project — Akka 2.6.x based server
//
// Kept as a separate sub-project so it gets its own isolated classpath and
// avoids dependency conflicts with the Pekko jars above.
// ---------------------------------------------------------------------------
lazy val akkaServer = (project in file("akka-server"))
  .settings(
    name         := "akka-server",
    version      := "0.1",
    scalaVersion := "2.13.12",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor"         % AkkaVersion,
      "com.typesafe.akka" %% "akka-remote"        % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster"       % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
      "com.typesafe"       % "config"              % "1.4.3"
    ),
    Compile / mainClass       := Some("com.example.AkkaIntegrationNode"),
    Compile / run / mainClass := Some("com.example.AkkaIntegrationNode"),
    assembly / assemblyJarName := "akka-mains-assembly.jar",
    assembly / mainClass       := None,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties")                       => MergeStrategy.first
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF"))   => MergeStrategy.discard
      case PathList("reference.conf")                                                  => MergeStrategy.concat
      case PathList("application.conf")                                                => MergeStrategy.concat
      case PathList(ps @ _*) if ps.lastOption.contains("module-info.class")            => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value(x)
    }
  )

// ---------------------------------------------------------------------------
// akka-2614-server sub-project — Akka 2.6.14 strict-HOCON JOIN reproducer
//
// Sibling of akkaServer, pinned to a different Akka version so the same
// scenario can be exercised against the version the production dashboard's
// cluster runs.  Sources are intentionally duplicated rather than shared
// because each sbt sub-project must be self-contained for `sbt assembly`.
// ---------------------------------------------------------------------------
lazy val akka2614Server = (project in file("akka-2614-server"))
  .settings(
    name         := "akka-2614-server",
    version      := "0.1",
    scalaVersion := "2.13.12",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor"         % "2.6.14",
      "com.typesafe.akka" %% "akka-remote"        % "2.6.14",
      "com.typesafe.akka" %% "akka-cluster"       % "2.6.14",
      "com.typesafe.akka" %% "akka-cluster-tools" % "2.6.14",
      "com.typesafe"       % "config"              % "1.4.3"
    ),
    Compile / mainClass       := Some("com.example.AkkaStrictHoconJoinNode"),
    Compile / run / mainClass := Some("com.example.AkkaStrictHoconJoinNode"),
    assembly / assemblyJarName := "akka-2614-mains-assembly.jar",
    assembly / mainClass       := None,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties")                       => MergeStrategy.first
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF"))   => MergeStrategy.discard
      case PathList("reference.conf")                                                  => MergeStrategy.concat
      case PathList("application.conf")                                                => MergeStrategy.concat
      case PathList(ps @ _*) if ps.lastOption.contains("module-info.class")            => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value(x)
    }
  )
