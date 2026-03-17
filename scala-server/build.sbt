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
      "com.typesafe"      % "config"                       % "1.4.3"
    ),
    Compile / mainClass       := Some("com.example.PekkoServer"),
    Compile / run / mainClass := Some("com.example.PekkoServer")
  )
  .aggregate(akkaServer)

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
    Compile / run / mainClass := Some("com.example.AkkaIntegrationNode")
  )
