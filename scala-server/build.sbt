name := "scala-server"

version := "0.1"

scalaVersion := "2.13.12"

val PekkoVersion = "1.0.2"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor" % PekkoVersion,
  "org.apache.pekko" %% "pekko-remote" % PekkoVersion,
  "org.apache.pekko" %% "pekko-cluster" % PekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-tools" % PekkoVersion,
  "org.apache.pekko" %% "pekko-protobuf-v3" % PekkoVersion,
  "org.apache.pekko" %% "pekko-serialization-jackson" % PekkoVersion,
  "com.typesafe" % "config" % "1.4.3"
)

Compile / mainClass := Some("com.example.PekkoServer")
Compile / run / mainClass := Some("com.example.PekkoServer")
