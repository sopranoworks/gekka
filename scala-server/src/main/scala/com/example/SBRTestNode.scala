/*
 * SBRTestNode.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.pekko.cluster.{Cluster => PekkoCluster}
import org.apache.pekko.cluster.ClusterEvent._

/**
 * SBREventListener — extends ClusterEventListener to additionally capture
 * UnreachableMember, ReachableMember, and MemberDowned events needed by
 * the SBR integration tests.
 *
 * Signals printed (in addition to those from ClusterEventListener):
 *   PEKKO_MEMBER_UNREACHABLE:<host>:<port>  — member became unreachable
 *   PEKKO_MEMBER_REACHABLE:<host>:<port>    — unreachable member came back
 *   PEKKO_MEMBER_DOWN:<host>:<port>         — SBR marked member as Down
 */
class SBREventListener extends Actor with ActorLogging {
  val cluster = PekkoCluster(context.system)

  override def preStart(): Unit =
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberUp],
      classOf[MemberLeft],
      classOf[MemberExited],
      classOf[MemberRemoved],
      classOf[UnreachableMember],
      classOf[ReachableMember],
      classOf[MemberDowned]
    )

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_UP:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case MemberLeft(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_LEFT:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case MemberExited(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_EXITED:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case MemberRemoved(member, _) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_REMOVED:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case UnreachableMember(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_UNREACHABLE:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case ReachableMember(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_REACHABLE:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()

    case MemberDowned(member) =>
      val addr = member.address
      println(s"PEKKO_MEMBER_DOWN:${addr.host.getOrElse("")}:${addr.port.getOrElse(0)}")
      Console.flush()
  }
}

/**
 * SBRTestNode — a lightweight Pekko cluster node for SBR integration tests.
 *
 * Accepts one optional command-line argument: the SBR active-strategy name.
 * Defaults to "keep-majority". Also accepts "keep-oldest".
 *
 * The node uses aggressive failure-detector settings so SBR fires quickly
 * during tests:
 *   - heartbeat-interval        = 500ms
 *   - acceptable-heartbeat-pause = 3s
 *   - stable-after              = 2s
 *
 * Signals printed:
 *   SBR_NODE_READY               — node is up and ready for the Go test
 *   PEKKO_MEMBER_UP:<h>:<p>      — member Up
 *   PEKKO_MEMBER_UNREACHABLE:<h>:<p> — member unreachable (FD fired)
 *   PEKKO_MEMBER_DOWN:<h>:<p>    — SBR marked member as Down
 *   PEKKO_MEMBER_REMOVED:<h>:<p> — member fully removed
 *
 * Usage (from scala-server directory):
 *   sbt "runMain com.example.SBRTestNode keep-majority"
 *   sbt "runMain com.example.SBRTestNode keep-oldest"
 */
object SBRTestNode extends App {
  val strategy = if (args.nonEmpty) args(0) else "keep-majority"

  val config = ConfigFactory.parseString(
    s"""
      |pekko {
      |  loglevel = "INFO"
      |  actor.provider = cluster
      |  remote.artery {
      |    transport = tcp
      |    canonical.hostname = "127.0.0.1"
      |    canonical.port = 2552
      |  }
      |  cluster {
      |    seed-nodes = ["pekko://GekkaSystem@127.0.0.1:2552"]
      |    min-nr-of-members = 1
      |    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
      |    split-brain-resolver {
      |      active-strategy = $strategy
      |      stable-after = 2s
      |      keep-oldest {
      |        down-if-alone = off
      |      }
      |    }
      |    configuration-compatibility-check {
      |      enforce-on-join = off
      |    }
      |    failure-detector {
      |      heartbeat-interval = 500ms
      |      acceptable-heartbeat-pause = 3s
      |      threshold = 8.0
      |    }
      |  }
      |}
      |""".stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  val system = ActorSystem("GekkaSystem", config)

  system.actorOf(Props[SBREventListener], "sbrEventListener")

  println("SBR_NODE_READY")
  Console.flush()
}
