/*
 * AkkaMultiMemberJoinNode.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{
  InitialStateAsEvents,
  MemberDowned,
  MemberLeft,
  MemberRemoved,
  MemberUp
}

/**
 * AkkaMultiMemberJoinNode — production-shaped Akka 2.6.x cluster of TWO
 * ActorSystems in a single JVM.  Mirrors the actual TestCluster HOCON shape
 * (cluster provider, SBR enabled with defaults, multi-DC=JP, no
 * seed-node/timeout overrides).  Built for the gekka-as-3rd-member
 * Down-after-Welcome reproducer.
 *
 * Why two systems in one JVM:
 *   The single-member AkkaStrictHoconJoinNode reproducer passes — gekka
 *   successfully joins and is promoted to Up.  The failure mode (gekka
 *   gets Downed within ~1s of being Up) only manifests against a
 *   multi-member cluster like TestCluster.  This scenario adds the smallest
 *   possible difference (one extra ActorSystem) so we can determine
 *   whether the trigger is multi-member at all, or something else
 *   specific to the real TestCluster deployment.
 *
 * JVM system properties (must be set; non-zero):
 *   -Dnode.port.a=<P_A>  — first ActorSystem's Artery port
 *   -Dnode.port.b=<P_B>  — second ActorSystem's Artery port
 *
 * Both systems use seed-nodes = [A, B] so they form one cluster.
 *
 * Stdout signals consumed by the Go test:
 *   MULTI_NODE_READY:<P_A>,<P_B>            — both selves reached Up
 *   FOREIGN_MEMBER_UP:<host>:<port>         — a non-self member reached Up
 *   FOREIGN_MEMBER_LEFT:<host>:<port>       — a non-self member started Leaving
 *   FOREIGN_MEMBER_DOWNED:<host>:<port>     — a non-self member was Downed
 *   FOREIGN_MEMBER_REMOVED:<host>:<port>    — a non-self member was Removed
 *
 * The Go test treats FOREIGN_MEMBER_DOWNED for gekka's port as the bug.
 */
object AkkaMultiMemberJoinNode extends App {
  OrchestratorGate.require()

  def readPort(prop: String): Int = {
    val s = sys.props.getOrElse(prop, "")
    if (s.isEmpty || s == "0") {
      Console.err.println(
        s"[AkkaMultiMemberJoinNode] -D$prop=<P> is required and must be non-zero")
      sys.exit(2)
    }
    s.toInt
  }

  val portA = readPort("node.port.a")
  val portB = readPort("node.port.b")
  if (portA == portB) {
    Console.err.println(
      s"[AkkaMultiMemberJoinNode] -Dnode.port.a and -Dnode.port.b must differ")
    sys.exit(2)
  }

  def buildConfig(selfPort: Int) = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = "WARNING"
       |  actor.provider = "cluster"
       |  remote {
       |    log-remote-lifecycle-events = off
       |    artery {
       |      enabled = on
       |      transport = "tcp"
       |      canonical.hostname = "127.0.0.1"
       |      canonical.port = $selfPort
       |    }
       |  }
       |  cluster {
       |    seed-nodes = [
       |      "akka://TestCluster@127.0.0.1:$portA",
       |      "akka://TestCluster@127.0.0.1:$portB"
       |    ]
       |    metrics { enabled = off }
       |    # Two ActorSystems share this JVM; suppress the JMX MBean re-registration
       |    # WARN that Akka logs when the second cluster registers akka:type=Cluster.
       |    jmx.multi-mbeans-in-same-jvm = on
       |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
       |    multi-data-center.self-data-center = "JP"
       |  }
       |  coordinated-shutdown.terminate-actor-system = on
       |}
       |""".stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  // Both systems share the same ActorSystem name so the seed URLs resolve.
  val systemA = ActorSystem("TestCluster", buildConfig(portA))
  val systemB = ActorSystem("TestCluster", buildConfig(portB))

  val clusterA = Cluster(systemA)
  val clusterB = Cluster(systemB)

  val readyA = Promise[Unit]()
  val readyB = Promise[Unit]()

  class MembershipWatcher(self: Cluster, tag: String, ownReady: Promise[Unit])
      extends Actor
      with ActorLogging {
    override def preStart(): Unit =
      self.subscribe(
        context.self,
        InitialStateAsEvents,
        classOf[MemberUp],
        classOf[MemberLeft],
        classOf[MemberDowned],
        classOf[MemberRemoved]
      )
    override def postStop(): Unit = self.unsubscribe(context.self)

    private def hostPort(addr: akka.actor.Address): String = {
      val h = addr.host.getOrElse("?")
      val p = addr.port.map(_.toString).getOrElse("?")
      s"$h:$p"
    }

    def receive: Receive = {
      case MemberUp(m) if m.address == self.selfAddress =>
        log.info("[{}] self Up at {}", tag, m.address)
        if (!ownReady.isCompleted) ownReady.success(())

      case MemberUp(m) =>
        val hp = hostPort(m.address)
        log.info("[{}] foreign Up at {}", tag, m.address)
        println(s"FOREIGN_MEMBER_UP:$hp")
        Console.flush()

      case MemberLeft(m) if m.address != self.selfAddress =>
        val hp = hostPort(m.address)
        log.info("[{}] foreign Leaving at {}", tag, m.address)
        println(s"FOREIGN_MEMBER_LEFT:$hp")
        Console.flush()

      case MemberDowned(m) if m.address != self.selfAddress =>
        val hp = hostPort(m.address)
        log.info("[{}] foreign Downed at {}", tag, m.address)
        println(s"FOREIGN_MEMBER_DOWNED:$hp")
        Console.flush()

      case MemberRemoved(m, _) if m.address != self.selfAddress =>
        val hp = hostPort(m.address)
        log.info("[{}] foreign Removed at {}", tag, m.address)
        println(s"FOREIGN_MEMBER_REMOVED:$hp")
        Console.flush()
    }
  }

  systemA.actorOf(Props(new MembershipWatcher(clusterA, "A", readyA)), "watcher")
  systemB.actorOf(Props(new MembershipWatcher(clusterB, "B", readyB)), "watcher")

  Await.result(readyA.future, 60.seconds)
  Await.result(readyB.future, 60.seconds)

  println(s"MULTI_NODE_READY:$portA,$portB")
  Console.flush()

  // Block forever — jvmproc owns the JVM lifecycle and SIGKILLs on cleanup.
  Await.result(Promise[Unit]().future, scala.concurrent.duration.Duration.Inf)
}
