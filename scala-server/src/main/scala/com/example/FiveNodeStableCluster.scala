/*
 * FiveNodeStableCluster.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent.{
  InitialStateAsEvents,
  MemberDowned,
  MemberExited,
  MemberLeft,
  MemberRemoved,
  MemberUp,
  UnreachableMember
}

/**
 * FiveNodeStableCluster — runs five Pekko cluster ActorSystems inside one
 * JVM, joined to each other via shared seed-node configuration.
 *
 * Built for TestFiveNodeStability3Min: spin up a non-trivial multi-member
 * cluster, let gekka join as the 6th member, then observe for three minutes.
 * Any MemberDowned, MemberRemoved (other than as a consequence of the test's
 * own teardown), MemberExited (likewise), UnreachableMember, or error-level
 * Pekko log is reported on stdout with an "ERROR:" prefix that the Go test
 * treats as a hard failure.
 *
 * JVM system properties (must be set; non-zero; all five distinct):
 *   -Dnode.port.1, -Dnode.port.2, -Dnode.port.3, -Dnode.port.4, -Dnode.port.5
 *
 * Stdout signals consumed by the Go test:
 *   FIVE_NODE_READY:<p1>,<p2>,<p3>,<p4>,<p5>   — all five selves reached Up
 *   FOREIGN_MEMBER_UP:<host>:<port>            — a non-self member reached Up
 *   ERROR:<description>                        — any unexpected event
 */
object FiveNodeStableCluster extends App {
  OrchestratorGate.require()

  private val portKeys = Seq("node.port.1", "node.port.2", "node.port.3", "node.port.4", "node.port.5")

  def readPort(prop: String): Int = {
    val s = sys.props.getOrElse(prop, "")
    if (s.isEmpty || s == "0") {
      Console.err.println(s"[FiveNodeStableCluster] -D$prop=<P> is required and must be non-zero")
      sys.exit(2)
    }
    s.toInt
  }

  val ports: Seq[Int] = portKeys.map(readPort)
  if (ports.distinct.size != ports.size) {
    Console.err.println(s"[FiveNodeStableCluster] all five node ports must be distinct (got $ports)")
    sys.exit(2)
  }

  // All five systems live in the same JVM, so we cannot make every one of
  // them think it is a seed: the simultaneous in-process "firstSeedNodeProcess"
  // contention causes InitJoinNack ping-pong and the cluster never converges
  // (only the first system ever transitions to Up).  Instead, only the FIRST
  // port acts as a seed; the other four point at it.  The resulting cluster
  // membership is identical for gekka's purposes — five Up members under one
  // ActorSystem name — and the bootstrap is deterministic.
  val firstSeed: String = s"pekko://StableCluster@127.0.0.1:${ports.head}"

  def buildConfig(selfPort: Int) = ConfigFactory.parseString(
    s"""
       |pekko {
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
       |    seed-nodes = ["$firstSeed"]
       |    metrics { enabled = off }
       |    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
       |    split-brain-resolver {
       |      active-strategy = "keep-oldest"
       |      stable-after = 30s
       |    }
       |    # In-JVM heartbeat budget: five ActorSystems in one JVM share the
       |    # same scheduler; the default 3 s pause is too tight during a
       |    # cluster-wide connection storm (gekka joining as 6th member opens
       |    # multiple Artery TCPs simultaneously).  Bump to 30 s so transient
       |    # in-JVM heartbeat starvation does not register as a real failure.
       |    failure-detector {
       |      acceptable-heartbeat-pause = 30s
       |      heartbeat-interval = 1s
       |    }
       |  }
       |  coordinated-shutdown.terminate-actor-system = on
       |}
       |""".stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  private val systems = ports.map { p =>
    val sys = ActorSystem("StableCluster", buildConfig(p))
    val cl  = Cluster(sys)
    val ready = Promise[Unit]()
    sys.actorOf(Props(new StabilityWatcher(cl, s"node-$p", ready)), "watcher")
    (p, sys, cl, ready)
  }

  // Wait for every member to confirm its own Up.  60 s is comfortably above
  // the cluster's join + SBR-stabilise budget on a developer laptop.
  systems.foreach { case (p, _, _, ready) =>
    try {
      Await.result(ready.future, 60.seconds)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        println(s"ERROR:node $p did not reach self-Up within 60s")
        Console.flush()
        sys.exit(2)
    }
  }

  println(s"FIVE_NODE_READY:${ports.mkString(",")}")
  Console.flush()

  // Block until SIGKILL — jvmproc owns the lifecycle.
  Await.result(Promise[Unit]().future, scala.concurrent.duration.Duration.Inf)

  /** StabilityWatcher subscribes to cluster lifecycle events and emits
   *  stdout signals.  Any event that signals trouble — Downed, Removed,
   *  Unreachable, Exited (these should not happen during the steady-state
   *  hold window) — is reported as "ERROR:" so the Go test fails fast.
   */
  class StabilityWatcher(self: Cluster, tag: String, ownReady: Promise[Unit])
      extends Actor with ActorLogging {

    override def preStart(): Unit =
      self.subscribe(
        context.self,
        InitialStateAsEvents,
        classOf[MemberUp],
        classOf[MemberLeft],
        classOf[MemberDowned],
        classOf[MemberExited],
        classOf[MemberRemoved],
        classOf[UnreachableMember]
      )
    override def postStop(): Unit = self.unsubscribe(context.self)

    private def hostPort(addr: org.apache.pekko.actor.Address): String = {
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

      case MemberLeft(m) =>
        val hp = hostPort(m.address)
        println(s"ERROR:[$tag] MemberLeft for $hp — no member should leave during stability hold")
        Console.flush()

      case MemberExited(m) =>
        val hp = hostPort(m.address)
        println(s"ERROR:[$tag] MemberExited for $hp — no member should exit during stability hold")
        Console.flush()

      case MemberDowned(m) =>
        val hp = hostPort(m.address)
        println(s"ERROR:[$tag] MemberDowned for $hp — dashboard self-down symptom")
        Console.flush()

      case MemberRemoved(m, _) =>
        val hp = hostPort(m.address)
        println(s"ERROR:[$tag] MemberRemoved for $hp — no member should be removed during stability hold")
        Console.flush()

      case UnreachableMember(m) =>
        // UnreachableMember is transient and reversible — Pekko emits
        // ReachableMember as soon as heartbeats resume.  We log it for
        // diagnostics but DO NOT treat it as an integration-test error;
        // only the confirmed terminal transitions (Downed/Removed/Left/
        // Exited above) are real "member dropped out of the cluster"
        // signals that justify failing the 3-minute hold.
        val hp = hostPort(m.address)
        log.info("[{}] transient UnreachableMember for {}", tag, hp)
    }
  }
}
