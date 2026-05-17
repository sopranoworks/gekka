/*
 * AkkaStrictHoconJoinNode.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.actor.{Actor, ActorLogging, Props}

/**
 * AkkaStrictHoconJoinNode — production-shaped Akka 2.6.x single-member,
 * multi-DC cluster used by the gekka strict-HOCON JOIN reproducer.
 *
 * Differences from AkkaIntegrationNode (the happy-path scenario):
 *   - ActorSystem name is "TestSystem" (generic; not the real production name)
 *   - Listening port comes from the JVM system property -Dnode.port=<P>
 *   - JoinConfigCompatChecker is in production mode (enforce-on-join = on)
 *   - Multi-DC is ON, dc = "test" (matches the shape of TestCluster-style
 *     production clusters whose dc is non-default)
 *   - No echo / pubsub actors — this scenario tests JOIN only
 *
 * Stdout signals (parsed by the Go test):
 *   STRICT_NODE_READY                       — self has reached Up
 *   STRICT_FOREIGN_MEMBER_UP:<host>:<port>  — a NON-SELF member has reached Up
 *
 * The Go test asserts on STRICT_FOREIGN_MEMBER_UP — the SERVER's
 * authoritative view that the joiner was admitted.  Gekka's local cluster
 * state is logged for debugging but is not load-bearing (it can claim
 * "Up" while the server is silently dropping every message from gekka,
 * as TestCluster's logs proved).
 */
object AkkaStrictHoconJoinNode extends App {
  OrchestratorGate.require()

  val portStr = sys.props.getOrElse("node.port", "")
  if (portStr.isEmpty || portStr == "0") {
    Console.err.println(
      "[AkkaStrictHoconJoinNode] -Dnode.port=<P> is required and must be non-zero")
    sys.exit(2)
  }
  val port = portStr.toInt

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = "WARNING"
       |  actor.provider = cluster
       |  remote.artery {
       |    transport = tcp
       |    canonical.hostname = "127.0.0.1"
       |    canonical.port = $port
       |  }
       |  cluster {
       |    multi-data-center.self-data-center = "test"
       |    seed-nodes = ["akka://TestSystem@127.0.0.1:$port"]
       |    min-nr-of-members = 1
       |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
       |    split-brain-resolver {
       |      active-strategy = keep-oldest
       |      stable-after = 5s
       |    }
       |    configuration-compatibility-check {
       |      enforce-on-join = on
       |    }
       |    failure-detector {
       |      acceptable-heartbeat-pause = 15s
       |      heartbeat-interval = 1s
       |    }
       |  }
       |}
       |""".stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  val system  = ActorSystem("TestSystem", config)
  val cluster = Cluster(system)

  val selfReady = Promise[Unit]()

  class MembershipWatcher extends Actor with ActorLogging {
    override def preStart(): Unit =
      cluster.subscribe(self, InitialStateAsEvents, classOf[MemberUp])
    override def postStop(): Unit = cluster.unsubscribe(self)
    def receive: Receive = {
      case MemberUp(m) if m.address == cluster.selfAddress =>
        log.info("self is Up at {}", m.address)
        if (!selfReady.isCompleted) selfReady.success(())

      case MemberUp(m) =>
        val host = m.address.host.getOrElse("?")
        val mp   = m.address.port.map(_.toString).getOrElse("?")
        log.info("foreign member Up at {}", m.address)
        println(s"STRICT_FOREIGN_MEMBER_UP:$host:$mp")
        Console.flush()
    }
  }

  system.actorOf(Props(new MembershipWatcher), "membershipWatcher")

  Await.result(selfReady.future, 60.seconds)

  println("STRICT_NODE_READY")
  Console.flush()

  // Block forever — jvmproc owns the JVM lifecycle and will SIGKILL on cleanup.
  Await.result(Promise[Unit]().future, scala.concurrent.duration.Duration.Inf)
}
