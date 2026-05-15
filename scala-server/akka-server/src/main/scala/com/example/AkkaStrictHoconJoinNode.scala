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
import akka.cluster.{Cluster, ClusterEvent, MemberStatus}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.actor.{Actor, ActorLogging, Props}

/**
 * AkkaStrictHoconJoinNode — production-shaped Akka 2.6.x single-member cluster
 * used by the gekka strict-HOCON JOIN reproducer.
 *
 * Differences from AkkaIntegrationNode (the happy-path scenario):
 *   - ActorSystem name is "TestSystem" (generic; not the real production name)
 *   - Listening port comes from the JVM system property -Dnode.port=<P>
 *   - JoinConfigCompatChecker is in production mode (enforce-on-join = on),
 *     so a misconfigured joiner is silently dropped — exactly the symptom
 *     the gekka dashboard observed
 *   - No echo / pubsub actors — this scenario tests JOIN only
 *
 * Once the cluster reports self as Up, the line "STRICT_NODE_READY" is printed
 * to stdout so the Go test harness can begin the gekka-side join.
 *
 * Launched by:
 *   jvmproc.SpawnJava(t, ctx, jar, "com.example.AkkaStrictHoconJoinNode",
 *                     nil, jvmproc.Options{Dir: "scala-server",
 *                                          JVMFlags: []string{"-Dnode.port=<P>"}})
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
       |  loglevel = "INFO"
       |  actor.provider = cluster
       |  remote.artery {
       |    transport = tcp
       |    canonical.hostname = "127.0.0.1"
       |    canonical.port = $port
       |  }
       |  cluster {
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

  // Watcher actor: listen for MemberUp(self) and signal readiness.
  val ready = Promise[Unit]()

  class ReadyWatcher extends Actor with ActorLogging {
    override def preStart(): Unit =
      cluster.subscribe(self, InitialStateAsEvents, classOf[MemberUp])
    override def postStop(): Unit = cluster.unsubscribe(self)
    def receive: Receive = {
      case MemberUp(m) if m.address == cluster.selfAddress =>
        log.info("self is Up at {}", m.address)
        if (!ready.isCompleted) ready.success(())
    }
  }

  system.actorOf(Props(new ReadyWatcher), "readyWatcher")

  Await.result(ready.future, 60.seconds)

  println("STRICT_NODE_READY")
  Console.flush()

  // Block forever — jvmproc owns the JVM lifecycle and will SIGKILL on cleanup.
  Await.result(Promise[Unit]().future, scala.concurrent.duration.Duration.Inf)
}
