/*
 * PekkoIntegrationNode.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props}
import org.apache.pekko.cluster.pubsub.DistributedPubSub
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}

/**
 * IntegrationEchoActor — echoes any received byte-array or string message back to the sender
 * as raw UTF-8 bytes with the prefix "Echo: ".
 */
class IntegrationEchoActor extends Actor with ActorLogging {
  def receive: Receive = {
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("EchoActor received bytes: {}", str)
      sender() ! s"Echo: $str".getBytes("UTF-8")
    case msg: String =>
      log.info("EchoActor received string: {}", msg)
      sender() ! s"Echo: $msg".getBytes("UTF-8")
  }
}

/**
 * BridgeSubscriber — subscribes to the "bridge" DistributedPubSub topic and
 * prints a signal line to stdout for each received message so the Go test
 * can verify delivery.
 *
 * Signals printed:
 *   PEKKO_PUBSUB_SUBSCRIBED      — subscription confirmed by the mediator
 *   PEKKO_PUBSUB_RECEIVED:<text> — message received on the bridge topic
 */
class BridgeSubscriber(subscriptionReady: Promise[Unit]) extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("bridge", self)

  def receive: Receive = {
    case SubscribeAck(Subscribe("bridge", None, `self`)) =>
      log.info("BridgeSubscriber: subscribed to bridge topic")
      println("PEKKO_PUBSUB_SUBSCRIBED")
      Console.flush()
      subscriptionReady.success(())

    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("BridgeSubscriber received bytes: {}", str)
      println(s"PEKKO_PUBSUB_RECEIVED:$str")
      Console.flush()

    case msg: String =>
      log.info("BridgeSubscriber received string: {}", msg)
      println(s"PEKKO_PUBSUB_RECEIVED:$msg")
      Console.flush()

    case other =>
      log.debug("BridgeSubscriber: unhandled {}", other)
  }
}

/**
 * IntegrationSingleton — a simple singleton actor hosted by ClusterSingletonManager.
 *
 * It echoes any byte-array or string message back to the sender with the
 * "Singleton: " prefix, and prints a signal line to stdout for each received
 * message so the Go test can verify delivery via the ClusterSingletonProxy.
 *
 * Signals printed:
 *   PEKKO_SINGLETON_STARTED      — printed in preStart when the singleton begins running
 *   PEKKO_SINGLETON_RECEIVED:<text> — printed each time a message is received
 */
class IntegrationSingleton extends Actor with ActorLogging {
  override def preStart(): Unit = {
    log.info("IntegrationSingleton started on {}", context.system.settings.config.getString("pekko.remote.artery.canonical.hostname"))
    println("PEKKO_SINGLETON_STARTED")
    Console.flush()
  }

  def receive: Receive = {
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("IntegrationSingleton received bytes: {}", str)
      println(s"PEKKO_SINGLETON_RECEIVED:$str")
      Console.flush()
      sender() ! s"Singleton: $str".getBytes("UTF-8")

    case msg: String =>
      log.info("IntegrationSingleton received string: {}", msg)
      println(s"PEKKO_SINGLETON_RECEIVED:$msg")
      Console.flush()
      sender() ! s"Singleton: $msg".getBytes("UTF-8")

    case other =>
      log.debug("IntegrationSingleton: unhandled {}", other)
  }
}

/**
 * PekkoIntegrationNode — single entry point for the E2E integration test harness.
 *
 * Starts a Pekko ActorSystem named "GekkaSystem" on 127.0.0.1:2552 with:
 *   - Cluster provider (so pub/sub and gossip work)
 *   - IntegrationEchoActor at /user/echo
 *   - BridgeSubscriber subscribed to DistributedPubSub topic "bridge"
 *   - IntegrationSingleton hosted at /user/singletonManager/singleton via
 *     ClusterSingletonManager
 *
 * Prints "PEKKO_NODE_READY" once the subscription is confirmed and the node
 * is fully operational.  The singleton prints "PEKKO_SINGLETON_STARTED" when
 * it starts running on this node.
 *
 * Usage (from scala-server directory):
 *   sbt "runMain com.example.PekkoIntegrationNode"
 */
object PekkoIntegrationNode extends App {
  val config = ConfigFactory.parseString(
    """
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
      |      active-strategy = keep-oldest
      |      stable-after = 5s
      |    }
      |    configuration-compatibility-check {
      |      enforce-on-join = off
      |    }
      |    failure-detector {
      |      acceptable-heartbeat-pause = 15s
      |      heartbeat-interval = 1s
      |    }
      |  }
      |}
      |""".stripMargin
  ).withFallback(ConfigFactory.defaultReference())

  val system = ActorSystem("GekkaSystem", config)

  // Register the echo actor at /user/echo.
  system.actorOf(Props[IntegrationEchoActor], "echo")

  // Initialize DistributedPubSub mediator (starts gossip internally).
  val _ = DistributedPubSub(system).mediator

  // Subscribe to the "bridge" topic and wait for the SubscribeAck before
  // signalling readiness, so the Go test can immediately publish on "bridge".
  val subscriptionReady = Promise[Unit]()
  system.actorOf(Props(new BridgeSubscriber(subscriptionReady)), "bridgeSubscriber")

  // Host the singleton at /user/singletonManager/singleton.
  // The singleton is automatically started when this node is the oldest Up
  // member (which is immediately true for a single-seed cluster).
  system.actorOf(
    ClusterSingletonManager.props(
      singletonProps = Props[IntegrationSingleton],
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(system)
    ),
    name = "singletonManager"
  )

  Await.result(subscriptionReady.future, 30.seconds)

  // Everything is initialized — signal the Go test harness.
  println("PEKKO_NODE_READY")
  Console.flush()
}
