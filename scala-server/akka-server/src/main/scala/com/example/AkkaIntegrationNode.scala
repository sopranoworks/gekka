/*
 * AkkaIntegrationNode.scala
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
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}

/**
 * EchoActor — echoes any received byte-array or string message back to the sender
 * as raw UTF-8 bytes with the prefix "Echo: ".
 */
class AkkaEchoActor extends Actor with ActorLogging {
  def receive: Receive = {
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("AkkaEchoActor received bytes: {}", str)
      sender() ! s"Echo: $str".getBytes("UTF-8")
    case msg: String =>
      log.info("AkkaEchoActor received string: {}", msg)
      sender() ! s"Echo: $msg".getBytes("UTF-8")
  }
}

/**
 * AkkaBridgeSubscriber — subscribes to the "bridge" DistributedPubSub topic and
 * prints a signal line to stdout for each received message so the Go test
 * can verify delivery.
 *
 * Signals printed:
 *   AKKA_PUBSUB_SUBSCRIBED      — subscription confirmed by the mediator
 *   AKKA_PUBSUB_RECEIVED:<text> — message received on the bridge topic
 */
class AkkaBridgeSubscriber(subscriptionReady: Promise[Unit]) extends Actor with ActorLogging {
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("bridge", self)

  def receive: Receive = {
    case SubscribeAck(Subscribe("bridge", None, `self`)) =>
      log.info("AkkaBridgeSubscriber: subscribed to bridge topic")
      println("AKKA_PUBSUB_SUBSCRIBED")
      Console.flush()
      subscriptionReady.success(())

    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("AkkaBridgeSubscriber received bytes: {}", str)
      println(s"AKKA_PUBSUB_RECEIVED:$str")
      Console.flush()

    case msg: String =>
      log.info("AkkaBridgeSubscriber received string: {}", msg)
      println(s"AKKA_PUBSUB_RECEIVED:$msg")
      Console.flush()

    case other =>
      log.debug("AkkaBridgeSubscriber: unhandled {}", other)
  }
}

/**
 * AkkaIntegrationNode — entry point for the Akka 2.6.x E2E integration test harness.
 *
 * Starts an Akka ActorSystem named "GekkaSystem" on 127.0.0.1:2554 with:
 *   - Cluster provider (so pub/sub and gossip work)
 *   - AkkaEchoActor at /user/echo
 *   - AkkaBridgeSubscriber subscribed to DistributedPubSub topic "bridge"
 *
 * Prints "AKKA_NODE_READY" once the subscription is confirmed and the node
 * is fully operational.
 *
 * Usage (from scala-server directory):
 *   sbt "akka-server/runMain com.example.AkkaIntegrationNode"
 */
object AkkaIntegrationNode extends App {
  val config = ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = "INFO"
      |  actor.provider = cluster
      |  remote.artery {
      |    transport = tcp
      |    canonical.hostname = "127.0.0.1"
      |    canonical.port = 2554
      |  }
      |  cluster {
      |    seed-nodes = ["akka://GekkaSystem@127.0.0.1:2554"]
      |    min-nr-of-members = 1
      |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
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
  system.actorOf(Props[AkkaEchoActor], "echo")

  // Initialize DistributedPubSub mediator (starts gossip internally).
  val _ = DistributedPubSub(system).mediator

  // Subscribe to the "bridge" topic and wait for the SubscribeAck before
  // signalling readiness, so the Go test can immediately publish on "bridge".
  val subscriptionReady = Promise[Unit]()
  system.actorOf(Props(new AkkaBridgeSubscriber(subscriptionReady)), "bridgeSubscriber")

  Await.result(subscriptionReady.future, 30.seconds)

  // Everything is initialized — signal the Go test harness.
  println("AKKA_NODE_READY")
  Console.flush()
}
