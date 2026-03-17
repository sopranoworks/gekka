/*
 * PekkoIntegrationNode.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.example

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import org.apache.pekko.actor.typed.delivery.{ConsumerController, ProducerController}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.typed.{ActorRef => TypedActorRef, Behavior}
import org.apache.pekko.cluster.{Cluster => PekkoCluster, MemberStatus}
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.cluster.pubsub.DistributedPubSub
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

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

// ─── Reliable Delivery ─────────────────────────────────────────────────────

/**
 * ReliableConsumerBehavior — typed actor wrapping Pekko's ConsumerController.
 *
 * Signals printed:
 *   PEKKO_DELIVERY_CONSUMER_READY       — consumer connected to ConsumerController
 *   PEKKO_DELIVERY_RECEIVED:<text>      — a message was delivered and confirmed
 */
object ReliableConsumerBehavior {
  sealed trait Command
  final case class Start(
      cc: TypedActorRef[ConsumerController.Command[Array[Byte]]]
  ) extends Command
  private final case class Wrapped(
      d: ConsumerController.Delivery[Array[Byte]]
  ) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { ctx =>
    val adapter =
      ctx.messageAdapter[ConsumerController.Delivery[Array[Byte]]](Wrapped(_))
    Behaviors.receiveMessage {
      case Start(cc) =>
        cc ! ConsumerController.Start(adapter)
        println("PEKKO_DELIVERY_CONSUMER_READY")
        Console.flush()
        Behaviors.same
      case Wrapped(d) =>
        val text = new String(d.message, "UTF-8")
        println(s"PEKKO_DELIVERY_RECEIVED:$text")
        Console.flush()
        d.confirmTo ! ConsumerController.Confirmed
        Behaviors.same
    }
  }
}

/**
 * ReliableProducerBehavior — typed actor wrapping Pekko's ProducerController.
 * Sends up to totalToSend messages, one per RequestNext signal.
 *
 * Signals printed:
 *   PEKKO_DELIVERY_PRODUCER_NEXT:<n>    — message n handed to ProducerController
 */
object ReliableProducerBehavior {
  // Pekko 1.1.x: ProducerController.Start requires ActorRef[RequestNext[T]] directly.
  // The message-adapter indirection used in 1.0.x is no longer needed.
  def apply(totalToSend: Int): Behavior[ProducerController.RequestNext[Array[Byte]]] = {
    var sent = 0
    Behaviors.receiveMessage { rn =>
      if (sent < totalToSend) {
        val msg = s"scala-delivery-$sent".getBytes("UTF-8")
        rn.sendNextTo ! msg
        println(s"PEKKO_DELIVERY_PRODUCER_NEXT:$sent")
        Console.flush()
        sent += 1
      }
      Behaviors.same
    }
  }
}

/**
 * ClusterEventListener — subscribes to cluster domain events and prints
 * signal lines to stdout so the Go integration test can verify that a
 * departing member drives through the expected status transitions.
 *
 * Signals printed:
 *   PEKKO_MEMBER_UP:<host>:<port>           — member joined with status Up
 *   PEKKO_MEMBER_LEFT:<host>:<port>         — member is Leaving
 *   PEKKO_MEMBER_EXITED:<host>:<port>       — member transitioned to Exiting
 *   PEKKO_MEMBER_REMOVED:<host>:<port>      — member fully removed
 *   PEKKO_MEMBER_UNREACHABLE:<host>:<port>  — member detected as unreachable
 *   PEKKO_MEMBER_REACHABLE:<host>:<port>    — unreachable member came back
 *   PEKKO_MEMBER_DOWN:<host>:<port>         — SBR marked member as Down
 */
class ClusterEventListener extends Actor with ActorLogging {
  val cluster = PekkoCluster(context.system)

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberLeft],
      classOf[MemberExited], classOf[MemberRemoved],
      classOf[UnreachableMember], classOf[ReachableMember],
      classOf[MemberDowned])

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

  // Register the cluster event listener so the Go test can observe member transitions.
  system.actorOf(Props[ClusterEventListener], "clusterEventListener")

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

  implicit val ec: ExecutionContext = system.dispatcher

  // ── Reliable Delivery: Scala consumer / Go producer (Go→Scala direction) ─
  // Scala spawns a ConsumerController at /user/scalaConsumer and registers it
  // with Go's ProducerController at /user/goProducer on 127.0.0.1:2553.
  // The ConsumerController sends RegisterConsumer to Go, which then sends
  // SequencedMessages here.
  val scalaConsumerBehavior =
    system.spawn(ReliableConsumerBehavior(), "scalaConsumerBehavior")

  val scalaConsumerController =
    system.spawn(ConsumerController[Array[Byte]](), "scalaConsumer")

  scalaConsumerBehavior ! ReliableConsumerBehavior.Start(scalaConsumerController)

  // Resolve Go's ProducerController and connect when available.
  val goProducerPath = "pekko://GekkaSystem@127.0.0.1:2553/user/goProducer"
  system.actorSelection(goProducerPath).resolveOne(20.seconds).foreach { classicRef =>
    val typedGoProducer = classicRef.toTyped[ProducerController.Command[Array[Byte]]]
    scalaConsumerController ! ConsumerController.RegisterToProducerController(
      typedGoProducer
    )
  }

  // ── Reliable Delivery: Scala producer / Go consumer (Scala→Go direction) ─
  // Scala spawns a ProducerController at /user/scalaProducer.
  // Go's ConsumerController (at /user/goConsumer) sends RegisterConsumer here,
  // which triggers Scala to start sending SequencedMessages to Go.
  val scalaProducerBehavior =
    system.spawn(ReliableProducerBehavior(10), "scalaProducerBehavior")

  val scalaProducerController = system.spawn(
    ProducerController[Array[Byte]]("scala-producer", durableQueueBehavior = None),
    "scalaProducer"
  )
  scalaProducerController ! ProducerController.Start(scalaProducerBehavior)

  // Everything is initialized — signal the Go test harness.
  println("PEKKO_NODE_READY")
  Console.flush()
}
