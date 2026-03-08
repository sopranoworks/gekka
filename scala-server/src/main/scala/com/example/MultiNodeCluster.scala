package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

// MultiNodeCluster launches two Scala cluster nodes (seed :2552 and second :2554)
// inside one JVM process for integration testing.
//
// Stdout signals consumed by TestMultiNodeDynamicJoin:
//   "--- MULTI-NODE CLUSTER READY ---"   both Scala nodes are confirmed Up
//   "(Total Up: N)"                      emitted on every MemberUp / MemberRemoved
object MultiNodeCluster extends App {

  // ── Node 1: seed at :2552, config from cluster.conf ──────────────────────────
  val baseConf = ConfigFactory.load("cluster")
  val system1  = ActorSystem("ClusterSystem", baseConf)

  // ── Node 2: second node at :2554, same seed ───────────────────────────────────
  val conf2 = ConfigFactory.parseString(
    """
    pekko.remote.artery.canonical.port = 2554
    pekko.cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    """
  ).withFallback(baseConf)
  val system2 = ActorSystem("ClusterSystem", conf2)

  // Echo actors on both nodes — Go nodes can send []byte messages to /user/echo
  system1.actorOf(Props[EchoActor](), "echo")
  system2.actorOf(Props[EchoActor](), "echo")

  // ── Cluster monitor on node 1 ─────────────────────────────────────────────────
  // Tracks Up member count and emits structured stdout signals.
  class MultiNodeMonitor extends Actor {
    val cluster      = Cluster(context.system)
    var upPorts      = Set.empty[Int]
    var readyPrinted = false

    override def preStart(): Unit =
      // InitialStateAsEvents ensures we receive MemberUp for members already Up
      // at the moment of subscription (e.g. node 1 itself).
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberEvent], classOf[UnreachableMember])

    override def postStop(): Unit = cluster.unsubscribe(self)

    def receive: Receive = {
      case MemberUp(member) =>
        val port = member.address.port.getOrElse(0)
        upPorts += port
        println(s"[MULTI] MemberUp port=$port (Total Up: ${upPorts.size})")
        // Print READY only after both Scala nodes have confirmed Up
        if (!readyPrinted && upPorts.contains(2552) && upPorts.contains(2554)) {
          readyPrinted = true
          println("--- MULTI-NODE CLUSTER READY ---")
        }

      case MemberRemoved(member, _) =>
        val port = member.address.port.getOrElse(0)
        upPorts -= port
        println(s"[MULTI] MemberRemoved port=$port (Total Up: ${upPorts.size})")

      case UnreachableMember(member) =>
        println(s"[MULTI] Unreachable: ${member.address}")

      case _: MemberEvent => // Joining / WeaklyUp / Leaving / Exiting — ignored
    }
  }

  system1.actorOf(Props(new MultiNodeMonitor), "multiNodeMonitor")
}
