package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

// MultiNodeClusterRecovery is identical to MultiNodeCluster but overrides
// split-brain-resolver.stable-after to 60s, giving the Go test enough time
// to stop and resume heartbeats before SBR downs the unreachable member.
//
// Stdout signals consumed by TestClusterFailureRecovery:
//   "--- MULTI-NODE CLUSTER READY ---"   both Scala nodes are confirmed Up
//   "(Total Up: N)"                      emitted on every MemberUp / MemberRemoved
//   "[MULTI] Unreachable: <addr>"        a member has been marked unreachable
//   "[MULTI] ReachableMember port=N"     a previously unreachable member recovered
object MultiNodeClusterRecovery extends App {

  val baseConf = ConfigFactory.load("cluster")

  // Override SBR stable-after to give the test time to resume heartbeats.
  val recoveryOverride = ConfigFactory.parseString(
    "pekko.cluster.split-brain-resolver.stable-after = 60s"
  )

  // ── Node 1: seed at :2552 ─────────────────────────────────────────────────
  val conf1    = recoveryOverride.withFallback(baseConf)
  val system1  = ActorSystem("ClusterSystem", conf1)

  // ── Node 2: second node at :2554 ──────────────────────────────────────────
  val conf2 = ConfigFactory.parseString(
    """
    pekko.remote.artery.canonical.port = 2554
    pekko.cluster.seed-nodes = ["pekko://ClusterSystem@127.0.0.1:2552"]
    pekko.cluster.split-brain-resolver.stable-after = 60s
    """
  ).withFallback(baseConf)
  val system2 = ActorSystem("ClusterSystem", conf2)

  system1.actorOf(Props[EchoActor](), "echo")
  system2.actorOf(Props[EchoActor](), "echo")

  // ── Cluster monitor on node 1 ─────────────────────────────────────────────
  // Subscribes to MemberEvent, UnreachableMember, and ReachableMember so that
  // the Go test can observe the full failure-recovery cycle.
  class RecoveryMonitor extends Actor {
    val cluster      = Cluster(context.system)
    var upPorts      = Set.empty[Int]
    var readyPrinted = false

    override def preStart(): Unit =
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberEvent], classOf[UnreachableMember], classOf[ReachableMember])

    override def postStop(): Unit = cluster.unsubscribe(self)

    def receive: Receive = {
      case MemberUp(member) =>
        val port = member.address.port.getOrElse(0)
        upPorts += port
        println(s"[MULTI] MemberUp port=$port (Total Up: ${upPorts.size})")
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

      case ReachableMember(member) =>
        val port = member.address.port.getOrElse(0)
        println(s"[MULTI] ReachableMember port=$port")

      case _: MemberEvent => // Joining / WeaklyUp / Leaving / Exiting — ignored
    }
  }

  system1.actorOf(Props(new RecoveryMonitor), "recoveryMonitor")
}
