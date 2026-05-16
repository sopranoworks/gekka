package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

/**
 * ClusterListener prints stdout markers for every membership transition it
 * observes from the Pekko cluster.  The markers bypass the logback pipeline
 * (which is gated at WARN for the integration test contract — "a passing
 * run emits zero log lines") so that Go-side scanners always see the events
 * even when log.info messages are suppressed.
 *
 * Markers (one per line, stdout):
 *
 *   SEED_MEMBER_UP:<host>:<port>            non-self MemberUp
 *   SEED_MEMBER_LEFT:<host>:<port>          non-self Leaving
 *   SEED_MEMBER_EXITED:<host>:<port>        non-self Exiting
 *   SEED_MEMBER_REMOVED:<host>:<port>       non-self Removed
 *   SEED_MEMBER_UNREACHABLE:<host>:<port>   non-self detected unreachable
 *
 * Legacy markers — kept for backward compatibility with tests that pinned
 * the Go-side port to 2553:
 *
 *   --- GO NODE UP ---
 *   --- GO NODE UNREACHABLE ---
 *   --- GO NODE REMOVED ---
 */
class ClusterListener extends Actor {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  private def hp(addr: org.apache.pekko.actor.Address): String = {
    val h = addr.host.getOrElse("?")
    val p = addr.port.map(_.toString).getOrElse("?")
    s"$h:$p"
  }

  def receive: Receive = {
    case MemberUp(member) =>
      if (member.address != cluster.selfAddress) {
        println(s"SEED_MEMBER_UP:${hp(member.address)}")
        Console.flush()
      }
      if (member.address.port.contains(2553)) {
        println("--- GO NODE UP ---")
        Console.flush()
      }

    case MemberLeft(member) =>
      if (member.address != cluster.selfAddress) {
        println(s"SEED_MEMBER_LEFT:${hp(member.address)}")
        Console.flush()
      }

    case MemberExited(member) =>
      if (member.address != cluster.selfAddress) {
        println(s"SEED_MEMBER_EXITED:${hp(member.address)}")
        Console.flush()
      }

    case UnreachableMember(member) =>
      if (member.address != cluster.selfAddress) {
        println(s"SEED_MEMBER_UNREACHABLE:${hp(member.address)}")
        Console.flush()
      }
      if (member.address.port.contains(2553)) {
        println("--- GO NODE UNREACHABLE ---")
        Console.flush()
      }

    case MemberRemoved(member, _) =>
      if (member.address != cluster.selfAddress) {
        println(s"SEED_MEMBER_REMOVED:${hp(member.address)}")
        Console.flush()
      }
      if (member.address.port.contains(2553)) {
        println("--- GO NODE REMOVED ---")
        Console.flush()
      }

    case _: MemberEvent => // Joining / WeaklyUp / Down — ignored
  }
}

object ClusterSeedNode extends App {
  OrchestratorGate.require()
  val config = ConfigFactory.load("cluster")
  val system = ActorSystem("ClusterSystem", config)

  system.actorOf(Props[ClusterListener], name = "clusterListener")

  println("--- SEED NODE READY ---")
  Console.flush()
}
