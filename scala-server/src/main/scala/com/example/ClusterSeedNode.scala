package com.example

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

class ClusterListener extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      if (member.address.port.contains(2553)) {
         println("--- GO NODE UP ---")
      }
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
      if (member.address.port.contains(2553)) {
         println("--- GO NODE UNREACHABLE ---")
      }
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
      if (member.address.port.contains(2553)) {
         println("--- GO NODE REMOVED ---")
      }
    case _: MemberEvent => // ignore
  }
}

object ClusterSeedNode extends App {
  val config = ConfigFactory.load("cluster")
  val system = ActorSystem("ClusterSystem", config)

  system.actorOf(Props[ClusterListener], name = "clusterListener")
  
  println("--- SEED NODE READY ---")
}
