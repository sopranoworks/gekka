package com.example

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import com.typesafe.config.ConfigFactory

// SingletonActor receives byte messages and replies with "Ack: <msg>".
class SingletonActor extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart(): Unit =
    log.info("SingletonActor started on {}", cluster.selfAddress)

  override def postStop(): Unit =
    log.info("SingletonActor stopped on {}", cluster.selfAddress)

  def receive: Receive = {
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      log.info("Singleton received bytes: {}", str)
      sender() ! s"Ack: $str".getBytes("UTF-8")
    case msg: String =>
      log.info("Singleton received string: {}", msg)
      sender() ! s"Ack: $msg".getBytes("UTF-8")
  }
}

// SingletonClusterListener logs events and signals Go node state for integration tests.
class SingletonClusterListener extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      if (member.address.port.contains(2553))
        println("--- GO NODE UP ---")
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, _) =>
      log.info("Member is Removed: {}", member.address)
    case _: MemberEvent => // ignore
  }
}

object ClusterSingletonServer extends App {
  val config = ConfigFactory.load("cluster")
  val system = ActorSystem("ClusterSystem", config)

  // Host the singleton manager; the singleton actor lives at /user/singletonManager/singleton
  system.actorOf(
    ClusterSingletonManager.props(
      singletonProps = Props[SingletonActor],
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(system)
    ),
    name = "singletonManager"
  )

  system.actorOf(Props[SingletonClusterListener], name = "singletonClusterListener")

  println("--- SINGLETON SERVER READY ---")
}
