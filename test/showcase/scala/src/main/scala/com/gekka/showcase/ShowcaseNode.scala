package com.gekka.showcase

import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object ShowcaseNode {
  def start(nodeLabel: String): ActorSystem = {
    val config: Config = ConfigFactory.load()
    val system = ActorSystem("ShowcaseCluster", config)
    val cluster = Cluster(system)

    // FT1 receiver
    system.actorOf(EchoActor.props, EchoActor.name)

    val peers: List[String] =
      if (config.hasPath("showcase.peers"))
        config.getStringList("showcase.peers").asScala.toList
      else Nil
    if (peers.nonEmpty)
      system.actorOf(TellSenderActor.props(nodeLabel, peers), TellSenderActor.name)

    system.actorOf(AskActor.props, AskActor.name)
    if (peers.nonEmpty)
      system.actorOf(AskSenderActor.props(nodeLabel, peers), AskSenderActor.name)

    cluster.registerOnMemberUp {
      println(s"--- SHOWCASE NODE READY: $nodeLabel ---")
      Console.flush()
    }

    sys.addShutdownHook {
      CoordinatedShutdown(system).run(CoordinatedShutdown.JvmExitReason)
    }

    system
  }
}
