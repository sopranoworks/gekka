package com.gekka.showcase

import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._

object ShowcaseNode {
  def start(nodeLabel: String): ActorSystem = {
    val config: Config = ConfigFactory.load()
    val system = ActorSystem("ShowcaseCluster", config)
    val cluster = Cluster(system)

    // FT1 receiver
    system.actorOf(EchoActor.props, "echo")

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
