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

    // Cluster HTTP Management (GET /cluster/members etc.) — the runner's
    // Gate 1 membership poll depends on this endpoint.  Pekko Management
    // does not start automatically; without this call the configured
    // pekko.management.http.port is never bound.
    org.apache.pekko.management.scaladsl.PekkoManagement(system).start()

    // FT1 receiver
    system.actorOf(EchoActor.props, EchoActor.name)

    val peers: List[String] =
      if (config.hasPath("showcase.peers"))
        config.getStringList("showcase.peers").asScala.toList
      else Nil

    // Steady-state anchor (spec §4 / §5.4.2): traffic-actor ERROR
    // accounting is scoped to the strict Gate-2 window, approximated
    // per-node by the first local observation of the full membership Up.
    SteadyAnchor.startWatch(system, peers.size + 1)

    if (peers.nonEmpty)
      system.actorOf(TellSenderActor.props(nodeLabel, peers), TellSenderActor.name)

    system.actorOf(AskActor.props, AskActor.name)
    if (peers.nonEmpty)
      system.actorOf(AskSenderActor.props(nodeLabel, peers), AskSenderActor.name)

    DdataActors.all(nodeLabel).zip(DdataActors.names).foreach { case (p, n) =>
      system.actorOf(p, n.replace('/', '-')) // Pekko paths can't contain '/' inside one segment; use 'ddata-gcounter' etc.
    }

    import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
    val ownRole = s"singleton-$nodeLabel"
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = SingletonActor.props,
        terminationMessage = org.apache.pekko.actor.PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole(ownRole),
      ),
      name = s"singleton-manager-$ownRole",
    )

    system.actorOf(ClientActor.props(nodeLabel), "client")

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
