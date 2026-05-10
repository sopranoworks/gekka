package com.example.joiner

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.pekko.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory
import com.example.OrchestratorGate

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// ScalaShardingJoiner joins a Go-Seed cluster and registers a ShardRegion
// "echo" with extractor (id mod 4). The entity actor prints
// "[SCALA-SHARDING] entity=<id> received=<msg>" so the Go test can prove
// at least one shard ended up on the Scala region.
//
// Args: <seed-host> <seed-port>
object ScalaShardingJoiner extends App {
  OrchestratorGate.require()
  val seedHost = if (args.length > 0) args(0) else "127.0.0.1"
  val seedPort = if (args.length > 1) args(1).toInt else 2550

  val conf = ConfigFactory.parseString(s"""
    pekko {
      actor.provider = cluster
      remote.artery {
        transport = tcp
        canonical {
          hostname = "127.0.0.1"
          port = 0
        }
      }
      cluster {
        seed-nodes = ["pekko://ClusterSystem@$seedHost:$seedPort"]
        downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
        split-brain-resolver {
          active-strategy = keep-oldest
          stable-after    = 10s
        }
        failure-detector {
          acceptable-heartbeat-pause = 15s
          heartbeat-interval         = 1s
        }
        configuration-compatibility-check.enforce-on-join = off
        min-nr-of-members = 1
      }
      loglevel = INFO
    }
  """)

  val system = ActorSystem("ClusterSystem", conf)

  class ShardEcho extends Actor with ActorLogging {
    private val entityId = self.path.name
    def receive: Receive = {
      case msg: Array[Byte] =>
        val str = new String(msg, "UTF-8")
        println(s"[SCALA-SHARDING] entity=$entityId received=$str")
      case msg: String =>
        println(s"[SCALA-SHARDING] entity=$entityId received=$msg")
    }
  }

  val NumberOfShards = 4
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case (id: String, payload) => (id, payload)
  }
  val extractShardId: ShardRegion.ExtractShardId = {
    case (id: String, _) => (id.hashCode.abs % NumberOfShards).toString
  }

  ClusterSharding(system).start(
    typeName        = "echo",
    entityProps     = Props(new ShardEcho),
    settings        = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId  = extractShardId
  )

  println("--- SCALA SHARDING READY ---")
  Await.ready(system.whenTerminated, Duration.Inf)
}
