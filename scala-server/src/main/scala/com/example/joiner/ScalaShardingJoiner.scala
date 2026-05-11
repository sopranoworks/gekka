package com.example.joiner

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.apache.pekko.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.fasterxml.jackson.databind.ObjectMapper
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
        # Disable Artery compression for cross-language interop: gekka does
        # not maintain a compatible CompressionTable, so leaving compression
        # enabled makes Pekko's Decoder drop every inbound frame as
        # "compressed with a table that has already been discarded"
        # (originUid table 0, attempted id 65535).
        advanced.compression {
          actor-refs.max = 0
          manifests.max  = 0
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

  // Gekka encodes ShardingEnvelope as JSON and forwards it across nodes
  // using ByteArraySerializer (id=4). The fields of that JSON object are
  // {"EntityId":"...", "ShardId":"...", "Message":<json>, ...}. Decode it
  // here so Pekko's region can route to the correct entity and the entity
  // actor can print [SCALA-SHARDING] entity=<id> received=<msg>.
  private val jsonMapper = new ObjectMapper()
  private def decodeGekkaEnvelope(bytes: Array[Byte]): (String, String) = {
    val node = jsonMapper.readTree(bytes)
    val id   = Option(node.get("EntityId")).map(_.asText("")).getOrElse("")
    val mNode = node.get("Message")
    val msg = if (mNode == null) ""
              else if (mNode.isTextual) mNode.asText("")
              else mNode.toString
    (id, msg)
  }

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case (id: String, payload) => (id, payload)
    case bytes: Array[Byte] =>
      val (id, msg) = decodeGekkaEnvelope(bytes)
      (id, msg)
  }
  val extractShardId: ShardRegion.ExtractShardId = {
    case (id: String, _) => (id.hashCode.abs % NumberOfShards).toString
    case bytes: Array[Byte] =>
      val (id, _) = decodeGekkaEnvelope(bytes)
      (id.hashCode.abs % NumberOfShards).toString
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
