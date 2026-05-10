package com.example.joiner

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import com.example.{GoReplicator, OrchestratorGate}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// ScalaDDataJoiner joins a Go-Seed cluster and hosts the existing
// GoReplicator actor (defined in DistributedDataServer.scala) at
// /user/goReplicator. A small probe prints "[SCALA-DDATA] <key>=<value>"
// each time the GoReplicator merges incoming gossip, so the Go test
// can assert on per-key state changes.
//
// Args: <seed-host> <seed-port>
object ScalaDDataJoiner extends App {
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

  // Probe prints stdout markers the Go test consumes. Sets are printed
  // as a sorted comma-joined list so the assertion is deterministic.
  class StdoutProbe extends Actor with ActorLogging {
    import GoReplicator._
    def receive: Receive = {
      case CounterValue(key, value) => println(s"[SCALA-DDATA] $key=$value")
      case SetValue(key, elements)  => println(s"[SCALA-DDATA] $key=${elements.toList.sorted.mkString(",")}")
    }
  }
  val probe = system.actorOf(Props(new StdoutProbe), "ddTestProbe")
  system.actorOf(GoReplicator.props(probe), "goReplicator")

  println("--- SCALA DDATA READY ---")
  Await.ready(system.whenTerminated, Duration.Inf)
}
