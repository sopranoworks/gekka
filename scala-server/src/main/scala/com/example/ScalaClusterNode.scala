package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

// ScalaClusterNode starts a single Scala cluster node that joins an existing
// seed (which may be a Go node) whose address is supplied as CLI arguments.
//
// Usage (via sbt):
//   sbt "runMain com.example.ScalaClusterNode <seed-host> <seed-port>"
//
// Stdout signals consumed by TestCluster_GoDominantMixed:
//   "--- SCALA NODE STARTED ---"    process started; Artery handshake underway
//   "[SCALA-NODE] MemberUp: ..."    each MemberUp event
//
// Write "leave\n" to stdin to trigger a graceful Cluster.leave() + shutdown.
object ScalaClusterNode extends App {

  val seedHost = if (args.length > 0) args(0) else "127.0.0.1"
  val seedPort = if (args.length > 1) args(1).toInt else 2550

  // Self port = 0: let the OS pick a free port so multiple instances can coexist.
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
          active-strategy   = keep-oldest
          stable-after      = 10s
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

  // Echo actor so the test can send messages and verify delivery.
  system.actorOf(Props[EchoActor](), "echo")

  // Minimal cluster monitor — print membership events for test log visibility.
  class ScalaMonitor extends Actor {
    val cluster = Cluster(context.system)
    override def preStart(): Unit =
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
    override def postStop(): Unit = cluster.unsubscribe(self)
    def receive: Receive = {
      case MemberUp(m)        => println(s"[SCALA-NODE] MemberUp: ${m.address}")
      case MemberRemoved(m,_) => println(s"[SCALA-NODE] MemberRemoved: ${m.address}")
      case _: MemberEvent     =>
    }
  }
  system.actorOf(Props(new ScalaMonitor), "monitor")

  println("--- SCALA NODE STARTED ---")

  // Block on stdin; "leave\n" triggers a graceful cluster leave then JVM exit.
  new Thread(() => {
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
    val line   = reader.readLine()
    if (line != null && line.trim() == "leave") {
      println("--- SCALA NODE LEAVING ---")
      Cluster(system).leave(Cluster(system).selfAddress)
      Thread.sleep(6000) // give leave message time to propagate
      system.terminate()
    }
  }).start()
}
