package com.example.joiner

import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import com.example.OrchestratorGate

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

// ScalaSingletonJoiner joins a Go-Seed cluster, creates a Pekko
// ClusterSingletonProxy pointing at "/user/echoSingleton" on the Go side,
// and triggers Asks at two distinct cluster events:
//
//   first MemberUp        → fire phase1 Ask  (singleton on Go-Seed)
//   first MemberRemoved   → fire phase2 Ask  (singleton on new oldest after handover)
//
// Stdout markers consumed by Go tests:
//   "--- SCALA SINGLETON READY ---"          process started, proxy created
//   "[SCALA-SINGLETON] phase1=<reply>"       reply for the post-Up Ask
//   "[SCALA-SINGLETON] phase2=<reply>"       reply for the post-Removed Ask
//
// Args: <seed-host> <seed-port>
object ScalaSingletonJoiner extends App {
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
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val askTimeout: Timeout = Timeout(5.seconds)

  // Proxy points at the manager actor on the oldest node; the singleton
  // itself lives at "/user/echoSingleton/singleton".
  val proxy = system.actorOf(
    ClusterSingletonProxy.props(
      singletonManagerPath = "/user/echoSingleton",
      settings = ClusterSingletonProxySettings(system)
    ),
    name = "echoSingletonProxy"
  )

  class AskOnEvent(proxyRef: ActorRef) extends Actor with ActorLogging {
    val cluster = Cluster(context.system)
    private var phase1Sent = false
    private var phase2Sent = false
    override def preStart(): Unit =
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
        classOf[MemberEvent])
    override def postStop(): Unit = cluster.unsubscribe(self)
    private def fire(label: String): Unit = {
      val payload = s"hello-from-go-$label".getBytes("UTF-8")
      (proxyRef ? payload).onComplete {
        case Success(b: Array[Byte]) =>
          println(s"[SCALA-SINGLETON] $label=${new String(b, "UTF-8")}")
        case Success(other) =>
          println(s"[SCALA-SINGLETON] $label=UNEXPECTED:$other")
        case Failure(ex) =>
          println(s"[SCALA-SINGLETON] $label=ERROR:${ex.getMessage}")
      }
    }
    def receive: Receive = {
      case _: MemberUp if !phase1Sent =>
        phase1Sent = true
        fire("phase1")
      case _: MemberRemoved if !phase2Sent =>
        phase2Sent = true
        fire("phase2")
      case _: MemberEvent => // ignore other events / additional MemberUp/Removed
    }
  }
  system.actorOf(Props(new AskOnEvent(proxy)), "askOnEvent")

  println("--- SCALA SINGLETON READY ---")
  // Wait forever — test orchestrator owns the JVM lifecycle.
  Await.ready(system.whenTerminated, Duration.Inf)
}
