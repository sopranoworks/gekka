package com.example.joiner

import org.apache.pekko.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, ActorSelection, ActorSystem, Identify, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import com.example.OrchestratorGate

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

// ScalaAskJoiner joins a Go-Seed cluster and, once it sees MemberUp for
// the seed, sends Ask("hello-from-scala") to a Go-hosted echo actor.
// Exits 0 on receiving "Echo: hello-from-scala", 1 otherwise.
//
// Args: <seed-host> <seed-port> <echo-actor-path>
//   echo-actor-path = "pekko://ClusterSystem@127.0.0.1:2550/user/echo"
object ScalaAskJoiner extends App {
  OrchestratorGate.require()
  val seedHost = if (args.length > 0) args(0) else "127.0.0.1"
  val seedPort = if (args.length > 1) args(1).toInt else 2550
  val echoPath = if (args.length > 2) args(2) else s"pekko://ClusterSystem@$seedHost:$seedPort/user/echo"

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
  implicit val timeout: Timeout = Timeout(5.seconds)

  // On the first MemberUp event, resolve the Go-hosted echo actor via
  // Identify (so the subsequent Ask uses the resolved ActorRef and the
  // raw ByteArraySerializer, not actorSelection's MessageContainer
  // wrapper which gekka's inbound dispatch cannot decode), then send a
  // single Ask and exit based on the result.
  class AskOnceOnUp extends Actor with ActorLogging {
    val cluster = Cluster(context.system)
    private var done = false
    override def preStart(): Unit =
      cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
    override def postStop(): Unit = cluster.unsubscribe(self)
    def receive: Receive = {
      case _: MemberUp if !done =>
        done = true
        val sel: ActorSelection = context.actorSelection(echoPath)
        sel ! Identify("echo")

      case ActorIdentity("echo", Some(ref: ActorRef)) =>
        val payload = "hello-from-scala".getBytes("UTF-8")
        (ref ? payload).onComplete {
          case Success(b: Array[Byte]) =>
            val s = new String(b, "UTF-8")
            println(s"[SCALA-ASK] reply=$s")
            if (s == "Echo: hello-from-scala") {
              system.terminate()
              sys.exit(0)
            } else {
              system.terminate()
              sys.exit(1)
            }
          case Success(other) =>
            println(s"[SCALA-ASK] reply=UNEXPECTED:$other")
            system.terminate()
            sys.exit(1)
          case Failure(ex) =>
            println(s"[SCALA-ASK] reply=ERROR:${ex.getMessage}")
            system.terminate()
            sys.exit(1)
        }

      case ActorIdentity("echo", None) =>
        println("[SCALA-ASK] reply=ERROR:echo-not-found")
        system.terminate()
        sys.exit(1)

      case _: MemberEvent => // ignore
    }
  }
  system.actorOf(Props(new AskOnceOnUp), "askOnUp")

  println("--- SCALA ASK READY ---")
  Await.ready(system.whenTerminated, Duration.Inf)
}
