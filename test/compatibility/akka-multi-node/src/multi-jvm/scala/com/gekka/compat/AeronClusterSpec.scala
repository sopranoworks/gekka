/*
 * AeronClusterSpec.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * Hybrid Aeron-UDP cluster compatibility test.
 *
 * Topology
 * ────────
 *   JVM-1  role "AeronSystem" — Akka seed node (aeron-udp, port 2561)
 *   Go subprocess             — Gekka node      (aeron-udp, port 2563)
 *
 * The test verifies:
 *   1. The Akka node forms a single-member cluster over Aeron UDP.
 *   2. The Gekka (Go) node joins as a second member via the native Aeron UDP
 *      transport and reaches MemberUp status.
 *   3. A Ping/Pong message exchange between the Go node and the EchoActor on
 *      the Akka node succeeds (Step-1→Step-2→Step-3→Step-4 chain).
 *   4. The Go node remains reachable (no UnreachableMember event) for the
 *      60-second stability window.
 *
 * Run with:  sbt multi-jvm:test
 *   OR:      sbt "multi-jvm:testOnly com.gekka.compat.AeronSystemMultiJvmNode1"
 *
 * Required binaries (build first):
 *   go build -o bin/gekka-aeron-compat-test ./test/compat-bin/gekka-aeron-compat-test
 *   go build -o bin/gekka-compat-test        ./test/compat-bin/gekka-compat-test
 */
package com.gekka.compat

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

// ── Echo actor ──────────────────────────────────────────────────────────────
//
// Receives ping steps from the Go node, notifies the test probe, and replies
// with the next step so the Go node can continue the chain.

class AeronEchoActor(probe: ActorRef) extends Actor {
  var count = 0
  def receive: Receive = {
    case msg: String =>
      count += 1
      println(s"[akka/aeron] AeronEchoActor received: $msg (count=$count)")
      Console.flush()
      probe ! msg
      msg match {
        case "Step-1" => sender() ! "Step-2"
        case "Step-3" => sender() ! "Step-4"
        case _        => // no reply needed
      }
  }
}

// ── Multi-node configuration ─────────────────────────────────────────────────

object AeronClusterSpecConfig extends MultiNodeConfig {

  // Single Scala/JVM role.  The ActorSystem name becomes "AeronSystem" which
  // is also the system name embedded in seed-nodes and used by the Go binary.
  val node1: RoleName = role("AeronSystem")

  nodeConfig(node1)(ConfigFactory.parseString(
    """
    |akka {
    |  actor {
    |    provider = cluster
    |    # Re-enable Java serialisation so java.lang.String messages can be
    |    # exchanged between the Akka EchoActor and the Go node.  The default
    |    # akka-remote artery reference.conf sets this to off; override here for
    |    # the String-based Ping/Pong echo test.
    |    allow-java-serialization = on
    |  }
    |  remote.artery {
    |    transport          = aeron-udp
    |    canonical.hostname = "127.0.0.1"
    |    canonical.port     = 2561
    |  }
    |  cluster {
    |    seed-nodes = ["akka://AeronSystem@127.0.0.1:2561"]
    |    min-nr-of-members = 1
    |    downing-provider-class =
    |      "akka.cluster.sbr.SplitBrainResolverProvider"
    |    split-brain-resolver {
    |      active-strategy = keep-oldest
    |      stable-after    = 5s
    |    }
    |    configuration-compatibility-check.enforce-on-join = off
    |    failure-detector {
    |      acceptable-heartbeat-pause = 20s
    |      heartbeat-interval         = 1s
    |    }
    |  }
    |  loglevel = INFO
    |}
    """.stripMargin))
}

// ── Abstract spec ────────────────────────────────────────────────────────────

abstract class AeronSystem
    extends MultiNodeSpec(AeronClusterSpecConfig)
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender {

  import AeronClusterSpecConfig._

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit  = multiNodeSpecAfterAll()

  // ── Helpers ─────────────────────────────────────────────────────────────

  /** Locate the Go Aeron-UDP compat-test binary. */
  private def findGoBinary(): String = {
    val candidates = Seq(
      sys.env.getOrElse("GEKKA_AERON_COMPAT_TEST_BIN", ""),
      sys.env.getOrElse("GEKKA_COMPAT_TEST_BIN", ""),
      "../../../bin/gekka-aeron-compat-test",
      "../../bin/gekka-aeron-compat-test",
      "../../../bin/gekka-compat-test",
      "../../bin/gekka-compat-test",
      "gekka-aeron-compat-test",
      "gekka-compat-test",
    ).filter(_.nonEmpty)

    candidates.find { p =>
      val f = new java.io.File(p)
      f.canExecute || (!f.isAbsolute && Try(s"which $p".!!).isSuccess)
    }.getOrElse {
      fail(
        "gekka-aeron-compat-test binary not found. " +
          "Build with: go build -o bin/gekka-aeron-compat-test ./test/compat-bin/gekka-aeron-compat-test  " +
          "or set GEKKA_AERON_COMPAT_TEST_BIN."
      )
    }
  }

  // ── Test cases ───────────────────────────────────────────────────────────

  "AeronSystem node" must {

    "start, self-join, and become Up via Aeron UDP transport" in {
      runOn(node1) {
        val cluster = Cluster(system)
        cluster.join(cluster.selfAddress)

        val upProbe = TestProbe()
        cluster.subscribe(upProbe.ref, classOf[MemberUp])
        awaitAssert(
          cluster.state.members.exists(_.status == MemberStatus.Up) shouldBe true,
          20.seconds, 500.millis,
        )
        cluster.unsubscribe(upProbe.ref)

        println(s"AERON_SEED_UP: ${cluster.selfAddress}")
        Console.flush()
      }
      enterBarrier("aeron-seed-up")
    }

    "accept a Go/Gekka joiner via Aeron UDP and verify Ping/Pong over 60 s" in {
      runOn(node1) {
        val cluster = Cluster(system)

        val memberProbe = TestProbe()
        cluster.subscribe(memberProbe.ref,
          initialStateMode = InitialStateAsSnapshot,
          classOf[MemberUp])
        memberProbe.expectMsgType[CurrentClusterState](5.seconds)

        // Spawn the echo actor.
        val echoProbe = TestProbe()
        val echoActor = system.actorOf(Props(classOf[AeronEchoActor], echoProbe.ref), "aeron-echo")
        println(s"AeronEchoActor spawned at: ${echoActor.path}")
        Console.flush()
        enterBarrier("aeron-echo-ready")

        // ── Launch the Go binary ─────────────────────────────────────────
        val binary = findGoBinary()
        val goLogs = ListBuffer.empty[String]
        val failure = new java.util.concurrent.atomic.AtomicReference("")

        val logger = ProcessLogger(
          out => {
            goLogs += out
            println(s"[gekka/aeron] $out")
            Console.flush()
            if (out.startsWith("FAIL:")) failure.set(out)
          },
          err => {
            goLogs += err
            System.err.println(s"[gekka/aeron:err] $err")
            if (err.startsWith("FAIL:")) failure.set(err)
          },
        )

        val echoActorPath = s"akka://AeronSystem@127.0.0.1:2561/user/aeron-echo"
        println(s"Spawning Aeron Go binary: $binary")
        Console.flush()

        val proc = Process(Seq(
          binary,
          "--transport",   "aeron-udp",
          "--system",      "AeronSystem",
          "--seed-host",   "127.0.0.1",
          "--seed-port",   "2561",
          "--port",        "2563",
          "--mgmt-port",   "8563",
          "--echo-target", echoActorPath,
        )).run(logger)

        try {
          // ── Wait for Go node MemberUp ────────────────────────────────────
          val joinerUp = awaitAssert({
            if (failure.get().nonEmpty)
              fail(s"Go binary reported failure: ${failure.get()}")
            if (!proc.isAlive())
              fail(s"Go binary exited prematurely (code ${proc.exitValue()})")
            memberProbe.expectMsgType[MemberUp](2.seconds)
          }, 90.seconds, 2.seconds)

          val joinerAddr = joinerUp.member.address
          println(s"STATUS: AERON_MEMBER_UP: $joinerAddr")
          Console.flush()

          joinerAddr should not equal cluster.selfAddress
          joinerAddr.host shouldBe Some("127.0.0.1")
          joinerAddr.port shouldBe Some(2563)

          // ── Ping/Pong verification ───────────────────────────────────────
          // Go sends "Step-1" to Akka EchoActor → Akka replies "Step-2"
          // Go sends "Step-3" to Akka EchoActor → Akka replies "Step-4"
          echoProbe.expectMsg(30.seconds, "Step-1")
          println("STATUS: AERON_AKKA_RECEIVED_STEP_1")
          Console.flush()

          echoProbe.expectMsg(30.seconds, "Step-3")
          println("STATUS: AERON_AKKA_RECEIVED_STEP_3")
          Console.flush()

          awaitAssert({
            goLogs.exists(_.contains("STATUS: ECHO_STEP_2_RECEIVED")) shouldBe true
          }, 30.seconds, 500.millis)
          println("STATUS: AERON_GO_RECEIVED_STEP_2")

          awaitAssert({
            goLogs.exists(_.contains("STATUS: ECHO_STEP_4_RECEIVED")) shouldBe true
          }, 30.seconds, 500.millis)
          println("STATUS: AERON_GO_RECEIVED_STEP_4")
          Console.flush()

          enterBarrier("aeron-echo-done")

          // ── 60-second stability window ───────────────────────────────────
          val stabilityProbe = TestProbe()
          cluster.subscribe(stabilityProbe.ref, classOf[UnreachableMember])

          val stabilityMs = 60000L
          val stepMs      = 1000L
          val deadline    = System.currentTimeMillis() + stabilityMs
          val mgmtURL     = "http://127.0.0.1:8563/cluster/members"

          while (System.currentTimeMillis() < deadline) {
            if (failure.get().nonEmpty)
              fail(s"Go binary reported failure: ${failure.get()}")
            if (!proc.isAlive())
              fail(s"Go binary exited during stability window (code ${proc.exitValue()})")

            val remaining = deadline - System.currentTimeMillis()
            val waitMs    = math.min(stepMs, remaining).toInt

            stabilityProbe.receiveOne(waitMs.millis) match {
              case UnreachableMember(m) if m.address == joinerAddr =>
                fail(s"STABILITY_FAILED: Aeron Go node $joinerAddr became UNREACHABLE")
              case _ =>
            }

            val elapsed = stabilityMs - (deadline - System.currentTimeMillis())
            if (elapsed % 5000 < stepMs) {
              Try {
                val src = scala.io.Source.fromURL(mgmtURL)
                try src.mkString finally src.close()
              } match {
                case scala.util.Success(json) =>
                  if (!json.contains("Up"))
                    fail(s"MGMT_API_FAIL at ${elapsed}ms: no 'Up' in $json")
                  println(s"[aeron stability ${elapsed}ms] MGMT_OK: $json")
                  Console.flush()
                case scala.util.Failure(ex) =>
                  fail(s"MGMT_API_UNREACHABLE at ${elapsed}ms: $ex")
              }
            }
          }

          cluster.unsubscribe(stabilityProbe.ref)
          println("STABILITY_PASSED: Aeron Go node remained Up for 60 seconds")
          Console.flush()

          // Final management API check.
          val membersJson = Try {
            val src = scala.io.Source.fromURL(mgmtURL)
            try src.mkString finally src.close()
          }.getOrElse(fail(s"Could not reach $mgmtURL"))
          membersJson should include("Up")

          println("STATUS: AERON_COMPAT_TEST_PASSED")
          Console.flush()

        } finally {
          cluster.unsubscribe(memberProbe.ref)
          proc.destroy()
        }
      }

      enterBarrier("aeron-test-done")
    }
  }
}

// ── Concrete multi-JVM class ─────────────────────────────────────────────────
//
// sbt-multi-jvm discovers this class by the naming convention:
//   <SpecName>MultiJvmNode<N>   where SpecName = "AeronSystem"
//
// With one role ("AeronSystem"), exactly one JVM is launched.

class AeronSystemMultiJvmNode1 extends AeronSystem
