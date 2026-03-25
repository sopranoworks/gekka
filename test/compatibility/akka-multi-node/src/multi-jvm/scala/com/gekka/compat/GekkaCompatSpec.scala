/*
 * GekkaCompatSpec.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 */
package com.gekka.compat

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, Address, Props}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

// ── Echo Actor for Multi-Hop Test ───────────────────────────────────────────
class EchoActor(probe: akka.actor.ActorRef) extends Actor {
  var count = 0
  def receive: Receive = {
    case msg: String =>
      count += 1
      println(s"[akka] EchoActor received: $msg (count=$count)")
      probe ! msg
      if (msg == "Step-1") {
        sender() ! "Step-2"
      } else if (msg == "Step-3") {
        sender() ! "Step-4"
      }
  }
}

// ── MultiNodeConfig ──────────────────────────────────────────────────────────
//
// Defines one JVM role: GekkaSystem (the Akka seed).
// The ActorSystem is named after the role — "GekkaSystem" — so the Go joiner
// can construct the correct seed-node address: akka://GekkaSystem@127.0.0.1:2551.
//
object GekkaCompatSpecConfig extends MultiNodeConfig {
  // Role name == ActorSystem name (Akka MultiNodeSpec uses the role name as the system name).
  val node1: RoleName = role("GekkaSystem")

  nodeConfig(node1)(ConfigFactory.parseString(
    """
    |akka {
    |  actor.provider = cluster
    |  remote.artery {
    |    transport = tcp
    |    canonical.hostname = "127.0.0.1"
    |    canonical.port    = 2551
    |  }
    |  cluster {
    |    seed-nodes = ["akka://GekkaSystem@127.0.0.1:2551"]
    |    min-nr-of-members = 1
    |    downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
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

// ── Base MultiNodeSpec ────────────────────────────────────────────────────────

abstract class GekkaSystem
    extends MultiNodeSpec(GekkaCompatSpecConfig)
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender {

  import GekkaCompatSpecConfig._

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit  = multiNodeSpecAfterAll()

  // ── Helpers ───────────────────────────────────────────────────────────────

  /** Locate the gekka-compat-test binary.
   *
   *  Checks, in order:
   *    1. GEKKA_COMPAT_TEST_BIN env variable (absolute path or relative to cwd)
   *    2. ../../bin/gekka-compat-test  (built by: go build -o bin/gekka-compat-test ./cmd/gekka-compat-test)
   *    3. gekka-compat-test on $PATH
   */
  private def findGoBinary: String = {
    val candidates = Seq(
      sys.env.getOrElse("GEKKA_COMPAT_TEST_BIN", ""),
      "../../../bin/gekka-compat-test",
      "../../bin/gekka-compat-test",
      "gekka-compat-test",
    ).filter(_.nonEmpty)

    candidates.find { p =>
      val f = new java.io.File(p)
      f.canExecute || (!f.isAbsolute && Try(s"which $p".!!).isSuccess)
    }.getOrElse {
      fail(
        s"gekka-compat-test binary not found. " +
          s"Build it with:  go build -o bin/gekka-compat-test ./cmd/gekka-compat-test  " +
          s"or set GEKKA_COMPAT_TEST_BIN env var."
      )
    }
  }

  // ── Test ──────────────────────────────────────────────────────────────────

  "The Akka seed node (Node A)" must {

    "start and join its own seed-node list" in {
      runOn(node1) {
        val cluster = Cluster(system)
        cluster.join(cluster.selfAddress)

        // Wait until the seed node itself is Up.
        val probe = TestProbe()
        cluster.subscribe(probe.ref, classOf[MemberUp])
        awaitAssert {
          cluster.state.members.exists(_.status == MemberStatus.Up) shouldBe true
        }
        cluster.unsubscribe(probe.ref)

        info(s"AKKA_SEED_UP: ${cluster.selfAddress}")
        println(s"AKKA_SEED_UP: ${cluster.selfAddress}")
        Console.flush()
      }
      enterBarrier("seed-up")
    }

    "accept a Go/Gekka joiner via Artery TCP and observe MemberUp" in {
      runOn(node1) {
        val cluster = Cluster(system)

        // Subscribe before spawning the Go node so we don't miss the event.
        val memberProbe = TestProbe()
        cluster.subscribe(memberProbe.ref, initialStateMode = InitialStateAsSnapshot,
          classOf[MemberUp])
        // Drain the CurrentClusterState snapshot (contains seed itself).
        memberProbe.expectMsgType[CurrentClusterState](5.seconds)

        // ── Echo Test Setup ───────────────────────────────────────────────
        val echoProbe = TestProbe()
        val echoActor = system.actorOf(Props(classOf[EchoActor], echoProbe.ref), "echo")
        enterBarrier("echo-ready")

        // ── Spawn Go binary ───────────────────────────────────────────────
        val binary  = findGoBinary
        val goLogs  = ListBuffer.empty[String]
        val failure = new java.util.concurrent.atomic.AtomicReference[String]("")

        val logger  = ProcessLogger(
          out => {
            goLogs += out
            println(s"[gekka] $out")
            if (out.startsWith("FAIL:")) {
              failure.set(out)
            }
            Console.flush()
          },
          err => {
            goLogs += err
            System.err.println(s"[gekka:err] $err")
            if (err.startsWith("FAIL:")) {
              failure.set(err)
            }
          },
        )

        info(s"Spawning Go joiner: $binary")
        val proc = Process(Seq(
          binary,
          "--system",    "GekkaSystem",
          "--seed-host", "127.0.0.1",
          "--seed-port", "2551",
          "--port",      "2552",
          "--mgmt-port", "8558",
          "--echo-target", s"akka://GekkaSystem@127.0.0.1:2551/user/echo"
        )).run(logger)

        try {
          // ── Wait for Go node MemberUp (up to 60 s) ───────────────────────
          val joinerUp = awaitAssert({
            if (failure.get().nonEmpty) fail(s"Go binary reported failure: ${failure.get()}")
            if (!proc.isAlive()) fail(s"Go binary exited prematurely with code ${proc.exitValue()}")
            memberProbe.expectMsgType[MemberUp](1.second)
          }, 60.seconds, 1.second)

          val joinerAddr = joinerUp.member.address

          info(s"STATUS: MEMBER_UP: $joinerAddr")
          println(s"STATUS: MEMBER_UP: $joinerAddr")
          Console.flush()

          // ── Identity assertions ───────────────────────────────────────────
          // The joining node must not be the seed itself.
          joinerAddr should not equal cluster.selfAddress
          // Must have joined on the expected host and port.
          joinerAddr.host shouldBe Some("127.0.0.1")
          joinerAddr.port shouldBe Some(2552)
          // Protocol must be akka (Artery TCP).
          joinerAddr.protocol shouldBe "akka"
          // System name must match what the Go binary was started with.
          joinerAddr.system shouldBe "GekkaSystem"

          println(s"IDENTITY_OK: $joinerAddr")
          Console.flush()

          // ── Echo Test Verification ────────────────────────────────────────
          echoProbe.expectMsg(30.seconds, "Step-1")
          println("STATUS: AKKA_RECEIVED_STEP_1")

          echoProbe.expectMsg(30.seconds, "Step-3")
          println("STATUS: AKKA_RECEIVED_STEP_3")

          // Wait for Go node to report Step-2 received
          awaitAssert({
            if (goLogs.exists(_.contains("STATUS: ECHO_STEP_2_RECEIVED"))) {
               println("STATUS: GO_RECEIVED_STEP_2")
            } else {
               fail("Go node has not received Step-2 yet")
            }
          }, 30.seconds, 1.second)

          // Wait for Go node to report Step-4 received
          awaitAssert({
            if (goLogs.exists(_.contains("STATUS: ECHO_STEP_4_RECEIVED"))) {
               println("STATUS: GO_RECEIVED_STEP_4")
            } else {
               fail("Go node has not received Step-4 yet")
            }
          }, 30.seconds, 1.second)

          enterBarrier("echo-done")

          // ── 60-second stability phase ─────────────────────────────────────
          // Monitor for UnreachableMember events targeting the Go node.
          // Fail immediately if Go becomes unreachable — that indicates the
          // heartbeat or failure-detector integration is broken.
          val stabilityProbe = TestProbe()
          cluster.subscribe(stabilityProbe.ref, classOf[UnreachableMember])

          val stabilityWindowMs = 60000L
          val stepMs            = 1000L
          val mgmtURL           = "http://127.0.0.1:8558/cluster/members"
          val deadline          = System.currentTimeMillis() + stabilityWindowMs

          while (System.currentTimeMillis() < deadline) {
            // Fail-fast checks
            if (failure.get().nonEmpty) fail(s"Go binary reported failure during stability: ${failure.get()}")
            if (!proc.isAlive()) fail(s"Go binary exited during stability with code ${proc.exitValue()}")

            val remaining = deadline - System.currentTimeMillis()
            val waitMs    = math.min(stepMs, remaining).toInt

            // Check for UnreachableMember events (non-blocking after timeout).
            stabilityProbe.receiveOne(waitMs.millis) match {
              case UnreachableMember(m) if m.address == joinerAddr =>
                fail(s"STABILITY_FAILED: Go node $joinerAddr became UNREACHABLE after ${
                  stabilityWindowMs - remaining}ms")
              case _ => // other event or timeout — continue
            }

            // ── Periodic management API cross-check ───────────────────────
            val elapsed = stabilityWindowMs - (deadline - System.currentTimeMillis())
            // Only check every 5 seconds to reduce noise
            if (elapsed % 5000 < stepMs) {
              Try {
                val src = scala.io.Source.fromURL(mgmtURL)
                try src.mkString finally src.close()
              } match {
                case scala.util.Success(json) =>
                  if (!json.contains("Up")) {
                    fail(s"MGMT_API_FAIL at ${elapsed}ms: no 'Up' status in $json")
                  }
                  println(s"[stability ${elapsed}ms] MGMT_OK members=$json")
                  Console.flush()
                case scala.util.Failure(ex) =>
                  fail(s"MGMT_API_UNREACHABLE at ${elapsed}ms: $ex")
              }
            }
          }

          cluster.unsubscribe(stabilityProbe.ref)
          println("STABILITY_PASSED: Go node remained Up for 60 seconds")
          Console.flush()

          // ── Final management API snapshot ─────────────────────────────────
          val membersJson = Try {
            val src = scala.io.Source.fromURL(mgmtURL)
            try src.mkString finally src.close()
          }.getOrElse(fail(s"Could not reach management API at $mgmtURL after stability window"))

          info(s"CLUSTER_MEMBERS: $membersJson")
          println(s"CLUSTER_MEMBERS: $membersJson")
          Console.flush()

          membersJson should include("Up")

          println("STATUS: COMPAT_TEST_PASSED")
          Console.flush()

        } finally {
          cluster.unsubscribe(memberProbe.ref)
          proc.destroy()
        }
      }

      enterBarrier("test-done")
    }
  }
}

// ── Concrete multi-JVM node class ─────────────────────────────────────────────
//
// sbt-multi-jvm discovers this class by the naming convention:
//   <SpecName>MultiJvmNode<N>   (N is 1-indexed, one class per role)
//
// Since GekkaCompatSpecConfig defines exactly one JVM role (node1), we only
// need Node1.
//
class GekkaSystemMultiJvmNode1 extends GekkaSystem
