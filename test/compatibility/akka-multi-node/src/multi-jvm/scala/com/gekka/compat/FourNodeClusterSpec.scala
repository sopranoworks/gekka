/*
 * FourNodeClusterSpec.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * 4-node cluster test: 2 Akka (Scala) nodes + 2 Go (Gekka) nodes.
 * Verifies all 4 nodes discover each other, reach MemberUp, and remain
 * stable for 60 seconds.
 */
package com.gekka.compat

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try

import com.typesafe.config.ConfigFactory
import akka.actor.Address
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.{ImplicitSender, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

// ── MultiNodeConfig ──────────────────────────────────────────────────────────
//
// 2 JVM roles (Akka nodes). 2 Go nodes spawned as external processes by node1.
//   Akka node1: port 2551 (seed)
//   Akka node2: port 2553
//   Go node 1:  port 2552
//   Go node 2:  port 2554
//
object FourNodeClusterSpecConfig extends MultiNodeConfig {
  val akkaNode1: RoleName = role("akka-seed")
  val akkaNode2: RoleName = role("akka-node2")

  // System name = "FourNodeClusterSpec" (derived from spec class name by sbt-multi-jvm)
  private val commonConfig = ConfigFactory.parseString(
    """
    |akka {
    |  actor.provider = cluster
    |  remote.artery {
    |    transport = tcp
    |    canonical.hostname = "127.0.0.1"
    |  }
    |  cluster {
    |    seed-nodes = ["akka://FourNodeClusterSpec@127.0.0.1:2551"]
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
    """.stripMargin)

  nodeConfig(akkaNode1)(commonConfig.withFallback(
    ConfigFactory.parseString("akka.remote.artery.canonical.port = 2551")))

  nodeConfig(akkaNode2)(commonConfig.withFallback(
    ConfigFactory.parseString("akka.remote.artery.canonical.port = 2553")))
}

// ── Base MultiNodeSpec ────────────────────────────────────────────────────────

abstract class FourNodeClusterSpec
    extends MultiNodeSpec(FourNodeClusterSpecConfig)
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender {

  import FourNodeClusterSpecConfig._

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit  = multiNodeSpecAfterAll()

  /** Locate the gekka-compat-test binary. */
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
        "gekka-compat-test binary not found. " +
          "Build it with:  go build -o bin/gekka-compat-test ./test/compat-bin/gekka-compat-test  " +
          "or set GEKKA_COMPAT_TEST_BIN env var."
      )
    }
  }

  /** Spawn a Go node as an external process. */
  private def spawnGoNode(
    binary: String,
    localPort: Int,
    mgmtPort: Int,
    label: String
  ): (Process, ListBuffer[String], java.util.concurrent.atomic.AtomicReference[String]) = {
    val logs    = ListBuffer.empty[String]
    val failure = new java.util.concurrent.atomic.AtomicReference[String]("")

    val logger = ProcessLogger(
      out => {
        logs += out
        println(s"[$label] $out")
        if (out.startsWith("FAIL:")) failure.set(out)
        Console.flush()
      },
      err => {
        logs += err
        System.err.println(s"[$label:err] $err")
        if (err.startsWith("FAIL:")) failure.set(err)
      },
    )

    val proc = scala.sys.process.Process(Seq(
      binary,
      "--system",    "FourNodeClusterSpec",
      "--seed-host", "127.0.0.1",
      "--seed-port", "2551",
      "--port",      localPort.toString,
      "--mgmt-port", mgmtPort.toString,
      "--timeout",   "120s",
    )).run(logger)

    (proc, logs, failure)
  }

  // ── Test ──────────────────────────────────────────────────────────────────

  "A 4-node cluster (2 Akka + 2 Go)" must {

    "form a cluster where all nodes see each other as Up for 60 seconds" in {
      val cluster = Cluster(system)

      // ── Phase 1: Both Akka nodes join ──────────────────────────────────────
      runOn(akkaNode1) {
        cluster.join(cluster.selfAddress)
      }
      enterBarrier("seed-started")

      runOn(akkaNode2) {
        cluster.join(Address("akka", "FourNodeClusterSpec", "127.0.0.1", 2551))
      }
      enterBarrier("akka-nodes-joining")

      // Wait for both Akka nodes to be Up
      awaitAssert({
        val upMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
        upMembers.size shouldBe 2
      }, 30.seconds, 1.second)

      println(s"[${myself.name}] Both Akka nodes are Up")
      Console.flush()
      enterBarrier("akka-nodes-up")

      // ── Phase 2: node1 spawns both Go nodes ───────────────────────────────
      // Only node1 spawns Go binaries to avoid cross-JVM process coordination issues.
      var goProcs: List[Process] = Nil

      runOn(akkaNode1) {
        val binary = findGoBinary

        val (proc1, logs1, fail1) = spawnGoNode(binary, 2552, 8559, "gekka-go1")
        val (proc2, logs2, fail2) = spawnGoNode(binary, 2554, 8560, "gekka-go2")
        goProcs = List(proc1, proc2)

        // Wait for both Go nodes to report ARTERY_ASSOCIATED
        awaitAssert({
          if (fail1.get().nonEmpty) fail(s"Go node 1 failed: ${fail1.get()}")
          if (fail2.get().nonEmpty) fail(s"Go node 2 failed: ${fail2.get()}")
          if (!proc1.isAlive()) fail(s"Go node 1 exited with code ${proc1.exitValue()}")
          if (!proc2.isAlive()) fail(s"Go node 2 exited with code ${proc2.exitValue()}")

          val go1Associated = logs1.exists(_.contains("STATUS: ARTERY_ASSOCIATED"))
          val go2Associated = logs2.exists(_.contains("STATUS: ARTERY_ASSOCIATED"))
          println(s"[akka-seed] Go1 associated=$go1Associated, Go2 associated=$go2Associated")
          Console.flush()
          go1Associated shouldBe true
          go2Associated shouldBe true
        }, 90.seconds, 2.seconds)

        println("[akka-seed] Both Go nodes have completed Artery handshake")
        Console.flush()
      }

      enterBarrier("go-nodes-spawned")

      // ── Phase 3: All nodes verify 4 members Up ────────────────────────────
      awaitAssert({
        val upMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
        println(s"[${myself.name}] Up members: ${upMembers.size} — ${upMembers.map(_.address)}")
        Console.flush()
        upMembers.size shouldBe 4
      }, 90.seconds, 2.seconds)

      val allMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
      println(s"[${myself.name}] ALL 4 MEMBERS UP: ${allMembers.map(_.address).mkString(", ")}")
      Console.flush()

      // Verify expected ports
      val ports = allMembers.map(_.address.port.getOrElse(0)).toSet
      ports should contain allOf (2551, 2552, 2553, 2554)

      enterBarrier("all-four-up")

      // ── Phase 4: 60-second stability monitoring ────────────────────────────
      val stabilityProbe = TestProbe()
      cluster.subscribe(stabilityProbe.ref, classOf[UnreachableMember])

      val stabilityWindowMs = 60000L
      val stepMs            = 2000L
      val deadline          = System.currentTimeMillis() + stabilityWindowMs

      println(s"[${myself.name}] Starting 60-second stability monitoring...")
      Console.flush()

      while (System.currentTimeMillis() < deadline) {
        val remaining = deadline - System.currentTimeMillis()
        val waitMs    = math.min(stepMs, math.max(remaining, 0)).toInt

        stabilityProbe.receiveOne(waitMs.millis) match {
          case UnreachableMember(m) =>
            fail(s"STABILITY_FAILED: ${m.address} became UNREACHABLE after ${stabilityWindowMs - remaining}ms")
          case _ => // timeout or other — OK
        }

        // Periodic status check every 10 seconds
        val elapsed = stabilityWindowMs - (deadline - System.currentTimeMillis())
        if (elapsed > 0 && elapsed % 10000 < stepMs) {
          val upCount = cluster.state.members.count(_.status == MemberStatus.Up)
          if (upCount < 4) {
            fail(s"STABILITY_FAILED at ${elapsed}ms: only $upCount members Up (expected 4)")
          }
          println(s"[${myself.name}] [stability ${elapsed}ms] OK: $upCount members Up")
          Console.flush()
        }
      }

      cluster.unsubscribe(stabilityProbe.ref)

      // Final check
      val finalUp = cluster.state.members.count(_.status == MemberStatus.Up)
      finalUp shouldBe 4

      println(s"[${myself.name}] STABILITY_PASSED: All 4 nodes remained Up for 60 seconds")
      Console.flush()

      enterBarrier("stability-done")

      // Cleanup Go processes
      runOn(akkaNode1) {
        goProcs.foreach(_.destroy())
      }

      enterBarrier("test-done")
    }
  }
}

// ── Concrete multi-JVM node classes ──────────────────────────────────────────
class FourNodeClusterSpecMultiJvmNode1 extends FourNodeClusterSpec
class FourNodeClusterSpecMultiJvmNode2 extends FourNodeClusterSpec
