/*
 * SevenNodeClusterSpec.scala
 * This file is part of the gekka project.
 *
 * Copyright (c) 2026 Sopranoworks, Osamu Takahashi
 * SPDX-License-Identifier: MIT
 *
 * 7-node cluster test: 2 Akka (Scala) nodes + 5 Go (Gekka) nodes.
 * Pins the Joining-stuck convergence regression observed in the live
 * diag against a 5-peer Pekko cluster. With the upstream bugs
 * (a973dea / f69bed6 / c289a78 / cb82ddf) all closed, every Go joiner
 * must transition Joining → Up within 60 seconds and the cluster must
 * remain stable for 60 seconds afterwards.
 *
 * Differs from FourNodeClusterSpec only in fanout: same 2 Akka JVMs,
 * but five Go joiners (ports 2552, 2554, 2556, 2558, 2560) so the
 * convergence-dependent leader-action path is exercised under higher
 * pressure.
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
// 2 JVM roles (Akka nodes). 5 Go nodes spawned as external processes by node1.
//   Akka node1: port 2551 (seed)
//   Akka node2: port 2553
//   Go nodes:   ports 2552, 2554, 2556, 2558, 2560
//
object SevenNodeClusterSpecConfig extends MultiNodeConfig {
  val akkaNode1: RoleName = role("akka-seed")
  val akkaNode2: RoleName = role("akka-node2")

  private val commonConfig = ConfigFactory.parseString(
    """
    |akka {
    |  actor.provider = cluster
    |  remote.artery {
    |    transport = tcp
    |    canonical.hostname = "127.0.0.1"
    |  }
    |  cluster {
    |    seed-nodes = ["akka://SevenNodeClusterSpec@127.0.0.1:2551"]
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

abstract class SevenNodeClusterSpec
    extends MultiNodeSpec(SevenNodeClusterSpecConfig)
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender {

  import SevenNodeClusterSpecConfig._

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()
  override def afterAll(): Unit  = multiNodeSpecAfterAll()

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
      "--system",    "SevenNodeClusterSpec",
      "--seed-host", "127.0.0.1",
      "--seed-port", "2551",
      "--port",      localPort.toString,
      "--mgmt-port", mgmtPort.toString,
      "--timeout",   "120s",
    )).run(logger)

    (proc, logs, failure)
  }

  "A 7-node cluster (2 Akka + 5 Go)" must {

    "form a cluster where all nodes see each other as Up for 60 seconds" in {
      val cluster = Cluster(system)

      runOn(akkaNode1) {
        cluster.join(cluster.selfAddress)
      }
      enterBarrier("seed-started")

      runOn(akkaNode2) {
        cluster.join(Address("akka", "SevenNodeClusterSpec", "127.0.0.1", 2551))
      }
      enterBarrier("akka-nodes-joining")

      awaitAssert({
        val upMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
        upMembers.size shouldBe 2
      }, 30.seconds, 1.second)

      println(s"[${myself.name}] Both Akka nodes are Up")
      Console.flush()
      enterBarrier("akka-nodes-up")

      // node1 spawns all 5 Go nodes
      var goProcs: List[Process] = Nil

      runOn(akkaNode1) {
        val binary = findGoBinary

        // (localPort, mgmtPort, label) for each Go joiner.
        val goSpecs: Seq[(Int, Int, String)] = Seq(
          (2552, 8559, "gekka-go1"),
          (2554, 8560, "gekka-go2"),
          (2556, 8561, "gekka-go3"),
          (2558, 8562, "gekka-go4"),
          (2560, 8563, "gekka-go5"),
        )

        val spawned = goSpecs.map { case (p, m, lbl) => spawnGoNode(binary, p, m, lbl) }
        goProcs = spawned.map(_._1).toList

        // Wait for every Go node to report ARTERY_ASSOCIATED.
        awaitAssert({
          spawned.zipWithIndex.foreach { case ((proc, logs, fail), idx) =>
            val label = goSpecs(idx)._3
            if (fail.get().nonEmpty) sys.error(s"$label failed: ${fail.get()}")
            if (!proc.isAlive()) sys.error(s"$label exited with code ${proc.exitValue()}")
            val associated = logs.exists(_.contains("STATUS: ARTERY_ASSOCIATED"))
            if (!associated) sys.error(s"$label not yet ARTERY_ASSOCIATED")
          }
          println(s"[akka-seed] All 5 Go nodes have ARTERY_ASSOCIATED")
          Console.flush()
        }, 90.seconds, 2.seconds)
      }

      enterBarrier("go-nodes-spawned")

      // All nodes verify 7 members Up. Joining-stuck convergence reveals
      // itself here: gekka holds Joining indefinitely without ever being
      // promoted by the leader.
      awaitAssert({
        val upMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
        val joining = cluster.state.members.filter(_.status == MemberStatus.Joining)
        println(s"[${myself.name}] Up=${upMembers.size} Joining=${joining.size} — Up addrs: ${upMembers.map(_.address)}")
        Console.flush()
        upMembers.size shouldBe 7
      }, 90.seconds, 2.seconds)

      val allMembers = cluster.state.members.filter(_.status == MemberStatus.Up)
      println(s"[${myself.name}] ALL 7 MEMBERS UP: ${allMembers.map(_.address).mkString(", ")}")
      Console.flush()

      val ports = allMembers.map(_.address.port.getOrElse(0)).toSet
      ports should contain allOf (2551, 2552, 2553, 2554, 2556, 2558, 2560)

      enterBarrier("all-seven-up")

      // 60-second stability monitoring
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
          case _ => // OK
        }

        val elapsed = stabilityWindowMs - (deadline - System.currentTimeMillis())
        if (elapsed > 0 && elapsed % 10000 < stepMs) {
          val upCount = cluster.state.members.count(_.status == MemberStatus.Up)
          if (upCount < 7) {
            fail(s"STABILITY_FAILED at ${elapsed}ms: only $upCount members Up (expected 7)")
          }
          println(s"[${myself.name}] [stability ${elapsed}ms] OK: $upCount members Up")
          Console.flush()
        }
      }

      cluster.unsubscribe(stabilityProbe.ref)

      val finalUp = cluster.state.members.count(_.status == MemberStatus.Up)
      finalUp shouldBe 7

      println(s"[${myself.name}] STABILITY_PASSED: All 7 nodes remained Up for 60 seconds")
      Console.flush()

      enterBarrier("stability-done")

      runOn(akkaNode1) {
        goProcs.foreach(_.destroy())
      }

      enterBarrier("test-done")
    }
  }
}

// ── Concrete multi-JVM node classes ──────────────────────────────────────────
class SevenNodeClusterSpecMultiJvmNode1 extends SevenNodeClusterSpec
class SevenNodeClusterSpecMultiJvmNode2 extends SevenNodeClusterSpec
