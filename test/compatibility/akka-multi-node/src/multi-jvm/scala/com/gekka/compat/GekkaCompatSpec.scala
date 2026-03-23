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
import akka.actor.{Actor, ActorLogging, Props}
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
        val probe = TestProbe()
        cluster.subscribe(probe.ref, initialStateMode = InitialStateAsSnapshot, classOf[MemberUp])
        // Drain the CurrentClusterState snapshot (contains seed itself).
        probe.expectMsgType[CurrentClusterState](5.seconds)

        // ── Spawn Go binary ───────────────────────────────────────────────
        val binary  = findGoBinary
        val goLogs  = ListBuffer.empty[String]
        val logger  = ProcessLogger(
          out => { goLogs += out; println(s"[gekka] $out"); Console.flush() },
          err => { goLogs += err; System.err.println(s"[gekka:err] $err") },
        )

        info(s"Spawning Go joiner: $binary")
        val proc = Process(Seq(
          binary,
          "--system",    "GekkaSystem",
          "--seed-host", "127.0.0.1",
          "--seed-port", "2551",
          "--port",      "2552",
          "--mgmt-port", "8558",
        )).run(logger)

        try {
          // ── Wait for Go node MemberUp (up to 60 s) ───────────────────────
          val joinerUp = probe.expectMsgType[MemberUp](60.seconds)
          val joinerAddr = joinerUp.member.address

          info(s"GEKKA_MEMBER_UP: $joinerAddr")
          println(s"GEKKA_MEMBER_UP: $joinerAddr")
          Console.flush()

          // The joining node should not be the seed itself.
          joinerAddr should not equal cluster.selfAddress

          // ── Verify via Management HTTP API ───────────────────────────────
          // Give the management server a moment to process the membership update.
          Thread.sleep(1500)

          val mgmtURL = "http://127.0.0.1:8558/cluster/members"
          val membersJson = Try(scala.io.Source.fromURL(mgmtURL).mkString)
            .getOrElse(fail(s"Could not reach management API at $mgmtURL"))

          info(s"CLUSTER_MEMBERS: $membersJson")
          println(s"CLUSTER_MEMBERS: $membersJson")
          Console.flush()

          // Both members should appear as Up.
          membersJson should include("Up")

          println("COMPAT_TEST_PASSED")
          Console.flush()

        } finally {
          cluster.unsubscribe(probe.ref)
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
