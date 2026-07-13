package com.gekka.showcase

import java.util.concurrent.atomic.AtomicLong

import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.pekko.cluster.{Cluster, MemberStatus}

import scala.concurrent.duration._

/** Steady-state anchor for the showcase traffic actors.
  *
  * Spec §4 scopes the strict Gate-2 log window from Gate 1 PASS: WARN/ERROR
  * during setup are "written to per-node artifact logs but not counted".
  * Spec §5.4.2 anchors `SingletonWarmupGrace` at the same instant ("measured
  * from Gate 1 PASS"). A child process cannot observe the runner's Gate-1
  * poll; the per-node equivalent is the FIRST time the local cluster state
  * reports the full expected membership Up — the same condition Gate 1
  * polls, observed locally.
  *
  * Traffic itself is NOT gated on the anchor (spec revision-2 note: "DO NOT
  * delay or constrain FT1/2/3") — only ERROR accounting is: a timeout whose
  * SEND predates the anchor belongs to the setup phase and is not counted.
  */
object SteadyAnchor {
  private val atMillis = new AtomicLong(0L)

  /** Latch the anchor at now; only the first call wins. */
  def mark(): Unit = atMillis.compareAndSet(0L, System.currentTimeMillis())

  def isLatched: Boolean = atMillis.get() != 0L

  /** Anchor instant in epoch millis; 0 while unlatched. */
  def at: Long = atMillis.get()

  /** Whether a request sent at `sentAtMillis` belongs to the strict Gate-2
    * accounting window (anchor latched and the send did not precede it).
    */
  def countsForStrictWindow(sentAtMillis: Long): Boolean = {
    val a = atMillis.get()
    a != 0L && sentAtMillis >= a
  }

  /** Poll the local cluster state until `expected` members are Up, then
    * latch the anchor. Idempotent per JVM (the poll cancels itself once
    * latched).
    */
  def startWatch(system: ActorSystem, expected: Int): Unit = {
    import system.dispatcher
    val cluster = Cluster(system)
    var handle: Cancellable = null
    handle = system.scheduler.scheduleWithFixedDelay(200.millis, 200.millis) { () =>
      if (isLatched) {
        Option(handle).foreach(_.cancel())
      } else {
        val up = cluster.state.members.count(_.status == MemberStatus.Up)
        if (up >= expected) {
          mark()
          system.log.info(s"showcase: steady-state anchor latched (membersUp=$expected)")
          Option(handle).foreach(_.cancel())
        }
      }
    }
  }

  /** Test hook: reset the latch (never used by the showcase runtime). */
  private[showcase] def resetForTest(): Unit = atMillis.set(0L)
}
