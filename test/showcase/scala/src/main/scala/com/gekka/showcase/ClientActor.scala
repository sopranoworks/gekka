package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, Cancellable, DeadLetter, Props}
import org.apache.pekko.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

object ClientActor {
  // Spec §5.4.2
  val WarmupGrace: FiniteDuration = 30.seconds
  // Per-Ping timeout, spec §5.4
  val PingTimeoutMs: Long = 5000L
  val IntervalMs: Long    = 4000L

  // All roles in the cluster.
  val AllRoles: List[String] = List(
    "singleton-s1", "singleton-s2", "singleton-s3", "singleton-s4", "singleton-s5",
    "singleton-g1", "singleton-g2", "singleton-g3",
  )

  def props(selfLabel: String): Props = Props(new ClientActor(selfLabel))

  case object Tick
  case object WarmupEnded
  case object TimeoutSweep

  final case class Pending(seqNo: Long, role: String, sentAt: Long)
}

class ClientActor(selfLabel: String) extends Actor with ActorLogging {
  import ClientActor._
  import context.dispatcher

  private val proxies: Map[String, ActorRef] = AllRoles.map { role =>
    val settings = ClusterSingletonProxySettings(context.system).withRole(role)
    val proxy = context.actorOf(
      ClusterSingletonProxy.props(s"/user/singleton-manager-$role", settings),
      s"proxy-$role")
    role -> proxy
  }.toMap

  private var nextSeq: Long = 0L
  private val pending = mutable.Map.empty[Long, Pending]
  private val established = mutable.Set.empty[String]
  private val warmupMisses = mutable.Map.empty[String, Long].withDefaultValue(0L)
  private var warmupActive: Boolean = true

  private var tick: Cancellable = _
  private var sweep: Cancellable = _
  private var warmupEnder: Cancellable = _

  override def preStart(): Unit = {
    tick = context.system.scheduler.scheduleWithFixedDelay(1.second, IntervalMs.millis, self, Tick)
    sweep = context.system.scheduler.scheduleWithFixedDelay(2.seconds, 1.second, self, TimeoutSweep)
    warmupEnder = context.system.scheduler.scheduleOnce(WarmupGrace, self, WarmupEnded)
    context.system.eventStream.subscribe(self, classOf[DeadLetter])
  }

  override def postStop(): Unit = {
    Option(tick).foreach(_.cancel())
    Option(sweep).foreach(_.cancel())
    Option(warmupEnder).foreach(_.cancel())
    context.system.eventStream.unsubscribe(self)
  }

  override def receive: Receive = {
    case Tick =>
      proxies.foreach { case (role, proxy) =>
        val seq = nextSeq; nextSeq += 1
        pending(seq) = Pending(seq, role, System.currentTimeMillis())
        proxy ! Ping(seq, selfLabel)
      }

    case Pong(seq, _) =>
      pending.remove(seq).foreach { p =>
        if (!established(p.role)) established += p.role
      }

    case TimeoutSweep =>
      val now = System.currentTimeMillis()
      val expired = pending.values.filter(p => now - p.sentAt > PingTimeoutMs).toList
      expired.foreach { p =>
        pending.remove(p.seqNo)
        if (established(p.role)) {
          // §5.4.1 "failed" path
          log.error(s"SingletonClient: ping to ${p.role} timed out for seq=${p.seqNo}")
        } else if (warmupActive) {
          // §5.4.2: unresolved during grace — count, do NOT log ERROR
          warmupMisses(p.role) = warmupMisses(p.role) + 1L
        } else {
          // After warmup ended; unresolved is now also an ERROR
          log.error(s"SingletonClient: ping to ${p.role} timed out for seq=${p.seqNo}")
        }
      }

    case WarmupEnded =>
      warmupActive = false
      AllRoles.filterNot(established.contains).foreach { role =>
        log.error(s"SingletonClient: role $role never established within warmup-grace=30s")
      }

    case DeadLetter(msg, _, recipient) =>
      // §5.4.4: only relevant when buffer-size=0; with default buffering this is unreachable
      // for unestablished singletons because the proxy buffers Pings.
      // We still subscribe (cheap) so the spec contract is honoured if someone flips
      // pekko.cluster.singleton-proxy.buffer-size = 0 at deploy time.
      msg match {
        case Ping(seq, _) =>
          pending.remove(seq).foreach { p =>
            if (warmupActive && !established(p.role)) {
              warmupMisses(p.role) = warmupMisses(p.role) + 1L
            } else if (established(p.role)) {
              log.error(s"SingletonClient: dead-letter Ping to ${p.role} seq=${p.seqNo}")
            }
          }
        case _ => () // unrelated dead letters
      }

    case other =>
      log.error(s"ClientActor: unexpected message: ${other.getClass.getName}")
  }
}
