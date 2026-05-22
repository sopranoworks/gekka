package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Cancellable, Props}

import scala.collection.mutable
import scala.concurrent.duration._

object TellSenderActor {
  def props(selfLabel: String, peers: List[String]): Props =
    Props(new TellSenderActor(selfLabel, peers))

  val name: String = "echo-sender"

  case object Tick
  final case class Pending(seqNo: Long, peer: String, payloadKind: String, sentAt: Long)
  case object TimeoutSweep
  val EchoTimeoutMs: Long = 10000L
  val IntervalMs: Long    = 4000L
}

class TellSenderActor(selfLabel: String, peers: List[String]) extends Actor with ActorLogging {
  import TellSenderActor._
  import context.dispatcher

  private var nextSeq: Long = 0L
  private val pending = mutable.Map.empty[Long, Pending]
  private val payloadKinds = Vector("string", "long", "system", "custom")
  private var kindIdx = 0

  private var tickHandle: Cancellable = _
  private var sweepHandle: Cancellable = _

  override def preStart(): Unit = {
    tickHandle = context.system.scheduler.scheduleWithFixedDelay(
      1.second, IntervalMs.millis, self, Tick)
    sweepHandle = context.system.scheduler.scheduleWithFixedDelay(
      2.seconds, 1.second, self, TimeoutSweep)
  }

  override def postStop(): Unit = {
    Option(tickHandle).foreach(_.cancel())
    Option(sweepHandle).foreach(_.cancel())
  }

  override def receive: Receive = {
    case Tick =>
      peers.foreach { peer =>
        val seq = nextSeq; nextSeq += 1
        val kind = payloadKinds(kindIdx % payloadKinds.size); kindIdx += 1
        val payload: AnyRef = kind match {
          case "string" => s"hello-$seq-$selfLabel"
          case "long"   => java.lang.Long.valueOf(seq)
          case "system" => SystemMessagePing(seq, selfLabel)
          case "custom" => ShowcaseEchoCustom(seq, selfLabel, Vector.fill(16)(0x42.toByte))
        }
        val env = EchoEnvelope(seq, selfLabel, "SEND", kind, payload)
        pending(seq) = Pending(seq, peer, kind, System.currentTimeMillis())
        context.actorSelection(peer + "/user/echo") ! env
      }

    case env: EchoEnvelope if env.direction == "REPLY" =>
      pending.remove(env.seqNo)

    case TimeoutSweep =>
      val now = System.currentTimeMillis()
      val expired = pending.values.filter(p => now - p.sentAt > EchoTimeoutMs).toList
      expired.foreach { p =>
        log.error(s"EchoSender: no reply from ${p.peer} for seq=${p.seqNo} type=${p.payloadKind}")
        pending.remove(p.seqNo)
      }

    case other =>
      log.error(s"TellSenderActor: unexpected message: ${other.getClass.getName}")
  }
}
