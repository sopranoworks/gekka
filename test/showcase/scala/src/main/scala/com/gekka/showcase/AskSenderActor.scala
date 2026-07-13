package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, Cancellable, Props}
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object AskSenderActor {
  def props(selfLabel: String, peers: List[String]): Props =
    Props(new AskSenderActor(selfLabel, peers))

  val name: String = "ask-sender"

  case object Tick
  val IntervalMs: Long = 4000L
  val AskTimeout: Timeout = Timeout(5.seconds)
}

class AskSenderActor(selfLabel: String, peers: List[String]) extends Actor with ActorLogging {
  import AskSenderActor._
  import context.dispatcher

  private var nextSeq: Long = 0L
  private val payloadKinds = Vector("string", "long", "system", "custom")
  private var kindIdx = 0

  private var tickHandle: Cancellable = _

  override def preStart(): Unit = {
    tickHandle = context.system.scheduler.scheduleWithFixedDelay(
      1.second, IntervalMs.millis, self, Tick)
  }

  override def postStop(): Unit = Option(tickHandle).foreach(_.cancel())

  override def receive: Receive = {
    case Tick =>
      peers.foreach { peer =>
        val seq = nextSeq; nextSeq += 1
        val kind = payloadKinds(kindIdx % payloadKinds.size); kindIdx += 1
        val payload: AnyRef = kind match {
          case "string" => s"ask-$seq-$selfLabel"
          case "long"   => java.lang.Long.valueOf(seq)
          case "system" => SystemMessagePing(seq, selfLabel)
          case "custom" => ShowcaseEchoCustom(seq, selfLabel, Vector.fill(16)(0x42.toByte))
        }
        val env = AskEnvelope(seq, selfLabel, "SEND", kind, payload)
        val target = context.actorSelection(peer + "/user/ask")
        val sentAt = System.currentTimeMillis()
        val fut = Patterns.ask(target, env, AskTimeout.duration.toMillis)
        fut.onComplete {
          case Success(_) => // ok
          case Failure(ex) =>
            // Spec §4: asks issued before the steady-state anchor are
            // setup-phase traffic and must not fail the strict window.
            if (SteadyAnchor.countsForStrictWindow(sentAt))
              log.error(s"AskSender: ask to $peer timed out for seq=$seq type=$kind: ${ex.getMessage}")
            else
              log.debug(s"AskSender: setup-phase ask miss (not counted) peer=$peer seq=$seq")
        }
      }

    case other =>
      log.error(s"AskSenderActor: unexpected message: ${other.getClass.getName}")
  }
}
