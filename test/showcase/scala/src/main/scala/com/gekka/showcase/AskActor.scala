package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, Props}

object AskActor {
  def props: Props = Props(new AskActor)
  val name: String = "ask"
}

class AskActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case env: AskEnvelope if env.direction == "SEND" =>
      sender() ! env.copy(direction = "REPLY")
    case other =>
      log.error(s"AskActor: unexpected message: ${other.getClass.getName}")
  }
}
