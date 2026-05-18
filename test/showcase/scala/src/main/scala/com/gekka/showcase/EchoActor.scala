package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, Props}

object EchoActor {
  def props: Props = Props(new EchoActor)
  val name: String = "echo"
}

class EchoActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case env: EchoEnvelope if env.direction == "SEND" =>
      sender() ! env.copy(direction = "REPLY")
    case env: EchoEnvelope =>
      log.error(s"EchoActor: received non-SEND envelope direction=${env.direction} seq=${env.seqNo}")
    case other =>
      log.error(s"EchoActor: unexpected message: ${other.getClass.getName}")
  }
}
