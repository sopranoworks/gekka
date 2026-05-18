package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, Props}

object SingletonActor {
  def props: Props = Props(new SingletonActor)
}

class SingletonActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case Ping(seq, origin) => sender() ! Pong(seq, origin)
    case other             => log.error(s"SingletonActor: unexpected message: ${other.getClass.getName}")
  }
}
