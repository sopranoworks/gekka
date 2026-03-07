package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class EchoActor extends Actor {
  def receive = {
    case msg: String =>
      println(s"Received string: $msg")
      sender() ! s"Echo: $msg".getBytes("UTF-8")
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      println(s"Received bytes: $str")
      sender() ! s"Echo: $str".getBytes("UTF-8")
  }
}

object PekkoServer extends App {
  val config = ConfigFactory.load()
  val system = ActorSystem("RemoteSystem", config)
  val echoActor = system.actorOf(Props[EchoActor], "echo")
  
  println("--- PEKKO SERVER READY ---")
}
