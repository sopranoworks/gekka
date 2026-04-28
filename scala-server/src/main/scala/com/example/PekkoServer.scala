package com.example

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import org.apache.pekko.util.ByteString

class EchoActor extends Actor {
  override def preStart(): Unit = {
    println(s"[SCALA] EchoActor starting at ${self.path}")
  }

  def receive = {
    case msg: String =>
      println(s"[SCALA] Received string (class=${msg.getClass.getName}) from ${sender().path} addressed to ${self.path}: $msg")
      sender() ! s"Echo: $msg"
    case msg: Array[Byte] =>
      val str = new String(msg, "UTF-8")
      val hex = msg.map("%02x".format(_)).mkString
      println(s"[SCALA] Received bytes (class=${msg.getClass.getName}) from ${sender().path} addressed to ${self.path}: $str (hex: $hex)")
      sender() ! ("Echo: " + str).getBytes("UTF-8")
    case msg: ByteString =>
      val str = msg.utf8String
      println(s"[SCALA] Received ByteString (class=${msg.getClass.getName}) from ${sender().path} addressed to ${self.path}: $str")
      sender() ! s"Echo: $str"
    case other =>
      println(s"[SCALA] Received unknown message (class=${other.getClass.getName}) from ${sender().path} addressed to ${self.path}: $other")
      sender() ! s"Echo: unknown type ${other.getClass.getName}"
  }
}

object PekkoServer extends App {
  OrchestratorGate.require()
  val config = ConfigFactory.load()
  val system = ActorSystem("RemoteSystem", config)
  val echoActor = system.actorOf(Props[EchoActor], "echo")
  
  println("--- PEKKO SERVER READY ---")
}
