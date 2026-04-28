package com.example

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

// ---- GoReplicator actor ----

object GoReplicator {
  def props(testProbe: ActorRef): Props = Props(new GoReplicator(testProbe))

  class CounterState {
    val nodeValues: mutable.Map[String, Long] = mutable.Map.empty
    def total: Long = nodeValues.values.sum
    def mergeWith(incoming: Map[String, Long]): Unit =
      incoming.foreach { case (k, v) => nodeValues(k) = nodeValues.getOrElse(k, 0L).max(v) }
  }

  class ORSetState {
    val dots: mutable.Map[String, mutable.Set[(String, Long)]] = mutable.Map.empty
    val vv: mutable.Map[String, Long] = mutable.Map.empty
    def elements: Set[String] = dots.filter(_._2.nonEmpty).keySet.toSet

    def merge(snapDots: Map[String, List[(String, Long)]], snapVV: Map[String, Long]): Unit = {
      val allElems = dots.keySet ++ snapDots.keySet
      val newDots = mutable.Map.empty[String, mutable.Set[(String, Long)]]
      for (elem <- allElems) {
        val merged = mutable.Set.empty[(String, Long)]
        val incomingElemDots = snapDots.getOrElse(elem, Nil).toSet
        // local dots
        for (d <- dots.getOrElse(elem, mutable.Set.empty)) {
          if (incomingElemDots.contains(d)) merged.add(d)
          else if (!snapVV.contains(d._1) || d._2 > snapVV(d._1)) merged.add(d)
        }
        // incoming dots
        val localElemDots = dots.getOrElse(elem, mutable.Set.empty).toSet
        for (d <- incomingElemDots) {
          if (localElemDots.contains(d)) merged.add(d)
          else if (!vv.contains(d._1) || d._2 > vv(d._1)) merged.add(d)
        }
        if (merged.nonEmpty) newDots(elem) = merged
      }
      dots.clear()
      dots ++= newDots
      snapVV.foreach { case (k, v) => vv(k) = vv.getOrElse(k, 0L).max(v) }
    }
  }

  case class GetCounter(key: String)
  case class GetSet(key: String)
  case class CounterValue(key: String, value: Long)
  case class SetValue(key: String, elements: Set[String])
}

class GoReplicator(testProbe: ActorRef) extends Actor with ActorLogging {
  import GoReplicator._

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val counters = mutable.Map.empty[String, CounterState]
  private val sets = mutable.Map.empty[String, ORSetState]

  def receive: Receive = {
    case bytes: Array[Byte] =>
      val json = new String(bytes, "UTF-8")
      log.info("GoReplicator received: {}", json)
      try {
        val root: JsonNode = mapper.readTree(json)
        val msgType = root.get("type").asText()
        val key = root.get("key").asText()
        val payload = root.get("payload")

        msgType match {
          case "gcounter-gossip" =>
            val stateNode = payload.get("state")
            val incoming = mutable.Map.empty[String, Long]
            stateNode.fields().asScala.foreach { entry =>
              incoming(entry.getKey) = entry.getValue.asLong()
            }
            val state = counters.getOrElseUpdate(key, new CounterState)
            state.mergeWith(incoming.toMap)
            log.info("GoReplicator: merged GCounter[{}] total={}", key, state.total)
            testProbe ! CounterValue(key, state.total)

          case "orset-gossip" =>
            // dots: {"elem": [{"NodeID":"n","Counter":1}, ...], ...}
            // vv:   {"nodeID": counter, ...}
            val dotsNode = payload.get("dots")
            val vvNode = payload.get("vv")

            val snapDots = mutable.Map.empty[String, List[(String, Long)]]
            if (dotsNode != null) {
              dotsNode.fields().asScala.foreach { entry =>
                val elem = entry.getKey
                val dotList = entry.getValue.elements().asScala.map { d =>
                  (d.get("NodeID").asText(), d.get("Counter").asLong())
                }.toList
                snapDots(elem) = dotList
              }
            }
            val snapVV = mutable.Map.empty[String, Long]
            if (vvNode != null) {
              vvNode.fields().asScala.foreach { entry =>
                snapVV(entry.getKey) = entry.getValue.asLong()
              }
            }

            val state = sets.getOrElseUpdate(key, new ORSetState)
            state.merge(snapDots.toMap, snapVV.toMap)
            log.info("GoReplicator: merged ORSet[{}] elements={}", key, state.elements)
            testProbe ! SetValue(key, state.elements)

          case other =>
            log.warning("GoReplicator: unknown type={}", other)
        }
      } catch {
        case e: Exception => log.error(e, "GoReplicator parse error")
      }

    case GetCounter(key) =>
      sender() ! CounterValue(key, counters.get(key).map(_.total).getOrElse(0L))

    case GetSet(key) =>
      sender() ! SetValue(key, sets.get(key).map(_.elements).getOrElse(Set.empty))
  }
}

// ---- Test probe ----
class DDTestProbe extends Actor with ActorLogging {
  import GoReplicator._
  def receive: Receive = {
    case v: CounterValue => log.info("DDTestProbe: CounterValue key={} val={}", v.key, v.value)
    case s: SetValue     => log.info("DDTestProbe: SetValue key={} elems={}", s.key, s.elements)
  }
}

// ---- Main ----
object DistributedDataServer extends App {
  OrchestratorGate.require()
  val config = ConfigFactory.load("cluster")
  val system = ActorSystem("ClusterSystem", config)

  val probe = system.actorOf(Props[DDTestProbe], "ddTestProbe")
  system.actorOf(GoReplicator.props(probe), "goReplicator")

  println("--- DDATA SERVER READY ---")
  sys.addShutdownHook { system.terminate() }
}
