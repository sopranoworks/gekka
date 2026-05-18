package com.gekka.showcase

import org.apache.pekko.actor.{Actor, ActorLogging, Cancellable, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ddata.Replicator._
import org.apache.pekko.cluster.ddata._

import scala.concurrent.duration._
import scala.util.Random

object DdataActors {
  def all(selfLabel: String): List[Props] = List(
    GCounterActor.props(selfLabel),
    PNCounterActor.props(selfLabel),
    GSetActor.props(selfLabel),
    ORSetActor.props(selfLabel),
    ORMapActor.props(selfLabel),
    ORFlagActor.props(selfLabel),
    LWWRegisterActor.props(selfLabel),
    LWWMapActor.props(selfLabel),
  )

  val names: List[String] = List(
    "ddata/gcounter", "ddata/pncounter", "ddata/gset", "ddata/orset",
    "ddata/ormap", "ddata/orflag", "ddata/lwwregister", "ddata/lwwmap",
  )

  case object Tick

  abstract class CrdtActor[T <: ReplicatedData](selfLabel: String, keyName: String) extends Actor with ActorLogging {
    import context.dispatcher
    protected val replicator = DistributedData(context.system).replicator
    protected implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress
    protected val rng = new Random()
    private var tick: Cancellable = _

    protected def tickMs: Long = 1000L + rng.nextInt(4000) // [1s, 5s)

    override def preStart(): Unit = {
      tick = context.system.scheduler.scheduleOnce(tickMs.millis, self, Tick)
    }

    override def postStop(): Unit = Option(tick).foreach(_.cancel())

    protected def reschedule(): Unit = {
      tick = context.system.scheduler.scheduleOnce(tickMs.millis, self, Tick)
    }

    override def receive: Receive = handle.orElse {
      case _: UpdateResponse[_] => ()
      case _: GetResponse[_]    => ()
      case other =>
        log.error(s"${getClass.getSimpleName}: unexpected: ${other.getClass.getName}")
    }

    def handle: Receive
  }
}

object GCounterActor {
  def props(selfLabel: String): Props = Props(new GCounterActor(selfLabel))
}
class GCounterActor(selfLabel: String) extends DdataActors.CrdtActor[GCounter](selfLabel, "g-counter") {
  private val key = GCounterKey("showcase-gcounter")
  override def handle: Receive = {
    case DdataActors.Tick =>
      if (rng.nextBoolean()) replicator ! Update(key, GCounter.empty, WriteLocal)(_ :+ 1)
      else                   replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object PNCounterActor {
  def props(selfLabel: String): Props = Props(new PNCounterActor(selfLabel))
}
class PNCounterActor(selfLabel: String) extends DdataActors.CrdtActor[PNCounter](selfLabel, "pn-counter") {
  private val key = PNCounterKey("showcase-pncounter")
  override def handle: Receive = {
    case DdataActors.Tick =>
      val op = rng.nextInt(3)
      if (op == 0)      replicator ! Update(key, PNCounter.empty, WriteLocal)(_ :+ 1)
      else if (op == 1) replicator ! Update(key, PNCounter.empty, WriteLocal)(_ decrement 1)
      else              replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object GSetActor {
  def props(selfLabel: String): Props = Props(new GSetActor(selfLabel))
}
class GSetActor(selfLabel: String) extends DdataActors.CrdtActor[GSet[String]](selfLabel, "g-set") {
  private val key = GSetKey[String]("showcase-gset")
  override def handle: Receive = {
    case DdataActors.Tick =>
      if (rng.nextBoolean()) replicator ! Update(key, GSet.empty[String], WriteLocal)(_ + s"item-${rng.nextInt(50)}")
      else                   replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object ORSetActor {
  def props(selfLabel: String): Props = Props(new ORSetActor(selfLabel))
}
class ORSetActor(selfLabel: String) extends DdataActors.CrdtActor[ORSet[String]](selfLabel, "or-set") {
  private val key = ORSetKey[String]("showcase-orset")
  override def handle: Receive = {
    case DdataActors.Tick =>
      val op = rng.nextInt(3)
      if (op == 0)      replicator ! Update(key, ORSet.empty[String], WriteLocal)(_ :+ s"item-${rng.nextInt(50)}")
      else if (op == 1) replicator ! Update(key, ORSet.empty[String], WriteLocal)(_ remove s"item-${rng.nextInt(50)}")
      else              replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object ORMapActor {
  def props(selfLabel: String): Props = Props(new ORMapActor(selfLabel))
}
class ORMapActor(selfLabel: String) extends DdataActors.CrdtActor[ORMap[String, ORSet[String]]](selfLabel, "or-map") {
  private val key = ORMapKey[String, ORSet[String]]("showcase-ormap")
  override def handle: Receive = {
    case DdataActors.Tick =>
      val k = s"k${rng.nextInt(10)}"
      if (rng.nextBoolean()) {
        replicator ! Update(key, ORMap.empty[String, ORSet[String]], WriteLocal) { m =>
          m.updated(node, k, ORSet.empty[String])(_ :+ s"v${rng.nextInt(100)}")
        }
      } else {
        replicator ! Get(key, ReadLocal)
      }
      reschedule()
  }
}

object ORFlagActor {
  def props(selfLabel: String): Props = Props(new ORFlagActor(selfLabel))
}
class ORFlagActor(selfLabel: String) extends DdataActors.CrdtActor[Flag](selfLabel, "or-flag") {
  private val key = FlagKey("showcase-orflag")
  override def handle: Receive = {
    case DdataActors.Tick =>
      if (rng.nextBoolean()) replicator ! Update(key, Flag.empty, WriteLocal)(_.switchOn)
      else                   replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object LWWRegisterActor {
  def props(selfLabel: String): Props = Props(new LWWRegisterActor(selfLabel))
}
class LWWRegisterActor(selfLabel: String) extends DdataActors.CrdtActor[LWWRegister[String]](selfLabel, "lww-register") {
  private val key = LWWRegisterKey[String]("showcase-lwwregister")
  override def handle: Receive = {
    case DdataActors.Tick =>
      if (rng.nextBoolean())
        replicator ! Update(key, LWWRegister(node, s"init-$selfLabel"), WriteLocal)(_.withValueOf(s"v${rng.nextInt(1000)}"))
      else
        replicator ! Get(key, ReadLocal)
      reschedule()
  }
}

object LWWMapActor {
  def props(selfLabel: String): Props = Props(new LWWMapActor(selfLabel))
}
class LWWMapActor(selfLabel: String) extends DdataActors.CrdtActor[LWWMap[String, String]](selfLabel, "lww-map") {
  private val key = LWWMapKey[String, String]("showcase-lwwmap")
  override def handle: Receive = {
    case DdataActors.Tick =>
      val k = s"k${rng.nextInt(10)}"
      if (rng.nextBoolean()) replicator ! Update(key, LWWMap.empty[String, String], WriteLocal)(_ :+ (k -> s"v${rng.nextInt(1000)}"))
      else                   replicator ! Get(key, ReadLocal)
      reschedule()
  }
}
