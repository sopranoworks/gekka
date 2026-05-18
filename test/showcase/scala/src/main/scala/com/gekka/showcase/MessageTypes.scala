package com.gekka.showcase

import java.io.Serializable
import org.apache.pekko.actor.ActorRef

/** All messages are JSON-serializable via pekko-serialization-jackson. */
sealed trait ShowcaseMessage extends Serializable

// FT1 + FT2 envelopes share the same shape; the actor type differs.
final case class EchoEnvelope(seqNo: Long, originator: String, direction: String, payloadKind: String, payload: AnyRef)
  extends ShowcaseMessage
final case class AskEnvelope(seqNo: Long, originator: String, direction: String, payloadKind: String, payload: AnyRef)
  extends ShowcaseMessage

// A custom payload type to satisfy "messages can be ... classes" in §5.1.
final case class ShowcaseEchoCustom(seqNo: Long, originator: String, payload: Vector[Byte])
  extends ShowcaseMessage

// FT4 messages.
final case class Ping(seqNo: Long, origin: String) extends ShowcaseMessage
final case class Pong(seqNo: Long, origin: String) extends ShowcaseMessage
