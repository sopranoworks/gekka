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

// "gekka's internal SystemMessage ping" in §5.1. Real wire type (not the
// "system-msg-stub" String stand-in used in Phase 1) so the cross-language
// codec exercises @class polymorphism for a gekka-side semantic type, not
// just a String alias. Phase 3 / Task #2 setup.
final case class SystemMessagePing(seqNo: Long, origin: String) extends ShowcaseMessage

// FT4 messages.
final case class Ping(seqNo: Long, origin: String) extends ShowcaseMessage
final case class Pong(seqNo: Long, origin: String) extends ShowcaseMessage
