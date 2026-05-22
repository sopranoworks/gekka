package com.gekka.showcase

import java.io.File
import java.nio.file.{Files, Path, Paths}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension, SerializerWithStringManifest}
import com.typesafe.config.ConfigFactory

/** EmitGoldens — Phase 3 / Task #2.
 *
 * One-shot test main that produces Scala-emitted golden CBOR fixtures under
 * `internal/core/testdata/jackson_cbor/` for the Go-side Phase 3 decoder
 * tests. Each fixture also gets a sidecar `*.json` manifest recording the
 * source field values so the Go-side test can assert decoded fields against
 * a known-good reference rather than against bytes alone.
 *
 * Run from `test/showcase/scala/`:
 *   sbt "test:runMain com.gekka.showcase.EmitGoldens"
 *
 * Run from the repo root via the convenience script (added by Task #6):
 *   bin/emit-cbor-goldens.sh
 *
 * The emitter writes:
 *   internal/core/testdata/jackson_cbor/scala_emitted_<name>.cbor
 *   internal/core/testdata/jackson_cbor/scala_emitted_<name>.json
 *
 * Fixtures emitted:
 *   echoenvelope_string   — EchoEnvelope with payload = String
 *   echoenvelope_long     — EchoEnvelope with payload = java.lang.Long
 *   echoenvelope_system   — EchoEnvelope with payload = SystemMessagePing
 *   echoenvelope_custom   — EchoEnvelope with payload = ShowcaseEchoCustom
 *   askenvelope_string    — AskEnvelope  with payload = String
 *   askenvelope_long      — AskEnvelope  with payload = java.lang.Long
 *   askenvelope_system    — AskEnvelope  with payload = SystemMessagePing
 *   askenvelope_custom    — AskEnvelope  with payload = ShowcaseEchoCustom
 *   ping                  — Ping(seqNo=42, origin="s1")
 *   pong                  — Pong(seqNo=42, origin="g1")
 *   systemmessageping     — SystemMessagePing(seqNo=99, origin="s1")
 *
 * SPDX-License-Identifier: MIT
 */
object EmitGoldens {

  /** Resolves the testdata directory relative to the showcase-scala sbt project root.
   *  CWD when sbt runs is `test/showcase/scala/`, so the testdata lives four
   *  levels up plus `internal/core/testdata/jackson_cbor`. */
  private val TestdataDir: Path =
    Paths.get("..", "..", "..", "internal", "core", "testdata", "jackson_cbor").toAbsolutePath.normalize

  def main(args: Array[String]): Unit = {
    val cfg = ConfigFactory.parseString(
      """
      pekko.actor {
        provider                       = local
        allow-java-serialization       = off
        warn-about-java-serializer-usage = on
        serializers {
          jackson-cbor = "org.apache.pekko.serialization.jackson.JacksonCborSerializer"
        }
        serialization-bindings {
          "com.gekka.showcase.ShowcaseMessage" = jackson-cbor
        }
      }
      """)

    val system = ActorSystem("GoldenEmitter", cfg)
    try {
      val ser: Serialization = SerializationExtension(system)

      Files.createDirectories(TestdataDir)

      val customBytes: Vector[Byte] = Vector.fill(16)(0x42.toByte)
      val systemPayload              = SystemMessagePing(seqNo = 7L,  origin = "s1")
      val customPayload              = ShowcaseEchoCustom(seqNo = 11L, originator = "s1", payload = customBytes)

      // EchoEnvelope × 4 payload kinds
      emit(ser, "echoenvelope_string",
        EchoEnvelope(1L, "s1", "SEND", "string", "hello-1-s1"),
        Map("seqNo" -> "1", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "string", "payload" -> "hello-1-s1"))

      emit(ser, "echoenvelope_long",
        EchoEnvelope(2L, "s1", "SEND", "long", java.lang.Long.valueOf(2L)),
        Map("seqNo" -> "2", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "long", "payload" -> "2"))

      emit(ser, "echoenvelope_system",
        EchoEnvelope(3L, "s1", "SEND", "system", systemPayload),
        Map("seqNo" -> "3", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "system",
          "payload.@class" -> "com.gekka.showcase.SystemMessagePing",
          "payload.seqNo"  -> "7",
          "payload.origin" -> "s1"))

      emit(ser, "echoenvelope_custom",
        EchoEnvelope(4L, "s1", "SEND", "custom", customPayload),
        Map("seqNo" -> "4", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "custom",
          "payload.@class"     -> "com.gekka.showcase.ShowcaseEchoCustom",
          "payload.seqNo"      -> "11",
          "payload.originator" -> "s1",
          "payload.bytes_hex"  -> customBytes.map(b => f"${b & 0xff}%02x").mkString))

      // AskEnvelope × 4 payload kinds
      emit(ser, "askenvelope_string",
        AskEnvelope(101L, "s1", "SEND", "string", "ask-101-s1"),
        Map("seqNo" -> "101", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "string", "payload" -> "ask-101-s1"))

      emit(ser, "askenvelope_long",
        AskEnvelope(102L, "s1", "SEND", "long", java.lang.Long.valueOf(102L)),
        Map("seqNo" -> "102", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "long", "payload" -> "102"))

      emit(ser, "askenvelope_system",
        AskEnvelope(103L, "s1", "SEND", "system", systemPayload),
        Map("seqNo" -> "103", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "system",
          "payload.@class" -> "com.gekka.showcase.SystemMessagePing",
          "payload.seqNo"  -> "7",
          "payload.origin" -> "s1"))

      emit(ser, "askenvelope_custom",
        AskEnvelope(104L, "s1", "SEND", "custom", customPayload),
        Map("seqNo" -> "104", "originator" -> "s1", "direction" -> "SEND", "payloadKind" -> "custom",
          "payload.@class"     -> "com.gekka.showcase.ShowcaseEchoCustom",
          "payload.seqNo"      -> "11",
          "payload.originator" -> "s1",
          "payload.bytes_hex"  -> customBytes.map(b => f"${b & 0xff}%02x").mkString))

      // FT4 flat shapes
      emit(ser, "ping",  Ping(42L, "s1"), Map("seqNo" -> "42", "origin" -> "s1"))
      emit(ser, "pong",  Pong(42L, "g1"), Map("seqNo" -> "42", "origin" -> "g1"))

      // Standalone SystemMessagePing (used by the cross-language identity test, direction a)
      emit(ser, "systemmessageping",
        SystemMessagePing(seqNo = 99L, origin = "s1"),
        Map("seqNo" -> "99", "origin" -> "s1"))

      println(s"emitted ${TestdataDir.toString}: 11 goldens (cbor + json)")
    } finally {
      system.terminate()
    }
  }

  private def emit(ser: Serialization, name: String, msg: AnyRef, sourceFields: Map[String, String]): Unit = {
    val serializer = ser.findSerializerFor(msg)
    val bytes      = serializer.toBinary(msg)
    val manifest = serializer match {
      case s: SerializerWithStringManifest => s.manifest(msg)
      case _                                => msg.getClass.getName
    }
    val id = serializer.identifier

    val cborPath = TestdataDir.resolve(s"scala_emitted_$name.cbor")
    val jsonPath = TestdataDir.resolve(s"scala_emitted_$name.json")

    Files.write(cborPath, bytes)

    val hex = bytes.map(b => f"${b & 0xff}%02x").mkString
    val fieldsJson = sourceFields.toSeq.sortBy(_._1).map { case (k, v) =>
      s"""    ${quote(k)}: ${quote(v)}"""
    }.mkString(",\n")
    val sidecar =
      s"""{
         |  "fixture_name": ${quote(name)},
         |  "source_type": ${quote(msg.getClass.getName)},
         |  "manifest": ${quote(manifest)},
         |  "serializer_id": $id,
         |  "serializer_class": ${quote(serializer.getClass.getName)},
         |  "cbor_byte_length": ${bytes.length},
         |  "cbor_hex": ${quote(hex)},
         |  "source_fields": {
         |$fieldsJson
         |  }
         |}
         |""".stripMargin
    Files.write(jsonPath, sidecar.getBytes("UTF-8"))

    println(s"  $name: ${bytes.length} bytes, manifest=$manifest, id=$id -> ${cborPath.getFileName}")
  }

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
