package com.gekka.showcase

import java.nio.file.{Files, Paths}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension, SerializerWithStringManifest}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** JacksonCborGoldensSpec — Phase 3 / Task #5, direction (b).
 *
 * Decodes the `go_emitted_*.cbor` golden fixtures produced by the gekka-side
 * jacksoncbor tests (TestJacksonCbor_*_GoEmits_Stable) and asserts that
 * Pekko's JacksonCborSerializer reconstructs the source case-class values
 * field-for-field. This is the direction-(b) contract from 3-0.(ii):
 *
 *   Gekka emits bytes for a value with known source fields.
 *   This spec loads those bytes into Pekko's actual JacksonCborSerializer.
 *   Pekko's Jackson decoder reconstructs the case-class.
 *   Field equality between Pekko-decoded value and source values = PASS.
 *
 * Why byte-equality is NOT used: CBOR has multiple valid encodings for the
 * same logical value (map key ordering, integer widths, definite vs
 * indefinite-length containers). Byte-equality could both falsely fail
 * (semantically-equivalent encodings differ) and falsely pass (matching
 * bytes Jackson coerces to different field values). Only a real Jackson
 * decode verifies cross-language identity.
 *
 * SPDX-License-Identifier: MIT
 */
class JacksonCborGoldensSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  // CWD at sbt test time is test/showcase/scala/. Goldens live four directories up.
  private val TestdataDir = Paths.get("..", "..", "..", "internal", "core", "testdata", "jackson_cbor")
    .toAbsolutePath.normalize

  private val system: ActorSystem = {
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
    ActorSystem("JacksonCborGoldensSpec", cfg)
  }

  private val ser: Serialization = SerializationExtension(system)

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def decodeWithJacksonCbor(bytes: Array[Byte], manifest: String): AnyRef = {
    val s = ser.serializerByIdentity(33).asInstanceOf[SerializerWithStringManifest]
    s.fromBinary(bytes, manifest)
  }

  private def loadBytes(name: String): Array[Byte] =
    Files.readAllBytes(TestdataDir.resolve(name))

  "Pekko's JacksonCborSerializer" should {

    "decode go-emitted SystemMessagePing into the source field values" in {
      // Source fields from internal/core/testdata/jackson_cbor/go_emitted_systemmessageping.json:
      //   seqNo = 77, origin = "g1"
      val bytes = loadBytes("go_emitted_systemmessageping.cbor")
      val decoded = decodeWithJacksonCbor(bytes, "com.gekka.showcase.SystemMessagePing")
      decoded shouldBe a[SystemMessagePing]
      val sm = decoded.asInstanceOf[SystemMessagePing]
      sm.seqNo shouldBe 77L
      sm.origin shouldBe "g1"
    }

    "decode go-emitted Ping into the source field values" in {
      // Source fields from internal/core/testdata/jackson_cbor/go_emitted_ping.json:
      //   seqNo = 88, origin = "g2"
      val bytes = loadBytes("go_emitted_ping.cbor")
      val decoded = decodeWithJacksonCbor(bytes, "com.gekka.showcase.Ping")
      decoded shouldBe a[Ping]
      val p = decoded.asInstanceOf[Ping]
      p.seqNo shouldBe 88L
      p.origin shouldBe "g2"
    }
  }
}
