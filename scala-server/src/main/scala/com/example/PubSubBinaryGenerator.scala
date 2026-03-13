/*
 * PubSubBinaryGenerator.scala
 *
 * Generates binary test fixtures for Go's pub-sub compatibility tests.
 * Uses Pekko's actual DistributedPubSubMessageSerializer (serializer ID 9)
 * to produce canonical JVM wire-format bytes for all six message types.
 *
 * NOTE: This file lives in package org.apache.pekko.cluster.pubsub intentionally.
 * DistributedPubSubMediator.Internal is private[pekko], so the generator must reside
 * within the pekko package namespace to access Status, Delta, Bucket, ValueHolder,
 * and SendToOneSubscriber — exactly as Pekko's own serializer spec tests do.
 *
 * Output files (written to the directory given as args(0), default ../cluster/testdata):
 *   pubsub_publish.bin              - Publish("news", "Hello" bytes)        manifest="E" plain proto
 *   pubsub_send.bin                 - Send("/user/handler", bytes, true)    manifest="C" plain proto
 *   pubsub_sendtoall.bin            - SendToAll("/user/all", bytes, false)   manifest="D" plain proto
 *   pubsub_sendtoonesubscriber.bin  - SendToOneSubscriber(bytes)            manifest="F" plain proto
 *   pubsub_status.bin               - Status(Map(addr->42L), reply=true)    manifest="A" GZIP proto
 *   pubsub_delta.bin                - Delta(List(Bucket(addr,3,...)))        manifest="B" GZIP proto
 *
 * Run:
 *   cd scala-server && sbt "runMain org.apache.pekko.cluster.pubsub.PubSubBinaryGenerator ../cluster/testdata"
 */
package org.apache.pekko.cluster.pubsub

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.immutable.TreeMap
import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.{ActorSystem, Address}
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator._
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.Internal._
import org.apache.pekko.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory

object PubSubBinaryGenerator { // scalastyle:off

  /** Fixed test address used in Status and Delta fixtures. */
  val TestAddress: Address = Address("pekko", "TestSys", "127.0.0.1", 2551)

  def main(args: Array[String]): Unit = {
    val outputDir = if (args.nonEmpty) args(0) else "../cluster/testdata"

    // Use provider=local so we don't need a live network.
    // withFallback(defaultReference()) ensures all reference.confs are loaded,
    // including pekko-cluster-tools which registers the pubsub serializer at ID 9.
    val config = ConfigFactory.parseString(
      """
        pekko {
          actor.provider = local
          loglevel = WARNING
          stdout-loglevel = WARNING
          log-dead-letters = 0
          log-dead-letters-during-shutdown = off
        }
      """
    ).withFallback(ConfigFactory.defaultReference())

    val system = ActorSystem("BinaryGen", config)

    try {
      val serialization = SerializationExtension(system)

      val dir = new File(outputDir)
      if (!dir.exists()) dir.mkdirs()

      println(s"Writing fixtures to: ${dir.getAbsolutePath}")
      println()

      def writeFixture(filename: String, obj: AnyRef): Unit = {
        val serializer = serialization.findSerializerFor(obj)
        val bytes = serializer.toBinary(obj)
        val manifest = Serializers.manifestFor(serializer, obj)
        val serializerId = serializer.identifier
        val path = Paths.get(outputDir, filename)
        Files.write(path, bytes)
        println(f"  $filename%-42s  manifest='$manifest'  serializerID=$serializerId  bytes=${bytes.length}%d")
      }

      println("--- Plain Protobuf (no GZIP) ---")

      // E: Publish
      // payload = "Hello" as raw bytes via ByteArraySerializer (serializerID=4, no manifest)
      val publishMsg = Publish("news", "Hello".getBytes("UTF-8"))
      writeFixture("pubsub_publish.bin", publishMsg)

      // C: Send
      val sendMsg = Send("/user/handler", Array[Byte](1, 2, 3, 4), localAffinity = true)
      writeFixture("pubsub_send.bin", sendMsg)

      // D: SendToAll
      val sendToAllMsg = SendToAll("/user/all", Array[Byte](5, 6, 7), allButSelf = false)
      writeFixture("pubsub_sendtoall.bin", sendToAllMsg)

      // F: SendToOneSubscriber
      val sendToOneMsg = SendToOneSubscriber(Array[Byte](9, 10, 11))
      writeFixture("pubsub_sendtoonesubscriber.bin", sendToOneMsg)

      println()
      println("--- GZIP-compressed Protobuf ---")

      // A: Status — single entry so proto bytes are deterministic for Go comparison
      val statusMsg = Status(
        Map(TestAddress -> 42L),
        isReplyToStatus = true
      )
      writeFixture("pubsub_status.bin", statusMsg)

      // B: Delta — single bucket, single content entry, no actor ref (None)
      val deltaMsg = Delta(List(
        Bucket(
          TestAddress,
          3L,
          TreeMap("/news" -> ValueHolder(2L, None))
        )
      ))
      writeFixture("pubsub_delta.bin", deltaMsg)

      println()
      println("All fixtures written successfully.")
      println()
      println("Run Go tests with:")
      println("  go test ./cluster/ -v -run TestPubSub")

    } finally {
      Await.result(system.terminate(), 10.seconds)
    }
  }
}
