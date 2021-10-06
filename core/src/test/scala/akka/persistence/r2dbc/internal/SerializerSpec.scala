/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.r2dbc.query.TimestampOffset
import akka.serialization.SerializationExtension
import org.scalatest.wordspec.AnyWordSpecLike

class SerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  private val serializer = new R2dbcSerializer(system.classicSystem.asInstanceOf[ExtendedActorSystem])
  private val commitTimestamp = Instant.now()

  "SpannerSerializer" must {
    Seq(
      "TimestampOffset-1" -> TimestampOffset(commitTimestamp, Map.empty),
      "TimestampOffset-2" -> TimestampOffset(commitTimestamp, Map("pid1" -> 5L)),
      "TimestampOffset-3" -> TimestampOffset(commitTimestamp, Map("pid1" -> 5L, "pid2" -> 3L, "pid3" -> 7L))).foreach {
      case (scenario, item) =>
        s"resolve serializer for $scenario" in {
          SerializationExtension(system).findSerializerFor(item).getClass should be(classOf[R2dbcSerializer])
        }

        s"serialize and de-serialize $scenario" in {
          verifySerialization(item)
        }
    }
  }

  def verifySerialization(item: AnyRef): Unit =
    serializer.fromBinary(serializer.toBinary(item), serializer.manifest(item)) should be(item)
}
