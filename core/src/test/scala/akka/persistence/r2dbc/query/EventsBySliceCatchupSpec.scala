/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink

object EventsBySliceCatchupSpec {
  private val BufferSize = 100

  private val config = ConfigFactory
    .parseString(s"""
    akka.persistence.r2dbc.query.buffer-size = $BufferSize
    """)
    .withFallback(TestConfig.config)
}

class EventsBySliceCatchupSpec
    extends ScalaTestWithActorTestKit(EventsBySliceCatchupSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system)
    .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  "eventsBySlices catching up from old events" should {

    // reproducer of bucket timestamp rounding bug
    "progress to end" in {
      pendingIfMoreThanOneDataPartition()

      TestData.oldEventsWithInterestingTimestampPattern.foreach { case (t, pid, seqNr) =>
        writeEvent(persistenceExt.sliceForPersistenceId(pid), pid, seqNr, Instant.parse(t), s"e-$seqNr")
      }

      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      val startTimestamp = Instant.parse("2025-04-30T14:56:51.569186Z")
      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String]("deal", 960, 975, TimestampOffset(startTimestamp, Map("deal|2186922658" -> 506)))
          .filterNot(_.source == EnvelopeOrigin.SourceBacktracking)
          .runWith(sinkProbe)
          .request(10000)

      result.expectNextN(1297)
      result.expectNoMessage()

      result.cancel()
    }

  }

}
