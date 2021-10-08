/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.EventEnvelope
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.serialization.SerializationExtension
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

class EventsBySliceBacktrackingSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val settings = new R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
  private val stringSerializer = SerializationExtension(system).serializerFor(classOf[String])
  private val log = LoggerFactory.getLogger(getClass)

  // to be able to store events with specific timestamps
  private def writeEvent(slice: Int, persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = s"INSERT INTO ${settings.journalTable} " +
      "(slice, entity_type_hint, persistence_id, sequence_number, db_timestamp, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload) " +
      "VALUES ($1, $2, $3, $4, $5, '', $6, '', $7, '', $8)"
    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)

    val result = r2dbcExecutor.updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind("$1", slice)
        .bind("$2", entityTypeHint)
        .bind("$3", persistenceId)
        .bind("$4", seqNr)
        .bind("$5", timestamp)
        .bind("$6", timestamp.toEpochMilli)
        .bind("$7", stringSerializer.identifier)
        .bind("$8", stringSerializer.toBinary(event))
    }
    result.futureValue shouldBe 1
  }

  "eventsBySlices backtracking" should {

    "find old events with earlier timestamp" in {
      // this scenario is handled by the backtracking query
      val entityTypeHint = nextEntityTypeHint()
      val pid1 = nextPid(entityTypeHint)
      val pid2 = nextPid(entityTypeHint)
      val slice1 = 1
      val slice2 = 2
      val sinkProbe = TestSink.probe[EventEnvelope](system.classicSystem)

      // don't let behind-current-time be a reason for not finding events
      val startTime = Instant.now().minusSeconds(10 * 60)

      writeEvent(slice1, pid1, 1L, startTime, "e1-1")
      writeEvent(slice1, pid1, 2L, startTime.plusMillis(1), "e1-2")

      val result: TestSubscriber.Probe[EventEnvelope] =
        query.eventsBySlices(entityTypeHint, slice1, slice2, NoOffset).runWith(sinkProbe).request(100)

      result.expectNext().event shouldBe s"e1-1"
      result.expectNext().event shouldBe s"e1-2"

      // first backtracking query kicks in immediately after the first normal query has finished
      // and it also emits duplicates (by design)
      result.expectNext().event shouldBe s"e1-1"
      result.expectNoMessage(100.millis) // not e1-2

      writeEvent(slice1, pid1, 3L, startTime.plusMillis(3), "e1-3")
      result.expectNext().event shouldBe s"e1-3"

      // before e1-3 so it will not be found by the normal query
      writeEvent(slice2, pid2, 1L, startTime.plusMillis(2), "e2-1")

      // no backtracking yet
      result.expectNoMessage(settings.querySettings.refreshInterval + 100.millis)

      // FIXME one case that isn't covered yet is if there is no progress (no more events) then
      // there will not be any new backtracking query and missed events will not be found (until there are
      // new events that move the latest offset forward)

      // after 1/2 of the backtracking widow, to kick off a backtracking query
      writeEvent(
        slice1,
        pid1,
        4L,
        startTime.plusMillis(settings.querySettings.backtrackingWindow.toMillis / 2).plusMillis(4),
        "e1-4")
      result.expectNext().event shouldBe s"e1-4"

      // backtracking finds it,  and it also emits duplicates (by design)
      // e1-1 was already handled by previous backtracking query
      result.expectNext().event shouldBe s"e1-2"
      result.expectNext().event shouldBe s"e2-1"
      result.expectNext().event shouldBe s"e1-3"
      result.expectNoMessage(100.millis) // not e1-4
      result.expectNoMessage(settings.querySettings.refreshInterval)
    }
  }

}
