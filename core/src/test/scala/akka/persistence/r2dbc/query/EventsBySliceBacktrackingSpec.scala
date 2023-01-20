/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EventsBySliceBacktrackingSpec {
  private val config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.journal.publish-events = off
    """)
    .withFallback(TestConfig.config)
}

class EventsBySliceBacktrackingSpec
    extends ScalaTestWithActorTestKit(EventsBySliceBacktrackingSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  private val settings = new R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  private val query = PersistenceQuery(testKit.system)
    .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
  private val stringSerializer = SerializationExtension(system).serializerFor(classOf[String])
  private val log = LoggerFactory.getLogger(getClass)

  // to be able to store events with specific timestamps
  private def writeEvent(slice: Int, persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    log.debugN("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${settings.journalTableWithSchema}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload)
      VALUES (?, ?, ?, ?, ?, '', '', ?, '', ?)"""
    val entityType = PersistenceId.extractEntityType(persistenceId)

    val result = r2dbcExecutor.updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, seqNr)
        .bind(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bind(6, stringSerializer.toBinary(event))
    }
    result.futureValue shouldBe 1
  }

  "eventsBySlices backtracking" should {

    "find old events with earlier timestamp" in {
      // this scenario is handled by the backtracking query
      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = query.sliceForPersistenceId(pid1)
      val slice2 = query.sliceForPersistenceId(pid2)
      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      // don't let behind-current-time be a reason for not finding events
      val startTime = InstantFactory.now().minusSeconds(10 * 60)

      writeEvent(slice1, pid1, 1L, startTime, "e1-1")
      writeEvent(slice1, pid1, 2L, startTime.plusMillis(1), "e1-2")

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, NoOffset)
          .runWith(sinkProbe)
          .request(100)

      val env1 = result.expectNext()
      env1.persistenceId shouldBe pid1
      env1.sequenceNr shouldBe 1L
      env1.eventOption shouldBe Some("e1-1")
      env1.source shouldBe EnvelopeOrigin.SourceQuery

      val env2 = result.expectNext()
      env2.persistenceId shouldBe pid1
      env2.sequenceNr shouldBe 2L
      env2.eventOption shouldBe Some("e1-2")
      env2.source shouldBe EnvelopeOrigin.SourceQuery

      // first backtracking query kicks in immediately after the first normal query has finished
      // and it also emits duplicates (by design)
      val env3 = result.expectNext()
      env3.persistenceId shouldBe pid1
      env3.sequenceNr shouldBe 1L
      env3.source shouldBe EnvelopeOrigin.SourceBacktracking
      // event payload isn't included in backtracking results
      env3.eventOption shouldBe None
      // but it can be lazy loaded
      query.loadEnvelope[String](env3.persistenceId, env3.sequenceNr).futureValue.eventOption shouldBe Some("e1-1")
      // backtracking up to (and equal to) the same offset
      val env4 = result.expectNext()
      env4.persistenceId shouldBe pid1
      env4.sequenceNr shouldBe 2L
      env4.eventOption shouldBe None

      result.expectNoMessage(100.millis) // not e1-2

      writeEvent(slice1, pid1, 3L, startTime.plusMillis(3), "e1-3")
      val env5 = result.expectNext()
      env5.persistenceId shouldBe pid1
      env5.sequenceNr shouldBe 3L

      // before e1-3 so it will not be found by the normal query
      writeEvent(slice2, pid2, 1L, startTime.plusMillis(2), "e2-1")

      // no backtracking yet
      result.expectNoMessage(settings.querySettings.refreshInterval + 100.millis)

      // after 1/2 of the backtracking widow, to kick off a backtracking query
      writeEvent(
        slice1,
        pid1,
        4L,
        startTime.plusMillis(settings.querySettings.backtrackingWindow.toMillis / 2).plusMillis(4),
        "e1-4")
      val env6 = result.expectNext()
      env6.persistenceId shouldBe pid1
      env6.sequenceNr shouldBe 4L

      // backtracking finds it,  and it also emits duplicates (by design)
      // e1-1 and e1-2 were already handled by previous backtracking query
      val env7 = result.expectNext()
      env7.persistenceId shouldBe pid2
      env7.sequenceNr shouldBe 1L

      val env8 = result.expectNext()
      env8.persistenceId shouldBe pid1
      env8.sequenceNr shouldBe 3L

      val env9 = result.expectNext()
      env9.persistenceId shouldBe pid1
      env9.sequenceNr shouldBe 4L
    }
  }

}
