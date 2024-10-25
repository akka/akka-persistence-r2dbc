/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.codec.PayloadCodec
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import scala.jdk.DurationConverters._

object EventsBySliceBacktrackingSpec {
  private val BufferSize = 10 // small buffer for testing

  private val config = ConfigFactory
    .parseString(s"""
    akka.persistence.r2dbc.journal.publish-events = off
    akka.persistence.r2dbc.query.buffer-size = $BufferSize
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
  implicit val payloadCodec: PayloadCodec = settings.codecSettings.JournalImplicits.journalPayloadCodec
  implicit val timestampCodec: TimestampCodec = settings.codecSettings.JournalImplicits.timestampCodec
  implicit val queryAdapter: QueryAdapter = settings.codecSettings.JournalImplicits.queryAdapter

  private val query = PersistenceQuery(testKit.system)
    .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
  private val stringSerializer = SerializationExtension(system).serializerFor(classOf[String])
  private val log = LoggerFactory.getLogger(getClass)

  // to be able to store events with specific timestamps
  private def writeEvent(slice: Int, persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${settings.journalTableWithSchema(slice)}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload)
      VALUES (?, ?, ?, ?, ?, '', '', ?, '', ?)"""
    val entityType = PersistenceId.extractEntityType(persistenceId)

    val result = r2dbcExecutor(slice).updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, seqNr)
        .bindTimestamp(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bindPayload(6, stringSerializer.toBinary(event))
    }
    result.futureValue shouldBe 1
  }

  "eventsBySlices backtracking" should {

    "find old events with earlier timestamp" in {
      pendingIfMoreThanOneDataPartition()

      // this scenario is handled by the backtracking query
      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = query.sliceForPersistenceId(pid1)
      val slice2 = query.sliceForPersistenceId(pid2)
      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      // don't let behind-current-time be a reason for not finding events
      val startTime = InstantFactory.now().minusSeconds(90)

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

      // after 1/2 of the backtracking, to kick off a backtracking query
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

      result.cancel()
    }

    "emit from backtracking after first normal query" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = query.sliceForPersistenceId(pid1)
      val slice2 = query.sliceForPersistenceId(pid2)
      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      // don't let behind-current-time be a reason for not finding events
      val startTime = InstantFactory.now().minusSeconds(90)

      writeEvent(slice1, pid1, 1L, startTime, "e1-1")
      writeEvent(slice1, pid1, 2L, startTime.plusMillis(2), "e1-2")
      writeEvent(slice1, pid1, 3L, startTime.plusMillis(4), "e1-3")

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, offset)
          .runWith(sinkProbe)
          .request(100)

      def expect(env: EventEnvelope[String], pid: String, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid
        env.sequenceNr shouldBe seqNr
        env.eventOption shouldBe eventOption
        env.offset
      }

      val result1 = startQuery(NoOffset)
      expect(result1.expectNext(), pid1, 1L, Some("e1-1"))
      expect(result1.expectNext(), pid1, 2L, Some("e1-2"))
      expect(result1.expectNext(), pid1, 3L, Some("e1-3"))

      // first backtracking query kicks in immediately after the first normal query has finished
      // and it also emits duplicates (by design)
      expect(result1.expectNext(), pid1, 1L, None)
      expect(result1.expectNext(), pid1, 2L, None)
      val offset1 = expect(result1.expectNext(), pid1, 3L, None)
      result1.cancel()

      // write delayed events from pid2
      writeEvent(slice2, pid2, 1L, startTime.plusMillis(1), "e2-1")
      writeEvent(slice2, pid2, 2L, startTime.plusMillis(3), "e2-2")
      writeEvent(slice2, pid2, 3L, startTime.plusMillis(5), "e2-3")

      val result2 = startQuery(offset1)
      // backtracking
      expect(result2.expectNext(), pid1, 1L, None)
      expect(result2.expectNext(), pid2, 1L, None)
      expect(result2.expectNext(), pid1, 2L, None)
      expect(result2.expectNext(), pid2, 2L, None)
      expect(result2.expectNext(), pid1, 3L, None)
      // from normal query
      expect(result2.expectNext(), pid2, 3L, Some("e2-3"))

      result2.cancel()
    }

    "skip backtracking when far behind current time" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = query.sliceForPersistenceId(pid1)
      val slice2 = query.sliceForPersistenceId(pid2)
      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      val startTime = InstantFactory.now().minusSeconds(60 * 60 * 24)

      (1 to 100).foreach { n =>
        writeEvent(slice1, pid1, n, startTime.plusSeconds(n).plusMillis(1), s"e1-$n")
        writeEvent(slice2, pid2, n, startTime.plusSeconds(n).plusMillis(2), s"e2-$n")
      }

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, offset)
          .runWith(sinkProbe)
          .request(1000)

      def expect(env: EventEnvelope[String], pid: String, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid
        env.sequenceNr shouldBe seqNr
        if (eventOption.isEmpty)
          env.source shouldBe EnvelopeOrigin.SourceBacktracking
        else
          env.source shouldBe EnvelopeOrigin.SourceQuery
        env.eventOption shouldBe eventOption
        env.offset
      }

      val result1 = startQuery(NoOffset)
      (1 to 100).foreach { n =>
        expect(result1.expectNext(), pid1, n, Some(s"e1-$n"))
        expect(result1.expectNext(), pid2, n, Some(s"e2-$n"))
      }
      // no backtracking
      result1.expectNoMessage()

      val now = InstantFactory.now().minus(settings.querySettings.backtrackingBehindCurrentTime.toJava)
      writeEvent(slice1, pid1, 101, now, "e1-101")
      writeEvent(slice2, pid2, 101, now.plusMillis(1), "e2-101")

      expect(result1.expectNext(), pid1, 101, Some("e1-101"))
      expect(result1.expectNext(), pid2, 101, Some("e2-101"))

      // backtracking events
      expect(result1.expectNext(), pid1, 101, None)
      expect(result1.expectNext(), pid2, 101, None)

      result1.cancel()
    }

    "still make initial backtracking until ahead of start offset" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val slice1 = query.sliceForPersistenceId(pid1)
      val slice2 = query.sliceForPersistenceId(pid2)
      val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)

      val startTime = InstantFactory.now().minusSeconds(60 * 60 * 24)

      writeEvent(slice1, pid1, 1, startTime.plusMillis(1), "e1-1")
      writeEvent(slice2, pid2, 1, startTime.plusMillis(2), "e2-1")
      writeEvent(slice1, pid1, 2, startTime.plusMillis(3), "e1-2")
      writeEvent(slice2, pid2, 2, startTime.plusMillis(4), "e2-2")

      (3 to 10).foreach { n =>
        writeEvent(slice1, pid1, n, startTime.plusSeconds(20 + n).plusMillis(1), s"e1-$n")
        writeEvent(slice2, pid2, n, startTime.plusSeconds(20 + n).plusMillis(2), s"e2-$n")
      }

      def startQuery(offset: Offset): TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, offset)
          .runWith(sinkProbe)
          .request(1000)

      def expect(env: EventEnvelope[String], pid: String, seqNr: Long, eventOption: Option[String]): Offset = {
        env.persistenceId shouldBe pid
        env.sequenceNr shouldBe seqNr
        if (eventOption.isEmpty)
          env.source shouldBe EnvelopeOrigin.SourceBacktracking
        else
          env.source shouldBe EnvelopeOrigin.SourceQuery
        env.eventOption shouldBe eventOption
        env.offset
      }

      val result1 = startQuery(TimestampOffset(startTime.plusSeconds(20), Map.empty))
      // from backtracking
      expect(result1.expectNext(), pid1, 1, None)
      expect(result1.expectNext(), pid2, 1, None)
      expect(result1.expectNext(), pid1, 2, None)
      expect(result1.expectNext(), pid2, 2, None)

      // from normal
      (3 to 10).foreach { n =>
        expect(result1.expectNext(), pid1, n, Some(s"e1-$n"))
        expect(result1.expectNext(), pid2, n, Some(s"e2-$n"))
      }
      // no backtracking
      result1.expectNoMessage()

      result1.cancel()
    }

    "predict backtracking filtered events based on latest seen counts" in {
      pendingIfMoreThanOneDataPartition()

      val entityType = nextEntityType()
      val pid = nextPid(entityType)
      val slice = query.sliceForPersistenceId(pid)
      val sinkProbe = TestSink[EventEnvelope[String]]()(system.classicSystem)

      // use times in the past well outside behind-current-time
      val timeZero = InstantFactory
        .now()
        .truncatedTo(ChronoUnit.SECONDS)
        .minus(settings.querySettings.backtrackingBehindCurrentTime.toJava)
        .minusSeconds(10)

      // events around the buffer size (of 10) will share the same timestamp
      // to test tracking of seen events that will be filtered on the next cycle

      val numberOfCycles = 5 // loop through the grouped events this many times
      val maxGroupSize = 7 // max size for groups of events that share the same timestamp
      val eventsPerCycle = maxGroupSize * (maxGroupSize + 1) / 2 // each cycle has groups of size 1 to maxGroupSize
      val numberOfEvents = numberOfCycles * eventsPerCycle // total number of events that will be generated

      // generate a timestamp pattern like 1, 2, 2, 4, 4, 4, 7, 7, 7, 7, 11, 11, 11, 11, 11, ...
      for (cycle <- 1 to numberOfCycles) {
        for (groupSize <- 1 to maxGroupSize) {
          for (shift <- 1 to groupSize) {
            val seqNr = (cycle - 1) * eventsPerCycle + (groupSize - 1) * groupSize / 2 + shift
            val eventTime = seqNr - shift + 1
            writeEvent(slice, pid, seqNr, timeZero.plusMillis(eventTime), s"event-$pid-$seqNr")
          }
        }
      }

      // start the query with a latest timestamp ahead of all events, to only run in backtracking mode
      val latestOffset = TimestampOffset(timeZero.plusMillis(numberOfEvents + 1), Map.empty)

      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, latestOffset)
          .runWith(sinkProbe)
          .request(numberOfEvents)

      // test that backtracking queries continued through all the written events, catching up with latest offset
      for (seqNr <- 1 to numberOfEvents) {
        val envelope = result.expectNext()
        envelope.persistenceId shouldBe pid
        envelope.sequenceNr shouldBe seqNr
        envelope.source shouldBe EnvelopeOrigin.SourceBacktracking
      }
    }

  }

}
