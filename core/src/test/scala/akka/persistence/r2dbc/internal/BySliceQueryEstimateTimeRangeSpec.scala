/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Clock
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.TimestampOffset
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object BySliceQueryEstimateTimeRangeSpec {
  final case class TestRow(persistenceId: String, seqNr: Long, dbTimestamp: Instant, readDbTimestamp: Instant)
      extends BySliceQuery.SerializedRow {
    override def source: String = EnvelopeOrigin.SourceQuery
  }

  final case class TestEnvelope(offset: TimestampOffset, persistenceId: String, seqNr: Long)

  final case class RowsQuery(
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      backtracking: Boolean,
      rowCount: Int,
      eventsAfterFromTimestamp: Int)

  /**
   * In-memory events, mimics the eventsBySlices and bucket count queries of PostgresQueryDao.
   */
  class InMemoryDao(events: Vector[TestRow], bufferSize: Int) extends BySliceQuery.Dao[TestRow] {
    val rowsQueries = new ConcurrentLinkedQueue[RowsQuery]
    val countBucketsQueries = new ConcurrentLinkedQueue[Instant]

    // index of first event with dbTimestamp >= timestamp
    private def indexFrom(timestamp: Instant): Int = {
      @tailrec def search(low: Int, high: Int): Int =
        if (low >= high) low
        else {
          val mid = (low + high) >>> 1
          if (events(mid).dbTimestamp.isBefore(timestamp)) search(mid + 1, high)
          else search(low, mid)
        }
      search(0, events.size)
    }

    override def currentDbTimestamp(slice: Int): Future[Instant] =
      Future.successful(InstantFactory.now())

    override def rowsBySlices(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        fromSeqNr: Option[Long],
        toTimestamp: Option[Instant],
        behindCurrentTime: FiniteDuration,
        backtracking: Boolean,
        correlationId: Option[String]): Source[TestRow, NotUsed] = {
      val now = InstantFactory.now()
      val fromIndex = indexFrom(fromTimestamp)
      val rows = events
        .drop(fromIndex)
        .iterator
        .filter(row => fromSeqNr.forall(seqNr => row.dbTimestamp != fromTimestamp || row.seqNr >= seqNr))
        .takeWhile(row => toTimestamp.forall(t => !row.dbTimestamp.isAfter(t)))
        .takeWhile(row =>
          behindCurrentTime == Duration.Zero || row.dbTimestamp.isBefore(now.minusMillis(behindCurrentTime.toMillis)))
        .take(bufferSize)
        .map(_.copy(readDbTimestamp = now))
        .toVector
      rowsQueries.add(RowsQuery(fromTimestamp, toTimestamp, backtracking, rows.size, events.size - fromIndex))
      Source(rows)
    }

    override def countBucketsMayChange: Boolean = false

    override def countBuckets(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        limit: Int,
        correlationId: Option[String]): Future[Seq[Bucket]] = {
      countBucketsQueries.add(fromTimestamp)
      val now = InstantFactory.now()
      val toTimestamp = Buckets.countBucketsToTimestamp(fromTimestamp, limit, now)
      val buckets = events
        .drop(indexFrom(fromTimestamp))
        .iterator
        .takeWhile(row => !row.dbTimestamp.isAfter(toTimestamp))
        .map(row => (row.dbTimestamp.getEpochSecond / Buckets.BucketDurationSeconds) * Buckets.BucketDurationSeconds)
        .foldLeft(Vector.empty[Bucket]) { (acc, startTime) =>
          if (acc.nonEmpty && acc.last.startTime == startTime)
            acc.updated(acc.size - 1, Bucket(startTime, acc.last.count + 1))
          else
            acc :+ Bucket(startTime, 1)
        }
        .take(limit)

      if (toTimestamp == now)
        Future.successful(buckets)
      else
        Future.successful(appendEmptyBucketIfLastIsMissing(buckets, toTimestamp))
    }
  }
}

class BySliceQueryEstimateTimeRangeSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with LogCapturing {
  import BySliceQueryEstimateTimeRangeSpec._

  private val settings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  private val bufferSize = settings.querySettings.bufferSize
  private val log = LoggerFactory.getLogger(getClass)

  private def everyTenSeconds(from: Instant, until: Instant): Iterator[Instant] =
    Iterator.iterate(from)(_.plusSeconds(10)).takeWhile(_.isBefore(until))

  private def createEvents(timestamps: Iterator[Instant]): Vector[TestRow] =
    timestamps.zipWithIndex.map { case (t, i) =>
      TestRow(s"TestEntity|p${i % 100}", seqNr = i / 100 + 1, t, Instant.EPOCH)
    }.toVector

  private def liveBySlices(dao: InMemoryDao, events: Vector[TestRow]): Source[TestEnvelope, NotUsed] = {
    val bySliceQuery = new BySliceQuery[TestRow, TestEnvelope](
      dao,
      createEnvelope = (offset, row) => TestEnvelope(offset, row.persistenceId, row.seqNr),
      extractOffset = _.offset,
      createHeartbeat = _ => None,
      Clock.systemUTC(),
      settings,
      log)(system.executionContext)

    val initialOffset = TimestampOffset(events.head.dbTimestamp.minusMillis(1), Map.empty)
    bySliceQuery.liveBySlices("[test]", None, "TestEntity", 0, 1023, initialOffset)
  }

  private def runUntilLastEvent(dao: InMemoryDao, events: Vector[TestRow]): Unit = {
    val last = events.last
    val done = liveBySlices(dao, events)
      .takeWhile(env => !(env.persistenceId == last.persistenceId && env.seqNr == last.seqNr), inclusive = true)
      .runWith(Sink.ignore)
    Await.result(done, 60.seconds)
  }

  private def assertBoundedQueries(dao: InMemoryDao, maxCountBucketsQueries: Int): Unit = {
    val queries = dao.rowsQueries.asScala.toVector.filterNot(_.backtracking)
    // a query without upper bound is expensive when there are many events after the fromTimestamp
    val expensive = queries.filter(q => q.toTimestamp.isEmpty && q.eventsAfterFromTimestamp > 2 * bufferSize)
    withClue(
      s"[${expensive.size}] of [${queries.size}] queries without upper bound, " +
      s"[${dao.countBucketsQueries.size}] bucket count queries: ") {
      expensive shouldBe empty
      dao.countBucketsQueries.size should be <= maxCountBucketsQueries
    }
  }

  "BySliceQuery with estimate-time-range" should {

    "use upper bound when catching up a long backlog" in {
      val endTime = InstantFactory.now().minusSeconds(3600)
      val startTime = endTime.minusSeconds(4 * 24 * 3600)
      val events = createEvents(everyTenSeconds(startTime, endTime))
      val dao = new InMemoryDao(events, bufferSize)

      runUntilLastEvent(dao, events)

      // each bucket count query covers ~28 hours, so 4 are needed
      assertBoundedQueries(dao, maxCountBucketsQueries = 6)
    }

    "use upper bound when catching up a backlog with a gap of few events" in {
      val endTime = InstantFactory.now().minusSeconds(3600)
      val gapEnd = endTime.minusSeconds(24 * 3600)
      val gapStart = gapEnd.minusSeconds(5 * 24 * 3600)
      val startTime = gapStart.minusSeconds(24 * 3600)
      val sparse = Iterator.iterate(gapStart)(_.plusSeconds(12 * 3600)).takeWhile(_.isBefore(gapEnd))
      val events =
        createEvents(everyTenSeconds(startTime, gapStart) ++ sparse ++ everyTenSeconds(gapEnd, endTime))
      val dao = new InMemoryDao(events, bufferSize)

      runUntilLastEvent(dao, events)

      // each bucket count query covers ~28 hours, so 7 are needed, also for the gap
      assertBoundedQueries(dao, maxCountBucketsQueries = 10)
    }

    "not count buckets for each query when caught up" in {
      val endTime = InstantFactory.now().minusSeconds(3600)
      val startTime = endTime.minusSeconds(2 * 24 * 3600)
      val events = createEvents(everyTenSeconds(startTime, endTime))
      val dao = new InMemoryDao(events, bufferSize)
      val last = events.last

      val (killSwitch, _) =
        liveBySlices(dao, events).viaMat(KillSwitches.single)(Keep.right).toMat(Sink.ignore)(Keep.both).run()

      try {
        eventually {
          dao.rowsQueries.asScala.exists(q => !q.backtracking && q.fromTimestamp == last.dbTimestamp) shouldBe true
        }
        val countBucketsQueriesWhenCaughtUp = dao.countBucketsQueries.size
        val rowsQueriesWhenCaughtUp = dao.rowsQueries.size
        // refresh-interval is 1s
        eventually {
          dao.rowsQueries.size should be >= rowsQueriesWhenCaughtUp + 3
        }
        dao.countBucketsQueries.size shouldBe countBucketsQueriesWhenCaughtUp
      } finally {
        killSwitch.shutdown()
      }
    }
  }

}
