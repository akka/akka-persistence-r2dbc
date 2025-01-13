/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Clock

import scala.collection.immutable
import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import org.slf4j.Logger

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object BySliceQuery {
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  object QueryState {
    val empty: QueryState =
      QueryState(
        latest = TimestampOffset.Zero,
        rowCount = 0,
        rowCountSinceBacktracking = 0,
        queryCount = 0,
        idleCount = 0,
        backtrackingCount = 0,
        latestBacktracking = TimestampOffset.Zero,
        latestBacktrackingSeenCount = 0,
        backtrackingExpectFiltered = 0,
        buckets = Buckets.empty,
        previous = TimestampOffset.Zero,
        previousBacktracking = TimestampOffset.Zero,
        startTimestamp = Instant.EPOCH,
        startWallClock = Instant.EPOCH,
        currentQueryWallClock = Instant.EPOCH,
        previousQueryWallClock = Instant.EPOCH,
        idleCountBeforeHeartbeat = 0)
  }

  final case class QueryState(
      latest: TimestampOffset,
      rowCount: Int,
      rowCountSinceBacktracking: Long,
      queryCount: Long,
      idleCount: Long,
      backtrackingCount: Int,
      latestBacktracking: TimestampOffset,
      latestBacktrackingSeenCount: Int,
      backtrackingExpectFiltered: Int,
      buckets: Buckets,
      previous: TimestampOffset,
      previousBacktracking: TimestampOffset,
      startTimestamp: Instant,
      startWallClock: Instant,
      currentQueryWallClock: Instant,
      previousQueryWallClock: Instant,
      idleCountBeforeHeartbeat: Long) {

    def backtracking: Boolean = backtrackingCount > 0

    def currentOffset: TimestampOffset =
      if (backtracking) latestBacktracking
      else latest

    def nextQueryFromTimestamp(backtrackingWindow: JDuration): Instant =
      if (backtracking && latest.timestamp.minus(backtrackingWindow).isAfter(latestBacktracking.timestamp))
        latest.timestamp.minus(backtrackingWindow)
      else if (backtracking)
        latestBacktracking.timestamp
      else
        latest.timestamp

    def nextQueryFromSeqNr: Option[Long] =
      if (backtracking) highestSeenSeqNr(previousBacktracking, latestBacktracking)
      else highestSeenSeqNr(previous, latest)

    def nextQueryToTimestamp(backtrackingWindow: JDuration, atLeastNumberOfEvents: Int): Option[Instant] = {
      buckets.findTimeForLimit(nextQueryFromTimestamp(backtrackingWindow), atLeastNumberOfEvents) match {
        case Some(t) =>
          if (backtracking)
            if (t.isAfter(latest.timestamp)) Some(latest.timestamp) else Some(t)
          else
            Some(t)
        case None =>
          if (backtracking) Some(latest.timestamp)
          else None
      }
    }
  }

  // only filter by highest seen seq nr when the next query is the same timestamp (or when unknown for initial queries)
  private def highestSeenSeqNr(previous: TimestampOffset, latest: TimestampOffset): Option[Long] =
    Option.when((previous == TimestampOffset.Zero || previous.timestamp == latest.timestamp) && latest.seen.nonEmpty) {
      latest.seen.values.max
    }

  object Buckets {
    type EpochSeconds = Long
    type Count = Long

    val empty = new Buckets(immutable.SortedMap.empty)
    // Note that 10 seconds is also defined in the aggregation sql in the dao, so be cautious if you change this.
    val BucketDurationSeconds = 10
    val Limit = 10000

    final case class Bucket(startTime: EpochSeconds, count: Count)
  }

  /**
   * Count of events or state changes per 10 seconds time bucket is retrieved from database (infrequently) with an
   * aggregation query. This is used for estimating an upper bound of `db_timestamp < ?` in the `eventsBySlices` and
   * `changesBySlices` database queries. It is important to reduce the result set in this way because the `LIMIT` is
   * used after sorting the rows. See issue #/178 for more background info..
   *
   * @param countByBucket
   *   Key is the epoch seconds for the start of the bucket. Value is the number of entries in the bucket.
   */
  class Buckets(countByBucket: immutable.SortedMap[Buckets.EpochSeconds, Buckets.Count]) {
    import Buckets.{ Bucket, BucketDurationSeconds, Count, EpochSeconds }

    val createdAt: Instant = InstantFactory.now()

    def findTimeForLimit(from: Instant, atLeastCounts: Int): Option[Instant] = {
      val fromEpochSeconds = from.toEpochMilli / 1000
      val iter = countByBucket.iterator.dropWhile { case (key, _) => fromEpochSeconds >= key }

      @tailrec def sumUntilFilled(key: EpochSeconds, sum: Count): (EpochSeconds, Count) = {
        if (iter.isEmpty || sum >= atLeastCounts)
          key -> sum
        else {
          val (nextKey, count) = iter.next()
          sumUntilFilled(nextKey, sum + count)
        }
      }

      val (key, sum) = sumUntilFilled(fromEpochSeconds, 0)
      if (sum >= atLeastCounts)
        Some(Instant.ofEpochSecond(key + BucketDurationSeconds))
      else
        None
    }

    // Key is the epoch seconds for the start of the bucket.
    // Value is the number of entries in the bucket.
    def add(bucketCounts: Seq[Bucket]): Buckets =
      new Buckets(countByBucket ++ bucketCounts.iterator.map { case Bucket(startTime, count) => startTime -> count })

    def clearUntil(time: Instant): Buckets = {
      val epochSeconds = time.minusSeconds(BucketDurationSeconds).toEpochMilli / 1000
      val newCountByBucket = countByBucket.dropWhile { case (key, _) => epochSeconds >= key }
      if (newCountByBucket.size == countByBucket.size)
        this
      else if (newCountByBucket.isEmpty)
        new Buckets(immutable.SortedMap(countByBucket.last)) // keep last
      else
        new Buckets(newCountByBucket)
    }

    def isEmpty: Boolean = countByBucket.isEmpty

    def size: Int = countByBucket.size

    override def toString: String = {
      s"Buckets(${countByBucket.mkString(", ")})"
    }
  }

  trait SerializedRow {
    def persistenceId: String
    def seqNr: Long
    def dbTimestamp: Instant
    def readDbTimestamp: Instant
    def source: String
  }

  trait Dao[SerializedRow] {
    def currentDbTimestamp(slice: Int): Future[Instant]

    def rowsBySlices(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        fromSeqNr: Option[Long], // for events with same timestamp as `fromTimestamp`
        toTimestamp: Option[Instant],
        behindCurrentTime: FiniteDuration,
        backtracking: Boolean): Source[SerializedRow, NotUsed]

    /**
     * For Durable State we always refresh the bucket counts at the interval. For Event Sourced we know that they don't
     * change because events are append only.
     */
    def countBucketsMayChange: Boolean

    def countBuckets(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        limit: Int): Future[Seq[Bucket]]

  }
}

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class BySliceQuery[Row <: BySliceQuery.SerializedRow, Envelope](
    dao: BySliceQuery.Dao[Row],
    createEnvelope: (TimestampOffset, Row) => Envelope,
    extractOffset: Envelope => TimestampOffset,
    createHeartbeat: Instant => Option[Envelope],
    clock: Clock,
    settings: R2dbcSettings,
    log: Logger)(implicit val ec: ExecutionContext) {
  import BySliceQuery._
  import TimestampOffset.toTimestampOffset

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val backtrackingBehindCurrentTime =
    JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis)
  private val firstBacktrackingQueryWindow = backtrackingWindow.plus(backtrackingBehindCurrentTime)
  private val eventBucketCountInterval = JDuration.ofSeconds(60)

  def currentBySlices(
      logPrefix: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      if (EnvelopeOrigin.isHeartbeatEvent(envelope))
        state
      else
        state.copy(latest = extractOffset(envelope), rowCount = state.rowCount + 1)
    }

    def nextQuery(state: QueryState, endTimestamp: Instant): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      // Note that we can't know how many events with the same timestamp that are filtered out
      // so continue until rowCount is 0. That means an extra query at the end to make sure there are no
      // more to fetch.
      if (state.queryCount == 0L || state.rowCount > 0) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1, previous = state.latest)

        val fromTimestamp = state.latest.timestamp
        val fromSeqNr = highestSeenSeqNr(state.previous, state.latest)

        val toTimestamp = newState.nextQueryToTimestamp(backtrackingWindow, settings.querySettings.bufferSize) match {
          case Some(t) =>
            if (t.isBefore(endTimestamp)) t else endTimestamp
          case None =>
            endTimestamp
        }

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "{} next query [{}], between time [{} - {}]. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            fromTimestamp,
            toTimestamp,
            state.rowCount)

        newState -> Some(
          dao
            .rowsBySlices(
              entityType,
              minSlice,
              maxSlice,
              fromTimestamp,
              fromSeqNr,
              toTimestamp = Some(toTimestamp),
              behindCurrentTime = Duration.Zero,
              backtracking = false)
            .filter { row =>
              filterEventsBeforeSnapshots(row.persistenceId, row.seqNr, row.source)
            }
            .via(deserializeAndAddOffset(state.latest)))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "{} query [{}] completed. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            state.rowCount)

        state -> None
      }
    }

    val currentTimestamp =
      if (settings.useAppTimestamp) Future.successful(InstantFactory.now())
      else dao.currentDbTimestamp(minSlice)

    Source
      .futureSource[Envelope, NotUsed] {
        currentTimestamp.map { currentTime =>
          if (log.isDebugEnabled())
            log.debug("{} query, from time [{}] until now [{}].", logPrefix, initialOffset.timestamp, currentTime)

          ContinuousQuery[QueryState, Envelope](
            initialState = QueryState.empty.copy(latest = initialOffset),
            updateState = nextOffset,
            delayNextQuery = _ => None,
            nextQuery = state => nextQuery(state, currentTime),
            beforeQuery = beforeQuery(logPrefix, entityType, minSlice, maxSlice, _))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def liveBySlices(
      logPrefix: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      filterEventsBeforeSnapshots: (String, Long, String) => Boolean = (_, _, _) => true): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    if (log.isDebugEnabled())
      log.debug("{} starting query from time [{}].", logPrefix, initialOffset.timestamp)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      if (EnvelopeOrigin.isHeartbeatEvent(envelope))
        state
      else {
        val offset = extractOffset(envelope)
        if (state.backtracking) {
          if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
            throw new IllegalArgumentException(
              s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

          val newSeenCount =
            if (offset.timestamp == state.latestBacktracking.timestamp &&
              highestSeenSeqNr(state.previousBacktracking, offset) ==
                highestSeenSeqNr(state.previousBacktracking, state.latestBacktracking))
              state.latestBacktrackingSeenCount + 1
            else 1

          state.copy(
            latestBacktracking = offset,
            latestBacktrackingSeenCount = newSeenCount,
            rowCount = state.rowCount + 1)

        } else {
          if (offset.timestamp.isBefore(state.latest.timestamp))
            throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

          state.copy(latest = offset, rowCount = state.rowCount + 1)
        }
      }
    }

    def delayNextQuery(state: QueryState): Option[FiniteDuration] = {
      if (switchFromBacktracking(state)) {
        // switch from backtracking immediately
        None
      } else {
        val delay = ContinuousQuery.adjustNextDelay(
          state.rowCount,
          settings.querySettings.bufferSize,
          settings.querySettings.refreshInterval)

        if (log.isDebugEnabled)
          delay.foreach { d =>
            log.debug("{} query [{}] delay next [{}] ms.", logPrefix, state.queryCount, d.toMillis)
          }

        delay
      }
    }

    def switchFromBacktracking(state: QueryState): Boolean = {
      state.backtracking && state.rowCount < settings.querySettings.bufferSize - state.backtrackingExpectFiltered
    }

    def switchToBacktracking(state: QueryState, newIdleCount: Long): Boolean = {
      // Note that when starting the query with offset = NoOffset it will switch to backtracking immediately after
      // the first normal query because between(latestBacktracking.timestamp, latest.timestamp) > halfBacktrackingWindow

      val qSettings = settings.querySettings

      def disableBacktrackingWhenFarBehindCurrentWallClockTime: Boolean = {
        val aheadOfInitial =
          initialOffset == TimestampOffset.Zero ||
          state.latestBacktracking.timestamp.compareTo(initialOffset.timestamp) >= 0

        val previousTimestamp =
          if (state.previous == TimestampOffset.Zero) state.latest.timestamp else state.previous.timestamp

        aheadOfInitial &&
        previousTimestamp.isBefore(clock.instant().minus(firstBacktrackingQueryWindow))
      }

      qSettings.backtrackingEnabled &&
      !state.backtracking &&
      state.latest != TimestampOffset.Zero &&
      !disableBacktrackingWhenFarBehindCurrentWallClockTime &&
      (newIdleCount >= 5 ||
      state.rowCountSinceBacktracking + state.rowCount >= qSettings.bufferSize * 3 ||
      JDuration
        .between(state.latestBacktracking.timestamp, state.latest.timestamp)
        .compareTo(halfBacktrackingWindow) > 0)
    }

    def nextQuery(state: QueryState): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      val newIdleCount = if (state.rowCount == 0) state.idleCount + 1 else 0
      val newIdleCountBeforeHeartbeat =
        if (state.backtracking) state.idleCountBeforeHeartbeat
        else if (state.rowCount == 0) state.idleCountBeforeHeartbeat + 1
        else 0
      // only start tracking query wall clock (for heartbeats) after initial backtracking query
      val newQueryWallClock = if (state.latestBacktracking != TimestampOffset.Zero) clock.instant() else Instant.EPOCH
      val newState =
        if (switchToBacktracking(state, newIdleCount)) {
          // switching to backtracking
          val fromOffset =
            if (state.latestBacktracking == TimestampOffset.Zero)
              TimestampOffset(timestamp = state.latest.timestamp.minus(firstBacktrackingQueryWindow), seen = Map.empty)
            else
              state.latestBacktracking

          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 1,
            latestBacktracking = fromOffset,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        } else if (switchFromBacktracking(state)) {
          // switching from backtracking
          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = 0,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        } else {
          // continue
          val newBacktrackingCount = if (state.backtracking) state.backtrackingCount + 1 else 0
          state.copy(
            rowCount = 0,
            rowCountSinceBacktracking = state.rowCountSinceBacktracking + state.rowCount,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtrackingCount = newBacktrackingCount,
            backtrackingExpectFiltered = state.latestBacktrackingSeenCount,
            currentQueryWallClock = newQueryWallClock,
            previousQueryWallClock = state.currentQueryWallClock,
            idleCountBeforeHeartbeat = newIdleCountBeforeHeartbeat)
        }

      val behindCurrentTime =
        if (newState.backtracking) settings.querySettings.backtrackingBehindCurrentTime
        else settings.querySettings.behindCurrentTime

      val fromTimestamp = newState.nextQueryFromTimestamp(backtrackingWindow)
      val fromSeqNr = newState.nextQueryFromSeqNr
      val toTimestamp = newState.nextQueryToTimestamp(backtrackingWindow, settings.querySettings.bufferSize)

      if (log.isDebugEnabled()) {
        val backtrackingInfo =
          if (newState.backtracking && !state.backtracking)
            s" switching to backtracking mode, [${state.rowCountSinceBacktracking + state.rowCount}] events behind,"
          else if (!newState.backtracking && state.backtracking)
            " switching from backtracking mode,"
          else if (newState.backtracking && state.backtracking)
            " in backtracking mode,"
          else
            ""
        log.debug(
          "{} next query [{}]{}, between time [{} - {}]. {}",
          logPrefix,
          newState.queryCount,
          backtrackingInfo,
          fromTimestamp,
          toTimestamp.getOrElse("None"),
          if (newIdleCount >= 3) s"Idle in [$newIdleCount] queries."
          else if (state.backtracking) s"Found [${state.rowCount}] rows in previous backtracking query."
          else s"Found [${state.rowCount}] rows in previous query.")
      }

      val newStateWithPrevious =
        if (newState.backtracking) newState.copy(previousBacktracking = newState.latestBacktracking)
        else newState.copy(previous = newState.latest)

      newStateWithPrevious ->
      Some(
        dao
          .rowsBySlices(
            entityType,
            minSlice,
            maxSlice,
            fromTimestamp,
            fromSeqNr,
            toTimestamp,
            behindCurrentTime,
            backtracking = newState.backtracking)
          .filter { row =>
            filterEventsBeforeSnapshots(row.persistenceId, row.seqNr, row.source)
          }
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    def heartbeat(state: QueryState): Option[Envelope] = {
      if (state.idleCountBeforeHeartbeat >= 2 && state.previousQueryWallClock != Instant.EPOCH) {
        // using wall clock to measure duration since the start time (database timestamp) up to idle backtracking limit
        val timestamp = state.startTimestamp.plus(
          JDuration.between(state.startWallClock, state.previousQueryWallClock.minus(backtrackingBehindCurrentTime)))
        createHeartbeat(timestamp)
      } else
        None
    }

    val nextHeartbeat: QueryState => Option[Envelope] =
      if (settings.journalPublishEvents) heartbeat else _ => None

    val currentTimestamp =
      if (settings.useAppTimestamp) Future.successful(InstantFactory.now())
      else dao.currentDbTimestamp(minSlice)

    Source
      .futureSource[Envelope, NotUsed] {
        currentTimestamp.map { currentTime =>
          val currentWallClock = clock.instant()
          ContinuousQuery[QueryState, Envelope](
            initialState = QueryState.empty
              .copy(latest = initialOffset, startTimestamp = currentTime, startWallClock = currentWallClock),
            updateState = nextOffset,
            delayNextQuery = delayNextQuery,
            nextQuery = nextQuery,
            beforeQuery = beforeQuery(logPrefix, entityType, minSlice, maxSlice, _),
            heartbeat = nextHeartbeat)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def beforeQuery(
      logPrefix: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      state: QueryState): Option[Future[QueryState]] = {
    // Don't run this too frequently
    if ((state.buckets.isEmpty || JDuration
        .between(state.buckets.createdAt, InstantFactory.now())
        .compareTo(eventBucketCountInterval) > 0) &&
      // For Durable State we always refresh the bucket counts at the interval. For Event Sourced we know
      // that they don't change because events are append only.
      (dao.countBucketsMayChange || state.buckets
        .findTimeForLimit(state.latest.timestamp, settings.querySettings.bufferSize)
        .isEmpty)) {

      val fromTimestamp =
        if (state.latestBacktracking.timestamp == Instant.EPOCH && state.latest.timestamp == Instant.EPOCH)
          Instant.EPOCH
        else if (state.latestBacktracking.timestamp == Instant.EPOCH)
          state.latest.timestamp.minus(firstBacktrackingQueryWindow)
        else
          state.latestBacktracking.timestamp

      val futureState =
        dao.countBuckets(entityType, minSlice, maxSlice, fromTimestamp, Buckets.Limit).map { counts =>
          val newBuckets = state.buckets.clearUntil(fromTimestamp).add(counts)
          val newState = state.copy(buckets = newBuckets)
          if (log.isDebugEnabled) {
            val sum = counts.iterator.map { case Bucket(_, count) => count }.sum
            log.debug(
              "{} retrieved [{}] event count buckets, with a total of [{}], from time [{}]",
              logPrefix,
              counts.size,
              sum,
              fromTimestamp)
          }
          newState
        }
      Some(futureState)
    } else {
      // already enough buckets or retrieved recently
      None
    }
  }

  // TODO Unit test in isolation
  private def deserializeAndAddOffset(timestampOffset: TimestampOffset): Flow[Row, Envelope, NotUsed] = {
    Flow[Row].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      row => {
        if (row.dbTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(row.persistenceId).exists(_ >= row.seqNr)) {
            if (currentSequenceNrs.size >= settings.querySettings.bufferSize) {
              throw new IllegalStateException(
                s"Too many events stored with the same timestamp [$currentTimestamp], buffer size [${settings.querySettings.bufferSize}]")
            }
            log.trace(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              row.persistenceId,
              row.seqNr,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(row.persistenceId, row.seqNr)
            val offset =
              TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
            createEnvelope(offset, row) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = row.dbTimestamp
          currentSequenceNrs = Map(row.persistenceId -> row.seqNr)
          val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
          createEnvelope(offset, row) :: Nil
        }
      }
    }
  }
}
