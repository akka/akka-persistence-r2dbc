/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.Offset
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.query.TimestampOffset
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
      QueryState(TimestampOffset.Zero, 0, 0, 0, backtracking = false, TimestampOffset.Zero)
  }

  final case class QueryState(
      latest: TimestampOffset,
      rowCount: Int,
      queryCount: Long,
      idleCount: Long,
      backtracking: Boolean,
      latestBacktracking: TimestampOffset) {

    def currentOffset: TimestampOffset =
      if (backtracking) latestBacktracking
      else latest

    def nextQueryFromTimestamp: Instant =
      if (backtracking) latestBacktracking.timestamp
      else latest.timestamp

    def nextQueryUntilTimestamp: Option[Instant] =
      if (backtracking) Some(latest.timestamp)
      else None
  }

  trait SerializedRow {
    def persistenceId: String
    def sequenceNr: Long
    def dbTimestamp: Instant
    def readDbTimestamp: Instant
  }

  trait Dao[SerializedRow] {
    def currentDbTimestamp(): Future[Instant]

    def rowsBySlices(
        entityTypeHint: String,
        minSlice: Int,
        maxSlice: Int,
        fromTimestamp: Instant,
        untilTimestamp: Option[Instant],
        behindCurrentTime: FiniteDuration): Source[SerializedRow, NotUsed]
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class BySliceQuery[Row <: BySliceQuery.SerializedRow, Envelope](
    dao: BySliceQuery.Dao[Row],
    createEnvelope: (TimestampOffset, Row) => Envelope,
    extractOffset: Envelope => TimestampOffset,
    settings: R2dbcSettings,
    log: Logger)(implicit val ec: ExecutionContext) {
  import BySliceQuery._
  import TimestampOffset.toTimestampOffset

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val firstBacktrackingQueryWindow =
    backtrackingWindow.plus(JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis))

  def currentBySlices(
      logPrefix: String,
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState =
      state.copy(latest = extractOffset(envelope), rowCount = state.rowCount + 1)

    def nextQuery(state: QueryState, toDbTimestamp: Instant): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      // FIXME why is this rowCount -1 of expected?, see test EventsBySliceSpec "read in chunks"
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize - 1) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "{} query [{}] from slices [{} - {}], from time [{}] to now [{}]. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            minSlice,
            maxSlice,
            state.latest.timestamp,
            toDbTimestamp,
            state.rowCount)

        newState -> Some(
          dao
            .rowsBySlices(
              entityTypeHint,
              minSlice,
              maxSlice,
              state.latest.timestamp,
              untilTimestamp = Some(toDbTimestamp),
              behindCurrentTime = Duration.Zero)
            .via(deserializeAndAddOffset(state.latest)))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "{} query [{}] from slices [{} - {}] completed. Found [{}] rows in previous query.",
            logPrefix,
            state.queryCount,
            minSlice,
            maxSlice,
            state.rowCount)

        state -> None
      }
    }

    Source
      .futureSource[Envelope, NotUsed] {
        dao.currentDbTimestamp().map { currentDbTime =>
          if (log.isDebugEnabled())
            log.debug(
              "{} query slices [{} - {}], from time [{}] until now [{}].",
              logPrefix,
              minSlice,
              maxSlice,
              initialOffset.timestamp,
              currentDbTime)

          ContinuousQuery[QueryState, Envelope](
            initialState = QueryState.empty.copy(latest = initialOffset),
            updateState = nextOffset,
            delayNextQuery = _ => None,
            nextQuery = state => nextQuery(state, currentDbTime))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def liveBySlices(
      logPrefix: String,
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    val someRefreshInterval = Some(settings.querySettings.refreshInterval)

    if (log.isDebugEnabled())
      log.debug(
        "Starting {} query from slices [{} - {}], from time [{}].",
        logPrefix,
        minSlice,
        maxSlice,
        initialOffset.timestamp)

    def nextOffset(state: QueryState, envelope: Envelope): QueryState = {
      val offset = extractOffset(envelope)
      if (state.backtracking) {
        if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
          throw new IllegalArgumentException(
            s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

        state.copy(latestBacktracking = offset, rowCount = state.rowCount + 1)
      } else {
        if (offset.timestamp.isBefore(state.latest.timestamp))
          throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

        state.copy(latest = offset, rowCount = state.rowCount + 1)
      }
    }

    def delayNextQuery(state: QueryState): Option[FiniteDuration] = {
      // FIXME verify that this is correct
      // the same row comes back and is filtered due to how the offset works
      val delay =
        if (0 <= state.rowCount && state.rowCount <= 1) someRefreshInterval
        else None // immediately if there might be more rows to fetch

      // TODO we could have different delays here depending on how many rows that were found.
      // e.g. a short delay if rowCount is < some threshold

      if (log.isDebugEnabled)
        delay.foreach { d =>
          log.debug(
            "{} query [{}] from slices [{} - {}] delay next [{}] ms.",
            logPrefix,
            state.queryCount,
            minSlice,
            maxSlice,
            d.toMillis)
        }

      delay
    }

    def nextQuery(state: QueryState): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      val newIdleCount = if (state.rowCount == 0) state.idleCount + 1 else 0
      val newState =
        if (settings.querySettings.backtrackingEnabled && !state.backtracking &&
          (newIdleCount >= 5 || JDuration
            .between(state.latestBacktracking.timestamp, state.latest.timestamp)
            .compareTo(halfBacktrackingWindow) > 0)) {
          // FIXME config for newIdleCount >= 5 and maybe something like `newIdleCount % 5 == 0`

          // switching to backtracking
          val fromOffset =
            if (state.latestBacktracking == TimestampOffset.Zero && initialOffset == TimestampOffset.Zero)
              TimestampOffset.Zero
            else if (state.latestBacktracking == TimestampOffset.Zero)
              TimestampOffset.Zero.copy(timestamp = initialOffset.timestamp.minus(firstBacktrackingQueryWindow))
            else
              state.latestBacktracking

          // FIXME the backtracking until offset is state.latest (not equal), but we should probably have an
          // additional lag to support async (at-least-once) projections without too many duplicates from
          // backtracking. For exactly-once I think all duplicates are filtered as expected.

          state.copy(
            rowCount = 0,
            queryCount = state.queryCount + 1,
            idleCount = newIdleCount,
            backtracking = true,
            latestBacktracking = fromOffset)
        } else if (state.backtracking && state.rowCount < settings.querySettings.bufferSize - 1) {
          // switch from backtracking
          state.copy(rowCount = 0, queryCount = state.queryCount + 1, idleCount = newIdleCount, backtracking = false)
        } else {
          state.copy(rowCount = 0, queryCount = state.queryCount + 1, idleCount = newIdleCount)
        }

      // FIXME for backtracking we could consider to use behind latest offset instead of behind current db time
      val behindCurrentTime =
        if (newState.backtracking) settings.querySettings.backtrackingBehindCurrentTime
        else settings.querySettings.behindCurrentTime

      if (log.isDebugEnabled())
        log.debug(
          "{} query [{}]{} from slices [{} - {}], from time [{}]. {}",
          logPrefix,
          newState.queryCount,
          if (newState.backtracking) " in backtracking mode" else "",
          minSlice,
          maxSlice,
          newState.nextQueryFromTimestamp,
          if (newIdleCount >= 3) s"Idle in [$newIdleCount] queries."
          else if (state.backtracking) s"Found [${state.rowCount}] rows in previous backtracking query."
          else s"Found [${state.rowCount}] rows in previous query.")

      newState ->
      Some(
        dao
          .rowsBySlices(
            entityTypeHint,
            minSlice,
            maxSlice,
            newState.nextQueryFromTimestamp,
            newState.nextQueryUntilTimestamp,
            behindCurrentTime)
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    ContinuousQuery[QueryState, Envelope](
      initialState = QueryState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery)
  }

  // TODO Unit test in isolation
  private def deserializeAndAddOffset(timestampOffset: TimestampOffset): Flow[Row, Envelope, NotUsed] = {
    Flow[Row].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      row => {
        if (row.dbTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(row.persistenceId).exists(_ >= row.sequenceNr)) {
            log.debug(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              row.persistenceId,
              row.sequenceNr,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(row.persistenceId, row.sequenceNr)
            val offset =
              TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
            createEnvelope(offset, row) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = row.dbTimestamp
          currentSequenceNrs = Map(row.persistenceId -> row.sequenceNr)
          val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
          createEnvelope(offset, row) :: Nil
        }
      }
    }
  }
}
