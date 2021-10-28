/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant

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

    def eventsBySlices(
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

  def currentBySlices(
      logPrefix: String,
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[Envelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(state: QueryState, eventEnvelope: Envelope): QueryState =
      state.copy(latest = extractOffset(eventEnvelope), rowCount = state.rowCount + 1)

    def nextQuery(state: QueryState, toDbTimestamp: Instant): (QueryState, Option[Source[Envelope, NotUsed]]) = {
      // FIXME why is this rowCount -1 of expected?, see test EventsBySliceSpec "read in chunks"
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize - 1) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "currentEventsBySlices query [{}] from slices [{} - {}], from time [{}] to now [{}]. Found [{}] rows in previous query.",
            state.queryCount,
            minSlice,
            maxSlice,
            state.latest.timestamp,
            toDbTimestamp,
            state.rowCount)

        newState -> Some(
          dao
            .eventsBySlices(
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
