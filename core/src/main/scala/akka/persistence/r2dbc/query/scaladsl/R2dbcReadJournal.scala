/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl._
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.TimestampOffset
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object R2dbcReadJournal {
  val Identifier = "akka.persistence.r2dbc.query"

  private object EventsBySlicesState {
    val empty: EventsBySlicesState =
      EventsBySlicesState(TimestampOffset.Zero, 0, 0, 0, backtracking = false, TimestampOffset.Zero)
  }

  private final case class EventsBySlicesState(
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

}

final class R2dbcReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery {

  import R2dbcReadJournal.EventsBySlicesState
  import TimestampOffset.toTimestampOffset

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))

  import settings.maxNumberOfSlices

  private val typedSystem = system.toTyped
  private val serialization = SerializationExtension(system)
  private val queryDao =
    new QueryDao(
      settings,
      ConnectionFactoryProvider(typedSystem)
        .connectionFactoryFor(sharedConfigPath + ".connection-factory"))(typedSystem.executionContext, typedSystem)

  private val backtrackingWindow = JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)
  private val halfBacktrackingWindow = backtrackingWindow.dividedBy(2)
  private val firstBacktrackingQueryWindow =
    backtrackingWindow.plus(JDuration.ofMillis(settings.querySettings.backtrackingBehindCurrentTime.toMillis))

  private val bySlice: BySliceQuery[SerializedJournalRow, EventEnvelope] = {
    val createEnvelope: (TimestampOffset, SerializedJournalRow) => EventEnvelope = (offset, row) => {
      val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get
      val envelope = EventEnvelope(offset, row.persistenceId, row.sequenceNr, payload, row.timestamp)
      row.metadata match {
        case None => envelope
        case Some(meta) =>
          envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
      }
    }

    val extractOffset: EventEnvelope => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(queryDao, createEnvelope, extractOffset, settings, log)(typedSystem.executionContext)
  }

  def extractEntityTypeHintFromPersistenceId(persistenceId: String): String =
    SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)

  override def sliceForPersistenceId(persistenceId: String): Int =
    SliceUtils.sliceForPersistenceId(persistenceId, maxNumberOfSlices)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    SliceUtils.sliceRanges(numberOfRanges, maxNumberOfSlices)

  override def currentEventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    bySlice
      .currentBySlices("currentEventsBySlices", entityTypeHint, minSlice, maxSlice, offset)
  }

  override def eventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    val someRefreshInterval = Some(settings.querySettings.refreshInterval)

    if (log.isDebugEnabled())
      log.debugN(
        "Starting eventsBySlices query from slices [{} - {}], from time [{}].",
        minSlice,
        maxSlice,
        initialOffset.timestamp)

    def nextOffset(state: EventsBySlicesState, eventEnvelope: EventEnvelope): EventsBySlicesState = {
      if (state.backtracking) {
        val offset = eventEnvelope.offset.asInstanceOf[TimestampOffset]
        if (offset.timestamp.isBefore(state.latestBacktracking.timestamp))
          throw new IllegalArgumentException(
            s"Unexpected offset [$offset] before latestBacktracking [${state.latestBacktracking}].")

        state.copy(latestBacktracking = offset, rowCount = state.rowCount + 1)
      } else {
        val offset = eventEnvelope.offset.asInstanceOf[TimestampOffset]
        if (offset.timestamp.isBefore(state.latest.timestamp))
          throw new IllegalArgumentException(s"Unexpected offset [$offset] before latest [${state.latest}].")

        state.copy(latest = offset, rowCount = state.rowCount + 1)
      }
    }

    def delayNextQuery(state: EventsBySlicesState): Option[FiniteDuration] = {
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
            "eventsBySlices query [{}] from slices [{} - {}] delay next [{}] ms.",
            state.queryCount,
            minSlice,
            maxSlice,
            d.toMillis)
        }

      delay
    }

    def nextQuery(state: EventsBySlicesState): (EventsBySlicesState, Option[Source[EventEnvelope, NotUsed]]) = {
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
        log.debugN(
          "eventsBySlices query [{}]{} from slices [{} - {}], from time [{}]. {}",
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
        queryDao
          .eventsBySlices(
            entityTypeHint,
            minSlice,
            maxSlice,
            newState.nextQueryFromTimestamp,
            newState.nextQueryUntilTimestamp,
            behindCurrentTime)
          .via(deserializeAndAddOffset(newState.currentOffset)))
    }

    ContinuousQuery[EventsBySlicesState, EventEnvelope](
      initialState = EventsBySlicesState.empty.copy(latest = initialOffset),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery)
  }

  // TODO Unit test in isolation
  private def deserializeAndAddOffset(
      timestampOffset: TimestampOffset): Flow[SerializedJournalRow, EventEnvelope, NotUsed] = {
    Flow[SerializedJournalRow].statefulMapConcat { () =>
      var currentTimestamp = timestampOffset.timestamp
      var currentSequenceNrs: Map[String, Long] = timestampOffset.seen
      row => {
        def toEnvelope(offset: TimestampOffset): EventEnvelope = {
          val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get
          val envelope = EventEnvelope(offset, row.persistenceId, row.sequenceNr, payload, row.timestamp)
          row.metadata match {
            case None => envelope
            case Some(meta) =>
              envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
          }
        }

        if (row.dbTimestamp == currentTimestamp) {
          // has this already been seen?
          if (currentSequenceNrs.get(row.persistenceId).exists(_ >= row.sequenceNr)) {
            log.debugN(
              "filtering [{}] [{}] as db timestamp is the same as last offset and is in seen [{}]",
              row.persistenceId,
              row.sequenceNr,
              currentSequenceNrs)
            Nil
          } else {
            currentSequenceNrs = currentSequenceNrs.updated(row.persistenceId, row.sequenceNr)
            val offset =
              TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
            toEnvelope(offset) :: Nil
          }
        } else {
          // ne timestamp, reset currentSequenceNrs
          currentTimestamp = row.dbTimestamp
          currentSequenceNrs = Map(row.persistenceId -> row.sequenceNr)
          val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, currentSequenceNrs)
          toEnvelope(offset) :: Nil
        }
      }
    }
  }
}
