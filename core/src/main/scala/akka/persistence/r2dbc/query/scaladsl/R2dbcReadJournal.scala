/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.EventEnvelope
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl._
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.TimestampOffset
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object R2dbcReadJournal {
  val Identifier = "akka.persistence.r2dbc.query"

  private final case class EventsBySlicesState(latestOffset: TimestampOffset, rowCount: Int, queryCount: Long)

}

final class R2dbcReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery {
  import R2dbcReadJournal.EventsBySlicesState

  private val log = LoggerFactory.getLogger(classOf[R2dbcReadJournal])
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  import settings.maxNumberOfSlices
  private val typedSystem = system.toTyped
  private val serialization = SerializationExtension(system)
  private val queryDao =
    new QueryDao(
      settings,
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(sharedConfigPath + ".connection-factory"))(
      typedSystem.executionContext,
      typedSystem)

  private def toTimestampOffset(offset: Offset): TimestampOffset = {
    offset match {
      case t: TimestampOffset => t
      case NoOffset           => TimestampOffset.Zero
    }
  }

  def extractEntityTypeHintFromPersistenceId(persistenceId: String): String =
    SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)

  def sliceForPersistenceId(persistenceId: String): Int =
    SliceUtils.sliceForPersistenceId(persistenceId, maxNumberOfSlices)

  def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    SliceUtils.sliceRanges(numberOfRanges, maxNumberOfSlices)

  def currentEventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    implicit val ec: ExecutionContext = system.dispatcher

    def nextOffset(state: EventsBySlicesState, eventEnvelope: EventEnvelope): EventsBySlicesState = {
      state.copy(latestOffset = eventEnvelope.offset.asInstanceOf[TimestampOffset], rowCount = state.rowCount + 1)
    }

    def nextQuery(
        state: EventsBySlicesState,
        toDbTimestamp: Instant): (EventsBySlicesState, Option[Source[EventEnvelope, NotUsed]]) = {
      // FIXME why is this rowCount -1 of expected?, see test EventsBySliceSpec "read in chunks"
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize - 1) {

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debugN(
            "Query next from slices [{} - {}], from time [{}] to now [{}]. Found [{}] rows in previous query.",
            minSlice,
            maxSlice,
            state.latestOffset.timestamp,
            toDbTimestamp,
            state.rowCount)

        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        newState -> Some(
          queryDao
            .eventsBySlices(
              entityTypeHint,
              minSlice,
              maxSlice,
              state.latestOffset.timestamp,
              toTimestamp = Some(toDbTimestamp))
            .statefulMapConcat(deserializeAndAddOffset(state.latestOffset)))
      } else {
        if (log.isDebugEnabled)
          log.debugN(
            "Query next from slices [{} - {}], from time [{}] to now [{}] completed. Found [{}] rows in previous query.",
            minSlice,
            maxSlice,
            state.latestOffset.timestamp,
            toDbTimestamp,
            state.rowCount)
        state -> None
      }
    }

    Source
      .futureSource[EventEnvelope, NotUsed] {
        queryDao.currentDbTimestamp().map { dbTimestamp =>
          if (log.isDebugEnabled())
            log.debugN(
              "Query slices [{} - {}], from time [{}] to now [{}].",
              minSlice,
              maxSlice,
              initialOffset.timestamp,
              dbTimestamp)

          ContinuousQuery[EventsBySlicesState, EventEnvelope](
            initialState = EventsBySlicesState(initialOffset, rowCount = 0, queryCount = 0),
            updateState = nextOffset,
            delayNextQuery = state => (state, None),
            nextQuery = state => nextQuery(state, dbTimestamp))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def eventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)
    val someRefreshInterval = Some(settings.querySettings.refreshInterval)

    if (log.isDebugEnabled())
      log.debugN(
        "Starting live query from slices [{} - {}], from time [{}].",
        minSlice,
        maxSlice,
        initialOffset.timestamp)

    def nextOffset(state: EventsBySlicesState, eventEnvelope: EventEnvelope): EventsBySlicesState = {
      state.copy(latestOffset = eventEnvelope.offset.asInstanceOf[TimestampOffset], rowCount = state.rowCount + 1)
    }

    def delayNextQuery(state: EventsBySlicesState): (EventsBySlicesState, Option[FiniteDuration]) = {
      // FIXME verify that this is correct
      // the same row comes back and is filtered due to how the offset works
      val delay =
        if (0 <= state.rowCount && state.rowCount <= 1) someRefreshInterval
        else None

      state -> delay
    }

    def nextQuery(state: EventsBySlicesState): (EventsBySlicesState, Option[Source[EventEnvelope, NotUsed]]) = {
      if (log.isDebugEnabled())
        log.debugN(
          "Query next from slices [{} - {}], from time [{}].",
          minSlice,
          maxSlice,
          state.latestOffset.timestamp)

      val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

      newState ->
      Some(
        queryDao
          .eventsBySlices(entityTypeHint, minSlice, maxSlice, state.latestOffset.timestamp, toTimestamp = None)
          .statefulMapConcat(deserializeAndAddOffset(state.latestOffset)))
    }

    ContinuousQuery[EventsBySlicesState, EventEnvelope](
      initialState = EventsBySlicesState(initialOffset, rowCount = 0, queryCount = 0),
      updateState = nextOffset,
      delayNextQuery = state => delayNextQuery(state),
      nextQuery = state => nextQuery(state))
  }

  // TODO Unit test in isolation
  private def deserializeAndAddOffset(
      timestampOffset: TimestampOffset): () => SerializedJournalRow => immutable.Iterable[EventEnvelope] = { () =>
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
            TimestampOffset(row.dbTimestamp, currentSequenceNrs)
          toEnvelope(offset) :: Nil
        }
      } else {
        // ne timestamp, reset currentSequenceNrs
        currentTimestamp = row.dbTimestamp
        currentSequenceNrs = Map(row.persistenceId -> row.sequenceNr)
        val offset = TimestampOffset(row.dbTimestamp, currentSequenceNrs)
        toEnvelope(offset) :: Nil
      }
    }
  }
}
