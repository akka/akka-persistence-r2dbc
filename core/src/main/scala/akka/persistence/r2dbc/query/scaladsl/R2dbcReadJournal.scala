/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl._
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.TimestampOffset
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object R2dbcReadJournal {
  val Identifier = "akka.persistence.r2dbc.query"

  private final case class ByPersistenceIdState(queryCount: Int, rowCount: Int, latestSeqNr: Long)

  private final case class PersistenceIdsQueryState(queryCount: Int, rowCount: Int, latestPid: String)
}

final class R2dbcReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with EventTimestampQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentPersistenceIdsQuery {
  import R2dbcReadJournal.ByPersistenceIdState
  import R2dbcReadJournal.PersistenceIdsQueryState

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))

  import settings.maxNumberOfSlices

  private val typedSystem = system.toTyped
  import typedSystem.executionContext
  private val serialization = SerializationExtension(system)
  private val connectionFactory = ConnectionFactoryProvider(typedSystem)
    .connectionFactoryFor(sharedConfigPath + ".connection-factory")
  private val queryDao =
    new QueryDao(settings, connectionFactory)(typedSystem.executionContext, typedSystem)

  private val bySlice: BySliceQuery[SerializedJournalRow, EventEnvelope] = {
    val createEnvelope: (TimestampOffset, SerializedJournalRow) => EventEnvelope = (offset, row) => {
      val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get
      val envelope = EventEnvelope(offset, row.persistenceId, row.seqNr, payload, row.dbTimestamp.toEpochMilli)
      row.metadata match {
        case None => envelope
        case Some(meta) =>
          envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
      }
    }

    val extractOffset: EventEnvelope => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(queryDao, createEnvelope, extractOffset, settings, log)(typedSystem.executionContext)
  }

  private val journalDao = new JournalDao(settings, connectionFactory)(typedSystem.executionContext, typedSystem)

  def extractEntityTypeFromPersistenceId(persistenceId: String): String =
    SliceUtils.extractEntityTypeFromPersistenceId(persistenceId)

  override def sliceForPersistenceId(persistenceId: String): Int =
    SliceUtils.sliceForPersistenceId(persistenceId, maxNumberOfSlices)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    SliceUtils.sliceRanges(numberOfRanges, maxNumberOfSlices)

  override def currentEventsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    bySlice
      .currentBySlices("currentEventsBySlices", entityType, minSlice, maxSlice, offset)
  }

  override def eventsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] =
    bySlice.liveBySlices("eventsBySlices", entityType, minSlice, maxSlice, offset)

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    val highestSeqNrFut =
      if (toSequenceNr == Long.MaxValue) journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr)
      else Future.successful(toSequenceNr)

    Source
      .futureSource[SerializedJournalRow, NotUsed] {
        highestSeqNrFut.map { highestSeqNr =>
          internalEventsByPersistenceId(persistenceId, fromSequenceNr, highestSeqNr)
        }
      }
      .map(deserializeRow)
      .mapMaterializedValue(_ => NotUsed)
  }

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[r2dbc] def internalEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {
    def updateState(state: ByPersistenceIdState, row: SerializedJournalRow): ByPersistenceIdState =
      state.copy(rowCount = state.rowCount + 1, latestSeqNr = row.seqNr)

    def nextQuery(
        state: ByPersistenceIdState,
        highestSeqNr: Long): (ByPersistenceIdState, Option[Source[SerializedJournalRow, NotUsed]]) = {
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "currentEventsByPersistenceId query [{}] for persistenceId [{}], from [{}] to [{}]. Found [{}] rows in previous query.",
            state.queryCount,
            persistenceId,
            state.latestSeqNr + 1,
            highestSeqNr,
            state.rowCount)

        newState -> Some(
          queryDao
            .eventsByPersistenceId(persistenceId, state.latestSeqNr + 1, highestSeqNr))
      } else {
        log.debug(
          "currentEventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query.",
          state.queryCount,
          persistenceId,
          state.rowCount)

        state -> None
      }
    }

    if (log.isDebugEnabled())
      log.debug(
        "currentEventsByPersistenceId query for persistenceId [{}], from [{}] to [{}].",
        persistenceId,
        fromSequenceNr,
        toSequenceNr)

    ContinuousQuery[ByPersistenceIdState, SerializedJournalRow](
      initialState = ByPersistenceIdState(0, 0, latestSeqNr = fromSequenceNr - 1),
      updateState = updateState,
      delayNextQuery = _ => None,
      nextQuery = state => nextQuery(state, toSequenceNr))
  }

  override def timestampOf(
      entityType: String,
      persistenceId: String,
      slice: Int,
      sequenceNr: Long): Future[Option[Instant]] =
    queryDao.timestampOfEvent(entityType, persistenceId, slice, sequenceNr)

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {

    log.debug("Starting eventsByPersistenceId query for persistenceId [{}], from [{}].", persistenceId, fromSequenceNr)

    def nextOffset(state: ByPersistenceIdState, row: SerializedJournalRow): ByPersistenceIdState =
      state.copy(rowCount = state.rowCount + 1, latestSeqNr = row.seqNr)

    def delayNextQuery(state: ByPersistenceIdState): Option[FiniteDuration] = {
      val delay = ContinuousQuery.adjustNextDelay(
        state.rowCount,
        settings.querySettings.bufferSize,
        settings.querySettings.refreshInterval)

      delay.foreach { d =>
        log.debug(
          "eventsByPersistenceId query [{}] for persistenceId [{}] delay next [{}] ms.",
          state.queryCount,
          persistenceId,
          d.toMillis)
      }

      delay
    }

    def nextQuery(
        state: ByPersistenceIdState): (ByPersistenceIdState, Option[Source[SerializedJournalRow, NotUsed]]) = {
      if (state.latestSeqNr >= toSequenceNr) {
        log.debug(
          "eventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query.",
          state.queryCount,
          persistenceId,
          state.rowCount)
        state -> None
      } else {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        log.debug(
          "eventsByPersistenceId query [{}] for persistenceId [{}], from [{}]. Found [{}] rows in previous query.",
          newState.queryCount,
          persistenceId,
          state.rowCount)

        newState ->
        Some(
          queryDao
            .eventsByPersistenceId(persistenceId, state.latestSeqNr + 1, toSequenceNr))
      }
    }

    ContinuousQuery[ByPersistenceIdState, SerializedJournalRow](
      initialState = ByPersistenceIdState(0, 0, latestSeqNr = fromSequenceNr - 1),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery)
      .map(deserializeRow)
  }

  def deserializeRow(row: SerializedJournalRow): EventEnvelope = {
    val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get
    val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, Map(row.persistenceId -> row.seqNr))
    val envelope = EventEnvelope(offset, row.persistenceId, row.seqNr, payload, row.dbTimestamp.toEpochMilli)
    row.metadata match {
      case None => envelope
      case Some(meta) =>
        envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
  }

  def currentPersistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] =
    queryDao.persistenceIds(afterId, limit)

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    import settings.querySettings.persistenceIdsBufferSize
    def updateState(state: PersistenceIdsQueryState, pid: String): PersistenceIdsQueryState =
      state.copy(rowCount = state.rowCount + 1, latestPid = pid)

    def nextQuery(state: PersistenceIdsQueryState): (PersistenceIdsQueryState, Option[Source[String, NotUsed]]) = {
      if (state.queryCount == 0L || state.rowCount >= persistenceIdsBufferSize) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "persistenceIds query [{}] after [{}]. Found [{}] rows in previous query.",
            state.queryCount,
            state.latestPid,
            state.rowCount)

        newState -> Some(
          queryDao
            .persistenceIds(if (state.latestPid == "") None else Some(state.latestPid), persistenceIdsBufferSize))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "persistenceIds query [{}] completed. Found [{}] rows in previous query.",
            state.queryCount,
            state.rowCount)

        state -> None
      }
    }

    ContinuousQuery[PersistenceIdsQueryState, String](
      initialState = PersistenceIdsQueryState(0, 0, ""),
      updateState = updateState,
      delayNextQuery = _ => None,
      nextQuery = state => nextQuery(state))
      .mapMaterializedValue(_ => NotUsed)
  }

}
