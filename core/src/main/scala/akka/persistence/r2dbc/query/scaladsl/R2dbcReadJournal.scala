/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import scala.collection.immutable

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
}

final class R2dbcReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery {
  private val log = LoggerFactory.getLogger(classOf[R2dbcReadJournal])
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = new R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  import settings.maxNumberOfSlices
  private val typedSystem = system.toTyped
  private val serialization = SerializationExtension(system)
  private val queryDao =
    new QueryDao(settings, ConnectionFactoryProvider(typedSystem).connectionFactoryFor(sharedConfigPath))(
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
    val timestampOffset = toTimestampOffset(offset)
    if (log.isDebugEnabled())
      log.debugN("Query slices [{} - {}], from time [{}].", minSlice, maxSlice, timestampOffset.timestamp)

    queryDao
      .eventsBySlices(entityTypeHint, minSlice, maxSlice, timestampOffset.timestamp)
      .statefulMapConcat(deserializeAndAddOffset(timestampOffset))
      .mapMaterializedValue(_ => NotUsed)
  }

  def eventsBySlices(
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope, NotUsed] = {
    val initialOffset = toTimestampOffset(offset)

    def nextOffset(previousOffset: TimestampOffset, eventEnvelope: EventEnvelope): TimestampOffset =
      eventEnvelope.offset.asInstanceOf[TimestampOffset]

    ContinuousQuery[TimestampOffset, EventEnvelope](
      initialOffset,
      nextOffset,
      offset => Some(currentEventsBySlices(entityTypeHint, minSlice, maxSlice, offset)),
      1, // the same row comes back and is filtered due to how the offset works
      settings.querySettings.refreshInterval)
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
