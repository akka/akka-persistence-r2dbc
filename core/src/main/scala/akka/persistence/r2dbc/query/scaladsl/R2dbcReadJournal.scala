/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import scala.collection.immutable

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl._
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
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
      offset: Offset): Source[EventEnvelope, NotUsed] =
    bySlice.liveBySlices("eventsBySlices", entityTypeHint, minSlice, maxSlice, offset)
}
