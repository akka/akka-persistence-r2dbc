/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.scaladsl

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.EventTimestampQuery
import akka.persistence.query.scaladsl.EventTimestampQuery
import akka.persistence.query.scaladsl.EventsBySliceQuery
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

// FIXME this should be incorporated in Akka Projections

@ApiMayChange
object EventSourcedProvider2 {

  def eventsBySlices[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      entityTypeHint: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsBySlicesQuery =
      PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId)

    if (!eventsBySlicesQuery.isInstanceOf[EventTimestampQuery])
      throw new IllegalArgumentException(
        s"[${eventsBySlicesQuery.getClass.getName}] with readJournalPluginId " +
        s"[$readJournalPluginId] must implement [${classOf[EventTimestampQuery].getName}]")

    new EventsBySlicesSourceProvider(eventsBySlicesQuery, entityTypeHint, minSlice, maxSlice, system)
  }

  def sliceForPersistenceId(system: ActorSystem[_], readJournalPluginId: String, persistenceId: String): Int =
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceQuery](readJournalPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(system: ActorSystem[_], readJournalPluginId: String, numberOfRanges: Int): immutable.Seq[Range] =
    PersistenceQuery(system).readJournalFor[EventsBySliceQuery](readJournalPluginId).sliceRanges(numberOfRanges)

  private class EventsBySlicesSourceProvider[Event](
      eventsBySlicesQuery: EventsBySliceQuery,
      entityTypeHint: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      system: ActorSystem[_])
      extends SourceProvider[Offset, EventEnvelope[Event]]
      with TimestampOffsetBySlicesSourceProvider
      with EventTimestampQuery {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offset: () => Future[Option[Offset]]): Future[Source[EventEnvelope[Event], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        eventsBySlicesQuery
          .eventsBySlices(entityTypeHint, minSlice, maxSlice, offset)
          .map(env => EventEnvelope(env))
      }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: EventEnvelope[Event]): Long = envelope.timestamp

    override def timestampOf(
        entityTypeHint: String,
        persistenceId: String,
        slice: Int,
        sequenceNumber: Long): Future[Option[Instant]] =
      eventsBySlicesQuery match {
        case timestampQuery: EventTimestampQuery =>
          timestampQuery.timestampOf(entityTypeHint, persistenceId, slice, sequenceNumber)
        case _ =>
          Future.failed(
            new IllegalStateException(
              s"[${eventsBySlicesQuery.getClass.getName}] must implement [${classOf[EventTimestampQuery].getName}]"))
      }
  }
}
