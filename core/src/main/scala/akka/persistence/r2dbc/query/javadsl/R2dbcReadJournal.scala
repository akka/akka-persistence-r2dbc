/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.javadsl

import java.util

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.EventBySliceEnvelope
import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.persistence.query.javadsl._
import akka.persistence.r2dbc.query.scaladsl
import akka.stream.javadsl.Source

object R2dbcReadJournal {
  val Identifier: String = scaladsl.R2dbcReadJournal.Identifier
}

final class R2dbcReadJournal(delegate: scaladsl.R2dbcReadJournal)
    extends ReadJournal
    with CurrentEventsBySliceQuery[Any]
    with EventsBySliceQuery[Any]
    // FIXME with javadsl.LoadEventQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentPersistenceIdsQuery {

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def currentEventsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventBySliceEnvelope[Any], NotUsed] =
    delegate.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def eventsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventBySliceEnvelope[Any], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import akka.util.ccompat.JavaConverters._
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    delegate.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    delegate.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentPersistenceIds(): Source[String, NotUsed] =
    delegate.currentPersistenceIds().asJava
}
