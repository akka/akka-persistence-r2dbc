/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._
import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.japi.Pair
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.query.Offset
import akka.persistence.query.javadsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.{
  CurrentEventsByPersistenceIdTypedQuery,
  CurrentEventsBySliceQuery,
  EventTimestampQuery,
  EventsByPersistenceIdTypedQuery,
  EventsBySliceQuery,
  LoadEventQuery
}
import akka.persistence.r2dbc.query.scaladsl
import akka.stream.javadsl.Source

object R2dbcReadJournal {
  val Identifier: String = scaladsl.R2dbcReadJournal.Identifier
}

final class R2dbcReadJournal(delegate: scaladsl.R2dbcReadJournal)
    extends ReadJournal
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery
    with CurrentEventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with EventsByPersistenceIdQuery
    with EventsByPersistenceIdTypedQuery
    with CurrentPersistenceIdsQuery
    with PagedPersistenceIdsQuery {

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
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
      toSequenceNr: Long): Source[ClassicEventEnvelope, NotUsed] =
    delegate.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    delegate.currentEventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[ClassicEventEnvelope, NotUsed] =
    delegate.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def eventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsByPersistenceIdTyped[Event](persistenceId, fromSequenceNr, toSequenceNr).asJava

  override def currentPersistenceIds(): Source[String, NotUsed] =
    delegate.currentPersistenceIds().asJava

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] =
    delegate.currentPersistenceIds(afterId.asScala, limit).asJava

  /**
   * Get the current persistence ids.
   *
   * Note: to reuse existing index, the actual query filters entity types based on persistence_id column and sql LIKE
   * operator. Hence the persistenceId must start with an entity type followed by default separator ("|") from
   * [[akka.persistence.typed.PersistenceId]].
   *
   * @param entityType
   *   The entity type name.
   * @param afterId
   *   The ID to start returning results from, or empty to return all ids. This should be an id returned from a previous
   *   invocation of this command. Callers should not assume that ids are returned in sorted order.
   * @param limit
   *   The maximum results to return. Use Long.MAX_VALUE to return all results. Must be greater than zero.
   * @return
   *   A source containing all the persistence ids, limited as specified.
   */
  def currentPersistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] =
    delegate.currentPersistenceIds(entityType, afterId, limit).asJava

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate.timestampOf(persistenceId, sequenceNr).map(_.asJava)(ExecutionContexts.parasitic).toJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).toJava

}
