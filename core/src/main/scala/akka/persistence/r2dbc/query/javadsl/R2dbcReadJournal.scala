/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.query.Offset
import akka.persistence.query.javadsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.javadsl.EventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.javadsl.EventsBySliceStartingFromSnapshotsQuery
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
    with CurrentEventsBySliceStartingFromSnapshotsQuery
    with EventsBySliceStartingFromSnapshotsQuery
    with EventTimestampQuery
    with LoadEventQuery
    with CurrentEventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with EventsByPersistenceIdQuery
    with EventsByPersistenceIdTypedQuery
    with CurrentPersistenceIdsQuery
    with PagedPersistenceIdsQuery
    with EventsByPersistenceIdStartingFromSnapshotQuery
    with CurrentEventsByPersistenceIdStartingFromSnapshotQuery {

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offset is [[akka.persistence.query.TimestampOffset]] and [[Offset.noOffset]].
   *
   * The timestamp is based on the database `CURRENT_TIMESTAMP` when the event was stored. `CURRENT_TIMESTAMP` is the
   * time when the transaction started, not when it was committed. This means that a "later" event may be visible first
   * and when retrieving events after the previously seen timestamp we may miss some events. In distributed SQL
   * databases there can also be clock skews for the database timestamps. For that reason it will perform additional
   * backtracking queries to catch missed events. Events from backtracking will typically be duplicates of previously
   * emitted events. It's the responsibility of the consumer to filter duplicates and make sure that events are
   * processed in exact sequence number order for each persistence id. Such deduplication is provided by the R2DBC
   * Projection.
   *
   * Events emitted by the backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the
   * consumer can load the full `EventBySliceEnvelope` with [[R2dbcReadJournal.loadEnvelope]].
   *
   * The events will be emitted in the timestamp order with the caveat of duplicate events as described above. Events
   * with the same timestamp are ordered by sequence number.
   *
   * The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
   * events when new events are persisted. Corresponding query that is completed when it reaches the end of the
   * currently stored events is provided by [[R2dbcReadJournal.currentEventsBySlices]].
   *
   * The slice range cannot span over more than one data partition, which in practise means that the number of
   * Projection instances must be be greater than or equal to the number of data partitions. For example, with 4 data
   * partitions the slice range (0 - 255) is allowed but not (0 - 511). Smaller slice range such as (0 - 127) is also
   * allowed.
   */
  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  /**
   * Same as `currentEventsBySlices` but with the purpose to use snapshots as starting points and thereby reducing
   * number of events that have to be loaded. This can be useful if the consumer start from zero without any previously
   * processed offset or if it has been disconnected for a long while and its offset is far behind.
   *
   * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
   * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
   *
   * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
   *
   * To use `currentEventsBySlicesStartingFromSnapshots` you must enable configuration
   * `akka.persistence.r2dbc.query.start-from-snapshot.enabled` and follow instructions in migration guide
   * https://doc.akka.io/libraries/akka-persistence-r2dbc/current/migration-guide.html#eventsBySlicesStartingFromSnapshots
   */
  override def currentEventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate
      .currentEventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot(_))
      .asJava

  /**
   * Same as `eventsBySlices` but with the purpose to use snapshots as starting points and thereby reducing number of
   * events that have to be loaded. This can be useful if the consumer start from zero without any previously processed
   * offset or if it has been disconnected for a long while and its offset is far behind.
   *
   * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
   * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
   *
   * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
   *
   * To use `eventsBySlicesStartingFromSnapshots` you must enable configuration
   * `akka.persistence.r2dbc.query.start-from-snapshot.enabled` and follow instructions in migration guide
   * https://doc.akka.io/libraries/akka-persistence-r2dbc/current/migration-guide.html#eventsBySlicesStartingFromSnapshots
   */
  override def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot(_)).asJava

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import scala.jdk.CollectionConverters._
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

  override def currentEventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate
      .currentEventsByPersistenceIdStartingFromSnapshot(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        transformSnapshot(_))
      .asJava

  override def eventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      transformSnapshot: java.util.function.Function[Snapshot, Event]): Source[EventEnvelope[Event], NotUsed] =
    delegate
      .eventsByPersistenceIdStartingFromSnapshot(persistenceId, fromSequenceNr, toSequenceNr, transformSnapshot(_))
      .asJava

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

  /**
   * Load the last event for the given `persistenceId` up to the given `toSeqNr`.
   *
   * @param persistenceId
   *   The persistence id to load the last event for.
   * @param toSeqNr
   *   The sequence number to load the last event up to.
   * @param includeDeleted
   *   Whether to include deleted events.
   */
  def loadLastEvent(
      persistenceId: String,
      toSeqNr: Long,
      includeDeleted: Boolean): CompletionStage[Option[ClassicEventEnvelope]] = {
    delegate.loadLastEvent(persistenceId, toSeqNr, includeDeleted).asJava
  }

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate.timestampOf(persistenceId, sequenceNr).map(_.toJava)(ExecutionContext.parasitic).asJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).asJava

}
