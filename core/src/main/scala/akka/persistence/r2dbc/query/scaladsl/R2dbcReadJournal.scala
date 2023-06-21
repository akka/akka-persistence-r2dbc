/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant
import java.time.{ Duration => JDuration }

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.query.internal.StartingFromSnapshotStage
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
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
    with LoadEventQuery
    with CurrentEventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdTypedQuery
    with EventsByPersistenceIdQuery
    with EventsByPersistenceIdTypedQuery
    with CurrentPersistenceIdsQuery
    with PagedPersistenceIdsQuery {
  import R2dbcReadJournal.ByPersistenceIdState
  import R2dbcReadJournal.PersistenceIdsQueryState

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  log.debug("R2DBC read journal starting up with dialect [{}]", settings.dialectName)

  private val typedSystem = system.toTyped
  import typedSystem.executionContext
  private val serialization = SerializationExtension(system)
  private val persistenceExt = Persistence(system)
  private val connectionFactory = ConnectionFactoryProvider(typedSystem)
    .connectionFactoryFor(sharedConfigPath + ".connection-factory")
  private val queryDao =
    settings.connectionFactorySettings.dialect.createQueryDao(settings, connectionFactory)(typedSystem)
  private lazy val snapshotDao =
    settings.connectionFactorySettings.dialect.createSnapshotDao(settings, connectionFactory)(typedSystem)

  private val _bySlice: BySliceQuery[SerializedJournalRow, EventEnvelope[Any]] = {
    val createEnvelope: (TimestampOffset, SerializedJournalRow) => EventEnvelope[Any] = (offset, row) => {
      val event = row.payload.map(payload => serialization.deserialize(payload, row.serId, row.serManifest).get)
      val metadata = row.metadata.map(meta => serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
      val source = if (event.isDefined) EnvelopeOrigin.SourceQuery else EnvelopeOrigin.SourceBacktracking

      new EventEnvelope(
        offset,
        row.persistenceId,
        row.seqNr,
        event,
        row.dbTimestamp.toEpochMilli,
        metadata,
        row.entityType,
        row.slice,
        filtered = false,
        source,
        tags = row.tags)
    }

    val extractOffset: EventEnvelope[Any] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(queryDao, createEnvelope, extractOffset, settings, log)(typedSystem.executionContext)
  }

  private def bySlice[Event]: BySliceQuery[SerializedJournalRow, EventEnvelope[Event]] =
    _bySlice.asInstanceOf[BySliceQuery[SerializedJournalRow, EventEnvelope[Event]]]

  private def snapshotsBySlice[Snapshot, Event](
      transformSnapshot: Snapshot => Event): BySliceQuery[SerializedSnapshotRow, EventEnvelope[Event]] = {
    val createEnvelope: (TimestampOffset, SerializedSnapshotRow) => EventEnvelope[Event] = (offset, row) => {
      val snapshot = serialization.deserialize(row.snapshot, row.serializerId, row.serializerManifest).get
      val event = transformSnapshot(snapshot.asInstanceOf[Snapshot])
      val metadata = row.metadata.map(meta =>
        serialization.deserialize(meta.payload, meta.serializerId, meta.serializerManifest).get)

      new EventEnvelope[Event](
        offset,
        row.persistenceId,
        row.seqNr,
        Option(event),
        row.dbTimestamp.toEpochMilli,
        metadata,
        row.entityType,
        row.slice,
        filtered = false,
        source = "",
        tags = Set.empty
      ) // FIXME tags would be needed for filters. We could store same tags as for the corresponding event.
    }

    val extractOffset: EventEnvelope[Event] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(snapshotDao, createEnvelope, extractOffset, settings, log)(typedSystem.executionContext)
  }

  private val journalDao =
    settings.connectionFactorySettings.dialect.createJournalDao(settings, connectionFactory)(typedSystem)

  def extractEntityTypeFromPersistenceId(persistenceId: String): String =
    PersistenceId.extractEntityType(persistenceId)

  override def sliceForPersistenceId(persistenceId: String): Int = {
    persistenceExt.sliceForPersistenceId(persistenceId)
  }

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    bySlice
      .currentBySlices("currentEventsBySlices", entityType, minSlice, maxSlice, offset)
  }

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offset is [[TimestampOffset]] and [[Offset.noOffset]].
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
   */
  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    val dbSource = bySlice[Event].liveBySlices("eventsBySlices", entityType, minSlice, maxSlice, offset, Map.empty)
    if (settings.journalPublishEvents) {
      val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
      mergeDbAndPubSubSources(dbSource, pubSubSource)
    } else
      dbSource
  }

  /**
   * Same as `eventsBySlices` but with the purpose to use snapshots as starting points and thereby reducing number of
   * events that have to be loaded. This can be useful if the consumer start from zero without any previously processed
   * offset or if it has been disconnected for a long while and its offset is far behind.
   *
   * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
   * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
   *
   * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
   */
  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    val timestampOffset = toTimestampOffset(offset)

    val snapshotSource =
      snapshotsBySlice[Snapshot, Event](transformSnapshot)
        .currentBySlices("snapshotsBySlices", entityType, minSlice, maxSlice, offset)

    Source.fromGraph(
      new StartingFromSnapshotStage[Event](
        snapshotSource,
        snapshotOffsets => {
          val initOffset =
            if (timestampOffset == TimestampOffset.Zero && snapshotOffsets.nonEmpty) {
              val minTimestamp = snapshotOffsets.valuesIterator.minBy { case (_, timestamp) => timestamp }._2
              TimestampOffset(minTimestamp, Map.empty)
            } else
              offset // FIXME not sure if we should adjust also for this case

          val dbSource =
            bySlice[Event].liveBySlices("eventsBySlices", entityType, minSlice, maxSlice, initOffset, snapshotOffsets)

          if (settings.journalPublishEvents) {
            val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
            mergeDbAndPubSubSources(dbSource, pubSubSource)
          } else
            dbSource
        }))

  }

  private def eventsBySlicesPubSubSource[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int): Source[EventEnvelope[Event], NotUsed] = {
    val pubSub = PubSub(typedSystem)
    Source
      .actorRef[EventEnvelope[Event]](
        completionMatcher = PartialFunction.empty,
        failureMatcher = PartialFunction.empty,
        bufferSize = settings.querySettings.bufferSize,
        overflowStrategy = OverflowStrategy.dropNew)
      .mapMaterializedValue { ref =>
        pubSub.eventTopics[Event](entityType, minSlice, maxSlice).foreach { topic =>
          import akka.actor.typed.scaladsl.adapter._
          topic ! Topic.Subscribe(ref.toTyped[EventEnvelope[Event]])
        }
      }
      .filter { env =>
        val slice = sliceForPersistenceId(env.persistenceId)
        minSlice <= slice && slice <= maxSlice
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def mergeDbAndPubSubSources[Event, Snapshot](
      dbSource: Source[EventEnvelope[Event], NotUsed],
      pubSubSource: Source[EventEnvelope[Event], NotUsed]) = {
    dbSource
      .mergePrioritized(pubSubSource, leftPriority = 1, rightPriority = 10)
      .via(
        skipPubSubTooFarAhead(
          settings.querySettings.backtrackingEnabled,
          JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis)))
      .via(deduplicate(settings.querySettings.deduplicateCapacity))
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def deduplicate[Event](
      capacity: Int): Flow[EventEnvelope[Event], EventEnvelope[Event], NotUsed] = {
    if (capacity == 0)
      Flow[EventEnvelope[Event]]
    else {
      val evictThreshold = (capacity * 1.1).toInt
      Flow[EventEnvelope[Event]]
        .statefulMapConcat(() => {
          // cache of seen pid/seqNr
          var seen = mutable.LinkedHashSet.empty[(String, Long)]
          env => {
            if (EnvelopeOrigin.fromBacktracking(env)) {
              // don't deduplicate from backtracking
              env :: Nil
            } else {
              val entry = env.persistenceId -> env.sequenceNr
              val result = {
                if (seen.contains(entry)) {
                  Nil
                } else {
                  seen.add(entry)
                  env :: Nil
                }
              }

              if (seen.size >= evictThreshold) {
                // weird that add modifies the instance but drop returns a new instance
                seen = seen.drop(seen.size - capacity)
              }

              result
            }
          }
        })
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def skipPubSubTooFarAhead[Event](
      enabled: Boolean,
      maxAheadOfBacktracking: JDuration): Flow[EventEnvelope[Event], EventEnvelope[Event], NotUsed] = {
    if (!enabled)
      Flow[EventEnvelope[Event]]
    else
      Flow[EventEnvelope[Event]]
        .statefulMapConcat(() => {
          // track backtracking offset
          var latestBacktracking = Instant.EPOCH
          env => {
            env.offset match {
              case t: TimestampOffset =>
                if (EnvelopeOrigin.fromBacktracking(env)) {
                  latestBacktracking = t.timestamp
                  env :: Nil
                } else if (EnvelopeOrigin.fromPubSub(env) && latestBacktracking == Instant.EPOCH) {
                  log.trace2(
                    "Dropping pubsub event for persistenceId [{}] seqNr [{}] because no event from backtracking yet.",
                    env.persistenceId,
                    env.sequenceNr)
                  Nil
                } else if (EnvelopeOrigin.fromPubSub(env) && JDuration
                    .between(latestBacktracking, t.timestamp)
                    .compareTo(maxAheadOfBacktracking) > 0) {
                  // drop from pubsub when too far ahead from backtracking
                  log.debug2(
                    "Dropping pubsub event for persistenceId [{}] seqNr [{}] because too far ahead of backtracking.",
                    env.persistenceId,
                    env.sequenceNr)
                  Nil
                } else {
                  env :: Nil
                }
              case _ =>
                env :: Nil
            }
          }
        })
  }

  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[ClassicEventEnvelope, NotUsed] =
    internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .map(deserializeRow)

  @ApiMayChange
  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .map(deserializeBySliceRow[Event])

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[r2dbc] def internalCurrentEventsByPersistenceId(
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
          log.debugN(
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
        log.debugN(
          "currentEventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query.",
          state.queryCount,
          persistenceId,
          state.rowCount)

        state -> None
      }
    }

    if (log.isDebugEnabled())
      log.debugN(
        "currentEventsByPersistenceId query for persistenceId [{}], from [{}] to [{}].",
        persistenceId,
        fromSequenceNr,
        toSequenceNr)

    val highestSeqNrFut =
      if (toSequenceNr == Long.MaxValue) journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr)
      else Future.successful(toSequenceNr)

    Source
      .futureSource[SerializedJournalRow, NotUsed] {
        highestSeqNrFut.map { highestSeqNr =>
          ContinuousQuery[ByPersistenceIdState, SerializedJournalRow](
            initialState = ByPersistenceIdState(0, 0, latestSeqNr = fromSequenceNr - 1),
            updateState = updateState,
            delayNextQuery = _ => None,
            nextQuery = state => nextQuery(state, highestSeqNr))
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    queryDao.timestampOfEvent(persistenceId, sequenceNr)
  }

  //LoadEventQuery
  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
    queryDao
      .loadEvent(persistenceId, sequenceNr)
      .map {
        case Some(row) => deserializeBySliceRow(row)
        case None =>
          throw new NoSuchElementException(
            s"Event with persistenceId [$persistenceId] and sequenceNr [$sequenceNr] not found.")
      }
  }

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[ClassicEventEnvelope, NotUsed] =
    internalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .map(deserializeRow)

  @ApiMayChange
  override def eventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    internalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr)
      .map(deserializeBySliceRow[Event])

  private def internalEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {

    log.debug("Starting eventsByPersistenceId query for persistenceId [{}], from [{}].", persistenceId, fromSequenceNr)

    def nextOffset(state: ByPersistenceIdState, row: SerializedJournalRow): ByPersistenceIdState =
      state.copy(rowCount = state.rowCount + 1, latestSeqNr = row.seqNr)

    def delayNextQuery(state: ByPersistenceIdState): Option[FiniteDuration] = {
      val delay = ContinuousQuery.adjustNextDelay(
        state.rowCount,
        settings.querySettings.bufferSize,
        settings.querySettings.refreshInterval)

      delay.foreach { d =>
        log.debugN(
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
        log.debugN(
          "eventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query.",
          state.queryCount,
          persistenceId,
          state.rowCount)
        state -> None
      } else {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        log.debugN(
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
  }

  private def deserializeBySliceRow[Event](row: SerializedJournalRow): EventEnvelope[Event] = {
    val event =
      row.payload.map(payload => serialization.deserialize(payload, row.serId, row.serManifest).get.asInstanceOf[Event])
    val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, Map(row.persistenceId -> row.seqNr))
    val metadata = row.metadata.map(meta => serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    val source = if (event.isDefined) EnvelopeOrigin.SourceQuery else EnvelopeOrigin.SourceBacktracking
    new EventEnvelope(
      offset,
      row.persistenceId,
      row.seqNr,
      event,
      row.dbTimestamp.toEpochMilli,
      metadata,
      row.entityType,
      row.slice,
      filtered = false,
      source,
      tags = row.tags)
  }

  private def deserializeRow(row: SerializedJournalRow): ClassicEventEnvelope = {
    val event = row.payload.map(payload => serialization.deserialize(payload, row.serId, row.serManifest).get)
    if (event.isEmpty)
      throw new IllegalStateException("Expected event payload to be loaded.")
    val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, Map(row.persistenceId -> row.seqNr))
    val envelope = ClassicEventEnvelope(offset, row.persistenceId, row.seqNr, event.get, row.dbTimestamp.toEpochMilli)
    row.metadata match {
      case None => envelope
      case Some(meta) =>
        envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
  }

  override def currentPersistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] =
    queryDao.persistenceIds(afterId, limit)

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
   *   The ID to start returning results from, or [[None]] to return all ids. This should be an id returned from a
   *   previous invocation of this command. Callers should not assume that ids are returned in sorted order.
   * @param limit
   *   The maximum results to return. Use Long.MaxValue to return all results. Must be greater than zero.
   * @return
   *   A source containing all the persistence ids, limited as specified.
   */
  def currentPersistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] =
    queryDao.persistenceIds(entityType, afterId, limit)

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    import settings.querySettings.persistenceIdsBufferSize
    def updateState(state: PersistenceIdsQueryState, pid: String): PersistenceIdsQueryState =
      state.copy(rowCount = state.rowCount + 1, latestPid = pid)

    def nextQuery(state: PersistenceIdsQueryState): (PersistenceIdsQueryState, Option[Source[String, NotUsed]]) = {
      if (state.queryCount == 0L || state.rowCount >= persistenceIdsBufferSize) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debugN(
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
