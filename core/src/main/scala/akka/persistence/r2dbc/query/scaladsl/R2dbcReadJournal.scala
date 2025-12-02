/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Clock
import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.SerializedEvent
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.query.Offset
import akka.persistence.query.QueryCorrelationId
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.CurrentPersistenceIdsForEntityTypeQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.scaladsl.EventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.LatestEventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.CorrelationId
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.internal.StartingFromSnapshotStage
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

  // Caching for LatestEventTimestampQuery

  private final case class EntitySliceRange(entityType: String, minSlice: Int, maxSlice: Int)

  private final case class CachedTimestamp(timestamp: Option[Instant], cachedAt: Instant)

}

final class R2dbcReadJournal(system: ExtendedActorSystem, config: Config, cfgPath: String)
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
    with CurrentEventsByPersistenceIdStartingFromSnapshotQuery
    with LatestEventTimestampQuery
    with CurrentPersistenceIdsForEntityTypeQuery {
  import R2dbcReadJournal.ByPersistenceIdState
  import R2dbcReadJournal.PersistenceIdsQueryState
  import R2dbcReadJournal.EntitySliceRange
  import R2dbcReadJournal.CachedTimestamp

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.query$""", "")
  private val settings = R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  log.debug("R2DBC read journal starting up with dialect [{}]", settings.dialectName)

  private val typedSystem = system.toTyped
  import typedSystem.executionContext
  private val serialization = SerializationExtension(system)
  private val persistenceExt = Persistence(system)
  private val executorProvider =
    new R2dbcExecutorProvider(
      typedSystem,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, typedSystem),
      settings,
      sharedConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))
  private val journalDao =
    settings.connectionFactorySettings.dialect.createJournalDao(executorProvider)
  private val queryDao =
    settings.connectionFactorySettings.dialect.createQueryDao(executorProvider)
  private lazy val snapshotDao =
    settings.connectionFactorySettings.dialect.createSnapshotDao(executorProvider)

  private val filteredPayloadSerId = SerializationExtension(system).findSerializerFor(FilteredPayload).identifier

  // key is tuple of entity type and slice
  private val heartbeatPersistenceIds = new ConcurrentHashMap[(String, Int), String]()
  private val heartbeatUuid = UUID.randomUUID().toString
  log.debug("Using heartbeat UUID [{}]", heartbeatUuid)

  // Optional caching of latestEventTimestamp results
  private val latestEventTimestampCache = new ConcurrentHashMap[EntitySliceRange, CachedTimestamp]()

  private def heartbeatPersistenceId(entityType: String, slice: Int): String = {
    val key = entityType -> slice
    heartbeatPersistenceIds.get(key) match {
      case null =>
        // no need to block other threads, it's just a cache
        val pid = generateHeartbeatPersistenceId(entityType, slice)
        heartbeatPersistenceIds.put(key, pid)
        pid
      case pid => pid
    }
  }

  @tailrec private def generateHeartbeatPersistenceId(entityType: String, slice: Int, n: Int = 1): String = {
    if (n < 1000000) {
      // including a uuid to make sure it is not the same as any persistence id of the application
      val pid = PersistenceId.concat(entityType, s"_hb-$heartbeatUuid-$n")
      if (persistenceExt.sliceForPersistenceId(pid) == slice)
        pid
      else
        generateHeartbeatPersistenceId(entityType, slice, n + 1)
    } else
      throw new IllegalStateException(s"Couldn't find heartbeat persistenceId for [$entityType] with slice [$slice]")

  }

  private def deserializePayload[Event](row: SerializedJournalRow): Option[Event] =
    row.payload.map(payload => serialization.deserialize(payload, row.serId, row.serManifest).get.asInstanceOf[Event])

  private val clock = Clock.systemUTC()

  private def bySlice[Event](
      entityType: String,
      minSlice: Int): BySliceQuery[SerializedJournalRow, EventEnvelope[Event]] = {
    val createEnvelope: (TimestampOffset, SerializedJournalRow) => EventEnvelope[Event] = createEventEnvelope

    val extractOffset: EventEnvelope[Event] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    val createHeartbeat: Instant => Option[EventEnvelope[Event]] = { timestamp =>
      Some(createEventEnvelopeHeartbeat(entityType, minSlice, timestamp))
    }

    new BySliceQuery(queryDao, createEnvelope, extractOffset, createHeartbeat, clock, settings, log)(
      typedSystem.executionContext)
  }

  private def deserializeBySliceRow[Event](row: SerializedJournalRow): EventEnvelope[Event] = {
    val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, Map(row.persistenceId -> row.seqNr))
    createEventEnvelope(offset, row)
  }

  private def createEventEnvelope[Event](offset: TimestampOffset, row: SerializedJournalRow): EventEnvelope[Event] = {
    val event = deserializePayload(row)
    val metadata = row.metadata.map(meta => serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    val source = if (event.isDefined) EnvelopeOrigin.SourceQuery else EnvelopeOrigin.SourceBacktracking
    val filtered = row.serId == filteredPayloadSerId

    new EventEnvelope(
      offset,
      row.persistenceId,
      row.seqNr,
      if (filtered) None else event,
      row.dbTimestamp.toEpochMilli,
      metadata,
      row.entityType,
      row.slice,
      filtered,
      source,
      tags = row.tags)
  }

  def createEventEnvelopeHeartbeat[Event](entityType: String, slice: Int, timestamp: Instant): EventEnvelope[Event] = {
    new EventEnvelope(
      TimestampOffset(timestamp, Map.empty),
      heartbeatPersistenceId(entityType, slice),
      1L,
      eventOption = None,
      timestamp.toEpochMilli,
      _eventMetadata = None,
      entityType,
      slice,
      filtered = true,
      source = EnvelopeOrigin.SourceHeartbeat,
      Set.empty)
  }

  private def deserializeRow(row: SerializedJournalRow): ClassicEventEnvelope = {
    val event = deserializePayload(row)
    // note that it's not possible to filter out FilteredPayload here
    val offset = TimestampOffset(row.dbTimestamp, row.readDbTimestamp, Map(row.persistenceId -> row.seqNr))
    val envelope = ClassicEventEnvelope(offset, row.persistenceId, row.seqNr, event.get, row.dbTimestamp.toEpochMilli)
    row.metadata match {
      case None => envelope
      case Some(meta) =>
        envelope.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
  }

  private def snapshotsBySlice[Snapshot, Event](
      entityType: String,
      minSlice: Int,
      transformSnapshot: Snapshot => Event): BySliceQuery[SerializedSnapshotRow, EventEnvelope[Event]] = {
    val createEnvelope: (TimestampOffset, SerializedSnapshotRow) => EventEnvelope[Event] =
      (offset, row) => createEnvelopeFromSnapshot(row, offset, transformSnapshot)

    val extractOffset: EventEnvelope[Event] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    val createHeartbeat: Instant => Option[EventEnvelope[Event]] = { timestamp =>
      Some(createEventEnvelopeHeartbeat(entityType, minSlice, timestamp).asInstanceOf[EventEnvelope[Event]])
    }

    new BySliceQuery(snapshotDao, createEnvelope, extractOffset, createHeartbeat, clock, settings, log)(
      typedSystem.executionContext)
  }

  private def createEnvelopeFromSnapshot[Snapshot, Event](
      row: SerializedSnapshotRow,
      offset: TimestampOffset,
      transformSnapshot: Snapshot => Event): EventEnvelope[Event] = {
    val snapshot = serialization.deserialize(row.snapshot, row.serializerId, row.serializerManifest).get
    val event = transformSnapshot(snapshot.asInstanceOf[Snapshot])
    val metadata =
      row.metadata.map(meta => serialization.deserialize(meta.payload, meta.serializerId, meta.serializerManifest).get)

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
      source = EnvelopeOrigin.SourceSnapshot,
      tags = row.tags)
  }

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
    val correlationId = QueryCorrelationId.get()
    val correlationIdText = CorrelationId.toLogText(correlationId)
    bySlice(entityType, minSlice)
      .currentBySlices(
        s"[$entityType] currentEventsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
        correlationId,
        entityType,
        minSlice,
        maxSlice,
        offset)
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
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    val correlationId = QueryCorrelationId.get()
    val correlationIdText = CorrelationId.toLogText(correlationId)
    val dbSource =
      bySlice[Event](entityType, minSlice).liveBySlices(
        s"[$entityType] eventsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
        correlationId,
        entityType,
        minSlice,
        maxSlice,
        offset)
    if (settings.journalPublishEvents) {
      val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
      mergeDbAndPubSubSources(dbSource, pubSubSource, correlationId)
    } else
      dbSource
  }

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
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("currentEventsBySlicesStartingFromSnapshots")
    val timestampOffset = toTimestampOffset(offset)
    val correlationId = QueryCorrelationId.get()
    val correlationIdText = CorrelationId.toLogText(correlationId)
    val snapshotSource =
      snapshotsBySlice[Snapshot, Event](entityType, minSlice, transformSnapshot)
        .currentBySlices(
          s"[$entityType] currentSnapshotsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
          correlationId,
          entityType,
          minSlice,
          maxSlice,
          offset)

    Source.fromGraph(
      new StartingFromSnapshotStage[Event](
        snapshotSource,
        { snapshotOffsets =>
          val initOffset =
            if (timestampOffset == TimestampOffset.Zero && snapshotOffsets.nonEmpty) {
              val minTimestamp = snapshotOffsets.valuesIterator.minBy { case (_, timestamp) => timestamp }._2
              TimestampOffset(minTimestamp, Map.empty)
            } else {
              // don't adjust because then there is a risk that there was no found snapshot for a persistenceId
              // but there can still be events between the given `offset` parameter and the min timestamp of the
              // snapshots and those would then be missed
              offset
            }

          log.debug(
            "currentEventsBySlicesStartingFromSnapshots initOffset [{}] with [{}] snapshots",
            initOffset,
            snapshotOffsets.size)

          bySlice(entityType, minSlice).currentBySlices(
            s"[$entityType] currentEventsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
            correlationId,
            entityType,
            minSlice,
            maxSlice,
            initOffset,
            filterEventsBeforeSnapshots(snapshotOffsets, backtrackingEnabled = false))
        }))
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
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("eventsBySlicesStartingFromSnapshots")
    val timestampOffset = toTimestampOffset(offset)
    val correlationId = QueryCorrelationId.get()
    val correlationIdText = CorrelationId.toLogText(correlationId)
    val snapshotSource =
      snapshotsBySlice[Snapshot, Event](entityType, minSlice, transformSnapshot)
        .currentBySlices(
          s"[$entityType] snapshotsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
          correlationId,
          entityType,
          minSlice,
          maxSlice,
          offset)

    Source.fromGraph(
      new StartingFromSnapshotStage[Event](
        snapshotSource,
        { snapshotOffsets =>
          val initOffset =
            if (timestampOffset == TimestampOffset.Zero && snapshotOffsets.nonEmpty) {
              val minTimestamp = snapshotOffsets.valuesIterator.minBy { case (_, timestamp) => timestamp }._2
              TimestampOffset(minTimestamp, Map.empty)
            } else {
              // don't adjust because then there is a risk that there was no found snapshot for a persistenceId
              // but there can still be events between the given `offset` parameter and the min timestamp of the
              // snapshots and those would then be missed
              offset
            }

          log.debug(
            s"eventsBySlicesStartingFromSnapshots $correlationIdText initOffset [{}] with [{}] snapshots",
            initOffset,
            snapshotOffsets.size)

          val dbSource =
            bySlice[Event](entityType, minSlice).liveBySlices(
              s"[$entityType] eventsBySlices [$minSlice-$maxSlice]$correlationIdText: ",
              correlationId,
              entityType,
              minSlice,
              maxSlice,
              initOffset,
              filterEventsBeforeSnapshots(snapshotOffsets, settings.querySettings.backtrackingEnabled))

          if (settings.journalPublishEvents) {
            // Note that events via PubSub are not filtered by snapshotOffsets. It's unlikely that
            // Those will be earlier than the snapshots and duplicates must be handled downstream in that case.
            // If we would use the filterEventsBeforeSnapshots function for PubSub it would be difficult to
            // know when memory of that Map can be released or the filter function would have to be shared
            // and thread safe, which is not worth it.
            val pubSubSource = eventsBySlicesPubSubSource[Event](entityType, minSlice, maxSlice)
            mergeDbAndPubSubSources(dbSource, pubSubSource, correlationId)
          } else
            dbSource
        }))
  }

  /**
   * Stateful filter function that decides if (persistenceId, seqNr, source) should be emitted by
   * `eventsBySlicesStartingFromSnapshots` and `currentEventsBySlicesStartingFromSnapshots`.
   */
  private def filterEventsBeforeSnapshots(
      snapshotOffsets: Map[String, (Long, Instant)],
      backtrackingEnabled: Boolean): (String, Long, String) => Boolean = {
    var _snapshotOffsets = snapshotOffsets
    (persistenceId, seqNr, source) => {
      if (_snapshotOffsets.isEmpty)
        true
      else
        _snapshotOffsets.get(persistenceId) match {
          case None                     => true
          case Some((snapshotSeqNr, _)) =>
            //  release memory by removing from the _snapshotOffsets Map
            if (seqNr == snapshotSeqNr &&
              ((backtrackingEnabled && source == EnvelopeOrigin.SourceBacktracking) ||
              (!backtrackingEnabled && source == EnvelopeOrigin.SourceQuery))) {
              _snapshotOffsets -= persistenceId
            }

            seqNr > snapshotSeqNr
        }
    }
  }

  private def checkStartFromSnapshotEnabled(methodName: String): Unit =
    if (!settings.querySettings.startFromSnapshotEnabled)
      throw new IllegalArgumentException(
        s"To use $methodName you must enable " +
        "configuration `akka.persistence.r2dbc.query.start-from-snapshot.enabled` and follow instructions in " +
        "migration guide https://doc.akka.io/libraries/akka-persistence-r2dbc/current/migration-guide.html#eventsBySlicesStartingFromSnapshots")

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
      .map { env =>
        env.eventOption match {
          case Some(se: SerializedEvent) =>
            env.withEvent(deserializeEvent(se))
          case _ => env
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def deserializeEvent[Event](se: SerializedEvent): Event =
    serialization.deserialize(se.bytes, se.serializerId, se.serializerManifest).get.asInstanceOf[Event]

  private def mergeDbAndPubSubSources[Event, Snapshot](
      dbSource: Source[EventEnvelope[Event], NotUsed],
      pubSubSource: Source[EventEnvelope[Event], NotUsed],
      correlationId: Option[String]) = {
    dbSource
      .mergePrioritized(pubSubSource, leftPriority = 1, rightPriority = 10)
      .via(
        skipPubSubTooFarAhead(
          settings.querySettings.backtrackingEnabled,
          JDuration.ofMillis(settings.querySettings.backtrackingWindow.toMillis),
          correlationId))
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
      maxAheadOfBacktracking: JDuration,
      correlationId: Option[String]): Flow[EventEnvelope[Event], EventEnvelope[Event], NotUsed] = {
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
                } else if (EnvelopeOrigin.fromHeartbeat(env)) {
                  latestBacktracking = t.timestamp
                  Nil // always drop heartbeats
                } else if (EnvelopeOrigin.fromPubSub(env) && latestBacktracking == Instant.EPOCH) {
                  log.trace(
                    "Dropping pubsub event for persistenceId [{}] seqNr [{}] because no event from backtracking yet{}",
                    env.persistenceId,
                    env.sequenceNr,
                    CorrelationId.toLogText(correlationId))
                  Nil
                } else if (EnvelopeOrigin.fromPubSub(env) && JDuration
                    .between(latestBacktracking, t.timestamp)
                    .compareTo(maxAheadOfBacktracking) > 0) {
                  // drop from pubsub when too far ahead from backtracking
                  log.debug(
                    "Dropping pubsub event for persistenceId [{}] seqNr [{}] because too far ahead of backtracking{}",
                    env.persistenceId,
                    env.sequenceNr,
                    CorrelationId.toLogText(correlationId))
                  Nil
                } else {
                  if (log.isDebugEnabled()) {
                    if (latestBacktracking.isAfter(t.timestamp))
                      log.debug(
                        "Event from query for persistenceId [{}] seqNr [{}] timestamp [{}]" +
                        " was before latest timestamp from backtracking or heartbeat [{}]{}",
                        env.persistenceId,
                        env.sequenceNr,
                        t.timestamp,
                        latestBacktracking,
                        CorrelationId.toLogText(correlationId))
                  }
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
      toSequenceNr: Long): Source[ClassicEventEnvelope, NotUsed] = {
    val correlationId = QueryCorrelationId.get()
    internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, correlationId = correlationId)
      .map(deserializeRow)
  }

  @ApiMayChange
  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] = {
    val correlationId = QueryCorrelationId.get()
    internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, correlationId = correlationId)
      .map(deserializeBySliceRow[Event])
  }

  /**
   * INTERNAL API: Used by both journal replay and currentEventsByPersistenceId
   */
  @InternalApi private[r2dbc] def internalCurrentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      readHighestSequenceNr: Boolean = true,
      includeDeleted: Boolean = false,
      correlationId: Option[String] = None): Source[SerializedJournalRow, NotUsed] = {
    lazy val correlationLogText = CorrelationId.toLogText(QueryCorrelationId.get())

    def updateState(state: ByPersistenceIdState, row: SerializedJournalRow): ByPersistenceIdState =
      state.copy(rowCount = state.rowCount + 1, latestSeqNr = row.seqNr)

    def nextQuery(
        state: ByPersistenceIdState,
        highestSeqNr: Long): (ByPersistenceIdState, Option[Source[SerializedJournalRow, NotUsed]]) = {
      if (state.queryCount == 0L || state.rowCount >= settings.querySettings.bufferSize) {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)

        if (state.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "currentEventsByPersistenceId query [{}] for persistenceId [{}], from [{}] to [{}]. Found [{}] rows in previous query{}",
            state.queryCount,
            persistenceId,
            state.latestSeqNr + 1,
            highestSeqNr,
            state.rowCount,
            correlationLogText)

        newState -> Some(
          queryDao
            .eventsByPersistenceId(persistenceId, state.latestSeqNr + 1, highestSeqNr, includeDeleted, correlationId))
      } else {
        log.debug(
          "currentEventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query{}",
          state.queryCount,
          persistenceId,
          state.rowCount,
          correlationLogText)

        state -> None
      }
    }

    if (log.isDebugEnabled())
      log.debug(
        "currentEventsByPersistenceId query for persistenceId [{}], from [{}] to [{}].{}",
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        correlationLogText)

    val highestSeqNrFut =
      if (readHighestSequenceNr && toSequenceNr == Long.MaxValue)
        journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr)
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

  /**
   * INTERNAL API
   */
  @InternalApi private[r2dbc] def internalLastEventByPersistenceId(
      persistenceId: String,
      toSequenceNr: Long,
      includeDeleted: Boolean): Future[Option[SerializedJournalRow]] = {
    queryDao.loadLastEvent(persistenceId, toSequenceNr, includeDeleted, None)
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    val correlationId = QueryCorrelationId.get()
    val result = queryDao.timestampOfEvent(persistenceId, sequenceNr, correlationId)
    if (log.isDebugEnabled) {
      lazy val correlationLogText = CorrelationId.toLogText(QueryCorrelationId.get())
      result.foreach { t =>
        log.debug("[{}] timestampOf seqNr [{}] is [{}]{}", persistenceId, sequenceNr, t, correlationLogText)
      }
    }
    result
  }

  // LatestEventTimestampQuery
  override def latestEventTimestamp(entityType: String, minSlice: Int, maxSlice: Int): Future[Option[Instant]] = {
    val correlationId = QueryCorrelationId.get()
    lazy val correlationLogText = CorrelationId.toLogText(QueryCorrelationId.get())
    settings.querySettings.cacheLatestEventTimestamp match {
      case Some(cacheTtl) if cacheTtl > Duration.Zero =>
        val cacheKey = EntitySliceRange(entityType, minSlice, maxSlice)
        val cachedValue = latestEventTimestampCache.get(cacheKey)
        val expiry = Option(cachedValue).map(_.cachedAt.plusMillis(cacheTtl.toMillis))
        val now = clock.instant()
        if (expiry.exists(now.isBefore)) { // cache hit and not expired
          log.debug(
            "[{}] latestEventTimestamp for slices [{} - {}] is [{}] (was cached at [{}]){}",
            entityType,
            minSlice,
            maxSlice,
            cachedValue.timestamp,
            cachedValue.cachedAt,
            correlationLogText)
          Future.successful(cachedValue.timestamp)
        } else { // cache miss or expired, fetch and cache
          val result = queryDao.latestEventTimestamp(entityType, minSlice, maxSlice, correlationId)
          result.foreach { timestamp =>
            log.debug(
              "[{}] latestEventTimestamp for slices [{} - {}] is [{}] (caching with TTL [{}]){}",
              entityType,
              minSlice,
              maxSlice,
              timestamp,
              cacheTtl.toCoarsest,
              correlationLogText)
            latestEventTimestampCache.put(cacheKey, CachedTimestamp(timestamp, now))
          }
          result
        }
      case _ => // caching disabled
        val result = queryDao.latestEventTimestamp(entityType, minSlice, maxSlice, correlationId)
        if (log.isDebugEnabled) {
          result.foreach { timestamp =>
            log.debug(
              "[{}] latestEventTimestamp for slices [{} - {}] is [{}]{}",
              entityType,
              minSlice,
              maxSlice,
              timestamp,
              correlationLogText)
          }
        }
        result
    }
  }

  /** INTERNAL API */
  @InternalApi private[r2dbc] def clearLatestEventTimestampCache(): Unit = latestEventTimestampCache.clear()

  //LoadEventQuery
  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
    val correlationId = QueryCorrelationId.get()
    if (log.isDebugEnabled()) {
      val correlationLogText = CorrelationId.toLogText(QueryCorrelationId.get())
      log.debug("[{}] loadEnvelope seqNr [{}]{}", persistenceId, sequenceNr, correlationLogText)
    }
    queryDao
      .loadEvent(persistenceId, sequenceNr, includePayload = true, correlationId)
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
      toSequenceNr: Long,
      correlationId: Option[String] = None): Source[SerializedJournalRow, NotUsed] = {
    lazy val correlationLogText = CorrelationId.toLogText(QueryCorrelationId.get())
    if (log.isDebugEnabled)
      log.debug(
        "Starting eventsByPersistenceId query for persistenceId [{}], from [{}]{}",
        persistenceId,
        fromSequenceNr,
        correlationLogText)

    def nextOffset(state: ByPersistenceIdState, row: SerializedJournalRow): ByPersistenceIdState =
      state.copy(rowCount = state.rowCount + 1, latestSeqNr = row.seqNr)

    def delayNextQuery(state: ByPersistenceIdState): Option[FiniteDuration] = {
      val delay = ContinuousQuery.adjustNextDelay(
        state.rowCount,
        settings.querySettings.bufferSize,
        settings.querySettings.refreshInterval)

      if (log.isDebugEnabled) {
        delay.foreach { d =>
          log.debug(
            "eventsByPersistenceId query [{}] for persistenceId [{}] delay next [{}] ms{}",
            state.queryCount,
            persistenceId,
            d.toMillis,
            correlationLogText)
        }
      }

      delay
    }

    def nextQuery(
        state: ByPersistenceIdState): (ByPersistenceIdState, Option[Source[SerializedJournalRow, NotUsed]]) = {
      if (state.latestSeqNr >= toSequenceNr) {
        if (log.isDebugEnabled) {
          log.debug(
            "eventsByPersistenceId query [{}] for persistenceId [{}] completed. Found [{}] rows in previous query{}",
            state.queryCount,
            persistenceId,
            state.rowCount,
            correlationLogText)
        }
        state -> None
      } else {
        val newState = state.copy(rowCount = 0, queryCount = state.queryCount + 1)
        if (log.isDebugEnabled) {
          log.debug(
            "eventsByPersistenceId query [{}] for persistenceId [{}], from [{}]. Found [{}] rows in previous query{}",
            newState.queryCount,
            persistenceId,
            state.latestSeqNr + 1,
            state.rowCount,
            correlationLogText)
        }
        newState ->
        Some(
          queryDao
            .eventsByPersistenceId(
              persistenceId,
              state.latestSeqNr + 1,
              toSequenceNr,
              includeDeleted = false,
              correlationId = correlationId))
      }
    }

    ContinuousQuery[ByPersistenceIdState, SerializedJournalRow](
      initialState = ByPersistenceIdState(0, 0, latestSeqNr = fromSequenceNr - 1),
      updateState = nextOffset,
      delayNextQuery = delayNextQuery,
      nextQuery = nextQuery)
  }

  override def currentEventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("currentEventsByPersistenceIdStartingFromSnapshot")
    val correlationId = QueryCorrelationId.get()
    Source
      .futureSource(snapshotDao.load(persistenceId, SnapshotSelectionCriteria.Latest).map {
        case Some(snapshotRow) =>
          if (fromSequenceNr <= snapshotRow.seqNr && snapshotRow.seqNr <= toSequenceNr) {
            val offset = TimestampOffset(snapshotRow.dbTimestamp, Map(snapshotRow.persistenceId -> snapshotRow.seqNr))
            val snapshotEnv = createEnvelopeFromSnapshot(snapshotRow, offset, transformSnapshot)
            Source
              .single(snapshotEnv)
              .concat(
                internalCurrentEventsByPersistenceId(
                  persistenceId,
                  snapshotEnv.sequenceNr + 1,
                  toSequenceNr,
                  correlationId = correlationId)
                  .map(deserializeBySliceRow[Event]))
          } else
            internalCurrentEventsByPersistenceId(
              persistenceId,
              fromSequenceNr,
              toSequenceNr,
              correlationId = correlationId)
              .map(deserializeBySliceRow[Event])
        case None =>
          internalCurrentEventsByPersistenceId(
            persistenceId,
            fromSequenceNr,
            toSequenceNr,
            correlationId = correlationId)
            .map(deserializeBySliceRow[Event])

      })
      .mapMaterializedValue(_ => NotUsed)
  }

  override def eventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    checkStartFromSnapshotEnabled("eventsByPersistenceIdStartingFromSnapshot")
    val correlationId = QueryCorrelationId.get()
    Source
      .futureSource(snapshotDao.load(persistenceId, SnapshotSelectionCriteria.Latest).map {
        case Some(snapshotRow) =>
          if (fromSequenceNr <= snapshotRow.seqNr && snapshotRow.seqNr <= toSequenceNr) {
            val offset = TimestampOffset(snapshotRow.dbTimestamp, Map(snapshotRow.persistenceId -> snapshotRow.seqNr))
            val snapshotEnv = createEnvelopeFromSnapshot(snapshotRow, offset, transformSnapshot)
            Source
              .single(snapshotEnv)
              .concat(
                internalEventsByPersistenceId(
                  persistenceId,
                  snapshotEnv.sequenceNr + 1,
                  toSequenceNr,
                  correlationId = correlationId)
                  .map(deserializeBySliceRow[Event]))
          } else
            internalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, correlationId = correlationId)
              .map(deserializeBySliceRow[Event])
        case None =>
          internalEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, correlationId = correlationId)
            .map(deserializeBySliceRow[Event])

      })
      .mapMaterializedValue(_ => NotUsed)
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
  override def currentPersistenceIds(
      entityType: String,
      afterId: Option[String],
      limit: Long): Source[String, NotUsed] =
    queryDao.persistenceIds(entityType, afterId, limit)

  /**
   * Load the last event for the given `persistenceId` up to the given `toSeqNr`.
   *
   * @param persistenceId
   *   The persistence id to load the last event for.
   * @param toSequenceNr
   *   The sequence number to load the last event up to.
   */
  def loadLastEvent[Event](persistenceId: String, toSequenceNr: Long): Future[Option[EventEnvelope[Event]]] = {
    internalLastEventByPersistenceId(persistenceId, toSequenceNr, includeDeleted = false).map(
      _.map(deserializeBySliceRow))
  }

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
