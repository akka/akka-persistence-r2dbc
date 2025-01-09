/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import java.time.Clock
import java.time.Instant
import java.util.UUID

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.Persistence
import akka.persistence.SerializedEvent
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.scaladsl.DurableStateStorePagedPersistenceIdsQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.DurableStateStoreBySliceQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.ContinuousQuery
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PubSub
import akka.persistence.state.scaladsl.DurableStateUpdateWithChangeEventStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.stream.scaladsl.Concat

object R2dbcDurableStateStore {
  val Identifier = "akka.persistence.r2dbc.state"

  private final case class PersistenceIdsQueryState(
      queryCount: Int,
      rowCount: Int,
      latestPid: String,
      tables: List[String])
}

class R2dbcDurableStateStore[A](system: ExtendedActorSystem, config: Config, cfgPath: String)
    extends DurableStateUpdateWithChangeEventStore[A]
    with DurableStateStoreBySliceQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A] {
  import R2dbcDurableStateStore.PersistenceIdsQueryState

  private val log = LoggerFactory.getLogger(getClass)
  private val sharedConfigPath = cfgPath.replaceAll("""\.state$""", "")
  private val settings = R2dbcSettings(system.settings.config.getConfig(sharedConfigPath))
  log.debug("R2DBC journal starting up with dialect [{}]", settings.dialectName)

  private val typedSystem = system.toTyped
  private val serialization = SerializationExtension(system)
  private val persistenceExt = Persistence(system)
  private val executorProvider =
    new R2dbcExecutorProvider(
      typedSystem,
      settings.connectionFactorySettings.dialect.daoExecutionContext(settings, typedSystem),
      settings,
      sharedConfigPath + ".connection-factory",
      LoggerFactory.getLogger(getClass))
  private val stateDao =
    settings.connectionFactorySettings.dialect.createDurableStateDao(executorProvider)
  private val changeEventWriterUuid = UUID.randomUUID().toString

  private val pubSub: Option[PubSub] =
    if (settings.journalPublishEvents) Some(PubSub(typedSystem))
    else None

  private val clock = Clock.systemUTC()

  private val bySlice: BySliceQuery[SerializedStateRow, DurableStateChange[A]] = {
    val createEnvelope: (TimestampOffset, SerializedStateRow) => DurableStateChange[A] = (offset, row) => {
      row.payload match {
        case null =>
          // payload = null => lazy loaded for backtracking (ugly, but not worth changing UpdatedDurableState in Akka)
          new UpdatedDurableState(
            row.persistenceId,
            row.revision,
            null.asInstanceOf[A],
            offset,
            row.dbTimestamp.toEpochMilli)
        case Some(bytes) =>
          val payload = serialization.deserialize(bytes, row.serId, row.serManifest).get.asInstanceOf[A]
          new UpdatedDurableState(row.persistenceId, row.revision, payload, offset, row.dbTimestamp.toEpochMilli)
        case None =>
          new DeletedDurableState(row.persistenceId, row.revision, offset, row.dbTimestamp.toEpochMilli)
      }
    }

    val extractOffset: DurableStateChange[A] => TimestampOffset = env => env.offset.asInstanceOf[TimestampOffset]

    new BySliceQuery(stateDao, createEnvelope, extractOffset, createHeartbeat = _ => None, clock, settings, log)(
      typedSystem.executionContext)
  }

  override def getObject(persistenceId: String): Future[GetObjectResult[A]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    stateDao.readState(persistenceId).map {
      case None => GetObjectResult(None, 0L)
      case Some(serializedRow) =>
        val payload =
          serializedRow.payload.map { bytes =>
            serialization
              .deserialize(bytes, serializedRow.serId, serializedRow.serManifest)
              .get
              .asInstanceOf[A]
          }
        GetObjectResult(payload, serializedRow.revision)
    }
  }

  /**
   * Insert the value if `revision` is 1, which will fail with `IllegalStateException` if there is already a stored
   * value for the given `persistenceId`. Otherwise update the value, which will fail with `IllegalStateException` if
   * the existing stored `revision` + 1 isn't equal to the given `revision`. This optimistic locking check can be
   * disabled with configuration `assert-single-writer`.
   */
  override def upsertObject(persistenceId: String, revision: Long, value: A, tag: String): Future[Done] =
    internalUpsertObject(persistenceId, revision, value, tag, changeEvent = None)

  /**
   * Insert the value if `revision` is 1, which will fail with `IllegalStateException` if there is already a stored
   * value for the given `persistenceId`. Otherwise update the value, which will fail with `IllegalStateException` if
   * the existing stored `revision` + 1 isn't equal to the given `revision`. This optimistic locking check can be
   * disabled with configuration `assert-single-writer`.
   *
   * The `changeEvent` is written to the event journal in the same transaction as the DurableState upsert. Same
   * `persistenceId` is used in the journal and the `revision` is used as `sequenceNr`.
   */
  override def upsertObject(
      persistenceId: String,
      revision: Long,
      value: A,
      tag: String,
      changeEvent: Any): Future[Done] =
    internalUpsertObject(persistenceId, revision, value, tag, changeEvent = Some(changeEvent))

  private def internalUpsertObject(
      persistenceId: String,
      revision: Long,
      value: A,
      tag: String,
      changeEvent: Option[Any]): Future[Done] = {

    val valueAnyRef = value.asInstanceOf[AnyRef]
    val serialized = serialization.serialize(valueAnyRef).get
    val serializer = serialization.findSerializerFor(valueAnyRef)
    val manifest = Serializers.manifestFor(serializer, valueAnyRef)

    val tags = if (tag.isEmpty) Set.empty[String] else Set(tag)
    val serializedRow = SerializedStateRow(
      persistenceId,
      revision,
      DurableStateDao.EmptyDbTimestamp,
      DurableStateDao.EmptyDbTimestamp,
      Some(serialized),
      serializer.identifier,
      manifest,
      tags)

    val changeEventTimestamp =
      stateDao.upsertState(serializedRow, value, serializedChangeEvent(persistenceId, revision, tag, changeEvent))

    import typedSystem.executionContext
    changeEventTimestamp.map { timestampOption =>
      publish(persistenceId, revision, changeEvent, timestampOption, tags)
      Done
    }
  }

  private def publish(
      persistenceId: String,
      revision: Long,
      changeEvent: Option[Any],
      changeEventTimestamp: Option[Instant],
      tags: Set[String]): Unit = {
    for {
      timestamp <- changeEventTimestamp
      event <- changeEvent
      p <- pubSub
    } yield {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)

      val offset = TimestampOffset(timestamp, timestamp, Map(persistenceId -> revision))

      val envelope = EventEnvelope(
        offset,
        persistenceId,
        revision,
        event,
        timestamp.toEpochMilli,
        entityType,
        slice,
        filtered = false,
        source = EnvelopeOrigin.SourcePubSub,
        tags)
      p.publish(envelope)
    }
  }

  private def serializedChangeEvent(persistenceId: String, revision: Long, tag: String, changeEvent: Option[Any]) = {
    changeEvent.map { event =>
      val eventAnyRef = event.asInstanceOf[AnyRef]
      val serializedEvent = eventAnyRef match {
        case s: SerializedEvent => s // already serialized
        case _ =>
          val bytes = serialization.serialize(eventAnyRef).get
          val serializer = serialization.findSerializerFor(eventAnyRef)
          val manifest = Serializers.manifestFor(serializer, eventAnyRef)
          new SerializedEvent(bytes, serializer.identifier, manifest)
      }

      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      val timestamp = if (settings.useAppTimestamp) InstantFactory.now() else JournalDao.EmptyDbTimestamp

      SerializedJournalRow(
        slice,
        entityType,
        persistenceId,
        revision,
        timestamp,
        JournalDao.EmptyDbTimestamp,
        Some(serializedEvent.bytes),
        serializedEvent.serializerId,
        serializedEvent.serializerManifest,
        changeEventWriterUuid,
        if (tag.isEmpty) Set.empty else Set(tag),
        metadata = None)
    }
  }

  @deprecated(message = "Use the deleteObject overload with revision instead.", since = "1.0.0")
  override def deleteObject(persistenceId: String): Future[Done] =
    deleteObject(persistenceId, revision = 0)

  /**
   * Delete the value, which will fail with `IllegalStateException` if the existing stored `revision` + 1 isn't equal to
   * the given `revision`. This optimistic locking check can be disabled with configuration `assert-single-writer`. The
   * stored revision for the persistenceId is updated and next call to [[getObject]] will return the revision, but with
   * no value.
   *
   * If the given revision is `0` it will fully delete the value and revision from the database without any optimistic
   * locking check. Next call to [[getObject]] will then return revision 0 and no value.
   */
  override def deleteObject(persistenceId: String, revision: Long): Future[Done] =
    internalDeleteObject(persistenceId, revision, changeEvent = None)

  /**
   * Delete the value, which will fail with `IllegalStateException` if the existing stored `revision` + 1 isn't equal to
   * the given `revision`. This optimistic locking check can be disabled with configuration `assert-single-writer`. The
   * stored revision for the persistenceId is updated and next call to [[getObject]] will return the revision, but with
   * no value.
   *
   * If the given revision is `0` it will fully delete the value and revision from the database without any optimistic
   * locking check. Next call to [[getObject]] will then return revision 0 and no value.
   *
   * The `changeEvent` is written to the event journal in the same transaction as the DurableState upsert. Same
   * `persistenceId` is used in the journal and the `revision` is used as `sequenceNr`.
   */
  override def deleteObject(persistenceId: String, revision: Long, changeEvent: Any): Future[Done] =
    internalDeleteObject(persistenceId, revision, changeEvent = Some(changeEvent))

  private def internalDeleteObject(persistenceId: String, revision: Long, changeEvent: Option[Any]): Future[Done] = {
    val changeEventTimestamp = stateDao.deleteState(
      persistenceId,
      revision,
      serializedChangeEvent(persistenceId, revision, tag = "", changeEvent))

    import typedSystem.executionContext
    changeEventTimestamp.map { timestampOption =>
      publish(persistenceId, revision, changeEvent, timestampOption, tags = Set.empty)
      Done
    }
  }

  override def sliceForPersistenceId(persistenceId: String): Int =
    persistenceExt.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  override def currentChangesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    bySlice.currentBySlices(
      s"[$entityType] currentChangesBySlices [$minSlice-$maxSlice]: ",
      entityType,
      minSlice,
      maxSlice,
      offset)

  override def changesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    bySlice.liveBySlices(
      s"[$entityType] changesBySlices [$minSlice-$maxSlice]: ",
      entityType,
      minSlice,
      maxSlice,
      offset)

  /**
   * Note: If you have configured `custom-table` this query will look in both the default table and the custom tables.
   * If you are only interested in ids for a specific entity type it's more efficient to use `currentPersistenceIds`
   * with `entityType` parameter.
   */
  override def currentPersistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] =
    stateDao.persistenceIds(afterId, limit)

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
    stateDao.persistenceIds(entityType, afterId, limit)

  def currentPersistenceIds(): Source[String, NotUsed] = {
    import settings.querySettings.persistenceIdsBufferSize
    def updateState(state: PersistenceIdsQueryState, pid: String): PersistenceIdsQueryState =
      state.copy(rowCount = state.rowCount + 1, latestPid = pid)

    def nextQuery(
        state: PersistenceIdsQueryState,
        dataPartitionSlice: Int): (PersistenceIdsQueryState, Option[Source[String, NotUsed]]) = {
      def next(newState: PersistenceIdsQueryState) = {
        val newState2 = newState.copy(rowCount = 0, queryCount = newState.queryCount + 1)

        if (newState.queryCount != 0 && log.isDebugEnabled())
          log.debug(
            "persistenceIds query [{}] after [{}]. Found [{}] rows in previous query.",
            newState.queryCount,
            newState.latestPid,
            newState.rowCount)

        val afterPid = if (newState.latestPid == "") None else Some(newState.latestPid)

        newState2 -> Some(
          stateDao
            .persistenceIds(afterPid, persistenceIdsBufferSize, newState.tables.head, dataPartitionSlice))
      }

      if (state.queryCount == 0L || state.rowCount >= persistenceIdsBufferSize) {
        next(state)
      } else if (state.tables.tail.nonEmpty) {
        // continue with next custom table
        next(state.copy(tables = state.tables.tail, latestPid = ""))
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "persistenceIds query [{}] completed. Found [{}] rows in previous query.",
            state.queryCount,
            state.rowCount)

        state -> None
      }
    }

    val entityTypes = settings.durableStateTableByEntityTypeWithSchema.keys.toList.sorted

    val queries: immutable.IndexedSeq[Source[String, NotUsed]] =
      settings.dataPartitionSliceRanges.map { sliceRange =>
        val tables =
          settings.durableStateTableWithSchema(sliceRange.min) ::
          entityTypes.map(settings.getDurableStateTableWithSchema(_, sliceRange.min))

        ContinuousQuery[PersistenceIdsQueryState, String](
          initialState = PersistenceIdsQueryState(0, 0, "", tables),
          updateState = updateState,
          delayNextQuery = _ => None,
          nextQuery = state => nextQuery(state, sliceRange.min))
          .mapMaterializedValue(_ => NotUsed)

      }

    Source
      .combine(queries)(Concat(_))
      .mapMaterializedValue(_ => NotUsed)

  }

}
