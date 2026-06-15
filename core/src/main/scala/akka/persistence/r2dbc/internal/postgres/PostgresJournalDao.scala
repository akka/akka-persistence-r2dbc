/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.postgres

import java.lang
import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.SerializedEvent
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.JournalAdditionalColumnFactory
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.SerializedEventMetadata
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresJournalDao])

  private[r2dbc] final case class EvaluatedAdditionalColumnBindings(
      additionalColumn: AdditionalColumn[_, _],
      binding: AdditionalColumn.Binding[_])

  def readMetadata(row: Row): Option[SerializedEventMetadata] = {
    row.get("meta_payload", classOf[Array[Byte]]) match {
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get[Integer]("meta_ser_id", classOf[Integer]),
            serManifest = row.get("meta_ser_manifest", classOf[String]),
            metaPayload))
    }
  }

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class PostgresJournalDao(executorProvider: R2dbcExecutorProvider) extends JournalDao {
  protected val settings: R2dbcSettings = executorProvider.settings
  protected val system: ActorSystem[_] = executorProvider.system
  implicit protected val ec: ExecutionContext = executorProvider.ec
  import settings.codecSettings.JournalImplicits._

  import JournalDao.SerializedJournalRow
  import PostgresJournalDao.EvaluatedAdditionalColumnBindings
  protected def log: Logger = PostgresJournalDao.log

  protected val persistenceExt: Persistence = Persistence(system)

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  private lazy val additionalColumns: Map[String, immutable.IndexedSeq[AdditionalColumn[Any, Any]]] = {
    settings.journalAdditionalColumnClasses.map { case (entityType, columnClasses) =>
      val instances = columnClasses.map(fqcn => JournalAdditionalColumnFactory.create(system, fqcn))
      entityType -> instances
    }
  }

  // only used (lazily) when journal additional-columns are configured and the event value isn't supplied
  // already-deserialized by the caller, e.g. for already-serialized (replicated) events or during migration
  private lazy val serialization = SerializationExtension(system)

  protected def journalTable(entityType: String, slice: Int): String =
    settings.getJournalTableWithSchema(entityType, slice)

  protected def insertEventWithParameterTimestampSql(
      entityType: String,
      slice: Int,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    def createSql = {
      val table = journalTable(entityType, slice)
      val baseSql = insertEvenBaseSql(table, additionalBindings)
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql ?) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(?, ${timestampSubSelect(table)})) RETURNING db_timestamp"
    }
    if (additionalBindings.isEmpty)
      sqlCache.get(slice, s"insertEventWithParameterTimestampSql-${settings.journalTableCacheKey(entityType)}")(
        createSql)
    else
      createSql
  }

  private def insertEventWithTransactionTimestampSql(
      entityType: String,
      slice: Int,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    def createSql = {
      val table = journalTable(entityType, slice)
      val baseSql = insertEvenBaseSql(table, additionalBindings)
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql CURRENT_TIMESTAMP) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(CURRENT_TIMESTAMP, ${timestampSubSelect(table)})) RETURNING db_timestamp"
    }
    if (additionalBindings.isEmpty)
      sqlCache.get(slice, s"insertEventWithTransactionTimestampSql-${settings.journalTableCacheKey(entityType)}")(
        createSql)
    else
      createSql
  }

  private def insertEvenBaseSql(
      table: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    s"INSERT INTO $table " +
    s"(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload$additionalCols, db_timestamp) " +
    s"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?$additionalParams, "
  }

  protected def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(c, AdditionalColumn.BindNull) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  protected def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(_, _: AdditionalColumn.BindValue[_]) |
            EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindNull) =>
          strB.append(", ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
  // always increasing for a pid (time not going backwards).
  // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
  private def timestampSubSelect(table: String) =
    s"(SELECT db_timestamp + '1 microsecond'::interval FROM $table " +
    "WHERE persistence_id = ? AND seq_nr = ?)"

  private def selectHighestSequenceNrSql(entityType: String, slice: Int) =
    sqlCache.get(slice, s"selectHighestSequenceNrSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      SELECT MAX(seq_nr) from ${journalTable(entityType, slice)}
      WHERE persistence_id = ? AND seq_nr >= ?"""
    }

  private def selectLowestSequenceNrSql(entityType: String, slice: Int) =
    sqlCache.get(slice, s"selectLowestSequenceNrSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      SELECT MIN(seq_nr) from ${journalTable(entityType, slice)}
      WHERE persistence_id = ?"""
    }

  private def deleteEventsSql(entityType: String, slice: Int) =
    sqlCache.get(slice, s"deleteEventsSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      DELETE FROM ${journalTable(entityType, slice)}
      WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""
    }

  protected def insertDeleteMarkerSql(
      entityType: String,
      slice: Int,
      timestamp: String = "CURRENT_TIMESTAMP"): String = {
    // timestamp param doesn't have to be part of cache key because it's just different for different dialects
    sqlCache.get(slice, s"insertDeleteMarkerSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      INSERT INTO ${journalTable(entityType, slice)}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
      VALUES (?, ?, ?, ?, $timestamp, ?, ?, ?, ?, ?, ?)"""
    }
  }

  private def deleteEventsByPersistenceIdBeforeTimestampSql(entityType: String, slice: Int) =
    sqlCache.get(slice, s"deleteEventsByPersistenceIdBeforeTimestampSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      DELETE FROM ${journalTable(entityType, slice)}
      WHERE persistence_id = ? AND db_timestamp < ?"""
    }

  private def deleteEventsBySliceBeforeTimestampSql(entityType: String, slice: Int) =
    sqlCache.get(slice, s"deleteEventsBySliceBeforeTimestampSql-${settings.journalTableCacheKey(entityType)}") {
      sql"""
      DELETE FROM ${journalTable(entityType, slice)}
      WHERE slice = ? AND entity_type = ? AND db_timestamp < ?"""
    }

  protected def additionalBindings(
      entityType: String,
      row: SerializedJournalRow): immutable.IndexedSeq[EvaluatedAdditionalColumnBindings] =
    additionalColumns.get(entityType) match {
      case None          => Vector.empty[EvaluatedAdditionalColumnBindings]
      case Some(columns) =>
        // The normal write path supplies the already-deserialized event in `eventValue`. When it isn't available
        // (already-serialized/replicated events that arrive as a `SerializedEvent`, or the migration tool) deserialize
        // from the stored payload so `bind` always sees the real event rather than a `SerializedEvent` wrapper.
        val value = row.eventValue match {
          case Some(_: SerializedEvent) | None => deserializeEvent(row)
          case Some(v)                         => v
        }
        val insert = AdditionalColumn.Insert(
          row.persistenceId,
          entityType,
          row.slice,
          row.seqNr,
          value,
          row.eventMetadata,
          row.tags)
        columns.map(c => EvaluatedAdditionalColumnBindings(c, c.bind(insert)))
    }

  private def deserializeEvent(row: SerializedJournalRow): Any =
    row.payload match {
      case Some(bytes) => serialization.deserialize(bytes, row.serId, row.serManifest).get
      case None =>
        throw new IllegalStateException(
          s"Event payload for persistenceId [${row.persistenceId}] seqNr [${row.seqNr}] not available for journal " +
          s"AdditionalColumn.")
    }

  protected def bindAdditionalColumns(
      stmt: Statement,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings],
      nextIndex: () => Int): Statement = {
    additionalBindings.foreach {
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindValue(v)) =>
        stmt.bind(nextIndex(), v)
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
        stmt.bindNull(nextIndex(), col.fieldClass)
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
    }
    stmt
  }

  /** A binding shape that can be reused across all rows in a batch — only the actual `BindValue` payloads differ. */
  protected def sameBindingShape(
      a: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings],
      b: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): Boolean = {
    if (a.size != b.size) false
    else {
      var i = 0
      var ok = true
      while (ok && i < a.size) {
        val sameShape = (a(i).binding, b(i).binding) match {
          case (_: AdditionalColumn.BindValue[_], _: AdditionalColumn.BindValue[_]) => true
          case (AdditionalColumn.BindNull, AdditionalColumn.BindNull)               => true
          case (AdditionalColumn.Skip, AdditionalColumn.Skip)                       => true
          case _                                                                    => false
        }
        ok = sameShape && (a(i).additionalColumn eq b(i).additionalColumn)
        i += 1
      }
      ok
    }
  }

  /**
   * Evaluate the additional-column bindings for all events of an `AtomicWrite`. The head row's bindings define the
   * INSERT shape; every other row must produce the same shape (same Skip/BindNull/BindValue decisions) because the
   * batch reuses a single prepared statement. Returns a `Try` so a non-deterministic `bind` (or a failed on-demand
   * deserialization) becomes a normal write failure rather than a synchronous throw out of the write path.
   */
  protected def evaluateBatchBindings(entityType: String, events: Seq[SerializedJournalRow]): Try[(
      immutable.IndexedSeq[EvaluatedAdditionalColumnBindings],
      Seq[immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]])] =
    Try {
      val headBindings = additionalBindings(entityType, events.head)
      val perEventBindings: Seq[immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]] =
        if (headBindings.isEmpty) Vector.empty
        else {
          val all = events.iterator.map(ev => additionalBindings(entityType, ev)).toVector
          all.tail.foreach { b =>
            if (!sameBindingShape(headBindings, b))
              throw new IllegalArgumentException(
                s"AdditionalColumn bindings for entityType [$entityType] must produce the same shape " +
                s"(same Skip/BindNull/BindValue decisions) across all events in an AtomicWrite. " +
                s"persistenceId [${events.head.persistenceId}].")
          }
          all
        }
      (headBindings, perEventBindings)
    }

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be the `db_timestamp` column and it is used in published events when that feature is
   * enabled.
   *
   * Note for implementing future database dialects: If a database dialect can't efficiently return the timestamp column
   * it can return `JournalDao.EmptyDbTimestamp` when the pub-sub feature is disabled. When enabled it would have to use
   * a select (in same transaction).
   */
  override def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId
    val entityType = events.head.entityType
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    evaluateBatchBindings(entityType, events) match {
      case Failure(exc) => Future.failed(exc)
      case Success((headBindings, perEventBindings)) =>
        val insertSql =
          if (useTimestampFromDb) insertEventWithTransactionTimestampSql(entityType, slice, headBindings)
          else insertEventWithParameterTimestampSql(entityType, slice, headBindings)

        val totalEvents = events.size
        if (totalEvents == 1) {
          val result = executor.updateOneReturning(s"insert [$persistenceId]")(
            connection =>
              bindInsertStatement(
                connection.createStatement(insertSql),
                events.head,
                useTimestampFromDb,
                previousSeqNr,
                headBindings),
            row => row.getTimestamp("db_timestamp"))
          if (log.isDebugEnabled())
            result.foreach { _ =>
              log.debug("Wrote [{}] events for persistenceId [{}]", 1, persistenceId)
            }
          result
        } else {
          val result = executor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
            connection =>
              events.iterator.zipWithIndex
                .foldLeft(connection.createStatement(insertSql)) { case (stmt, (write, idx)) =>
                  stmt.add()
                  val bindings = if (perEventBindings.isEmpty) headBindings else perEventBindings(idx)
                  bindInsertStatement(stmt, write, useTimestampFromDb, previousSeqNr, bindings)
                },
            row => row.getTimestamp("db_timestamp"))
          if (log.isDebugEnabled())
            result.foreach { _ =>
              log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, persistenceId)
            }
          result.map(_.head)(ExecutionContext.parasitic)
        }
    }
  }

  override def writeEventInTx(event: SerializedJournalRow, connection: Connection): Future[Instant] = {
    val persistenceId = event.persistenceId
    val entityType = event.entityType
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val previousSeqNr = event.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = event.dbTimestamp == Instant.EPOCH

    val bindings = additionalBindings(entityType, event)

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql(entityType, slice, bindings)
      else insertEventWithParameterTimestampSql(entityType, slice, bindings)

    val stmt =
      bindInsertStatement(connection.createStatement(insertSql), event, useTimestampFromDb, previousSeqNr, bindings)
    val result = R2dbcExecutor.updateOneReturningInTx(stmt, row => row.getTimestamp("db_timestamp"))
    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] event for persistenceId [{}]", 1, persistenceId)
      }
    result
  }

  protected def bindInsertStatement(
      stmt: Statement,
      write: SerializedJournalRow,
      useTimestampFromDb: Boolean,
      previousSeqNr: Long,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
    val idx = Iterator.range(0, Int.MaxValue)
    stmt
      .bind(idx.next(), write.slice)
      .bind(idx.next(), write.entityType)
      .bind(idx.next(), write.persistenceId)
      .bind(idx.next(), write.seqNr)
      .bind(idx.next(), write.writerUuid)
      .bind(idx.next(), "") // FIXME event adapter
      .bind(idx.next(), write.serId)
      .bind(idx.next(), write.serManifest)
      .bindPayload(idx.next(), write.payload.get)

    val tagsIdx = idx.next()
    if (write.tags.isEmpty)
      stmt.bindTagsNull(tagsIdx)
    else
      stmt.bindTags(tagsIdx, write.tags)

    // optional metadata
    write.metadata match {
      case Some(m) =>
        stmt
          .bind(idx.next(), m.serId)
          .bind(idx.next(), m.serManifest)
          .bind(idx.next(), m.payload)
      case None =>
        stmt
          .bindNull(idx.next(), classOf[Integer])
          .bindNull(idx.next(), classOf[String])
          .bindNull(idx.next(), classOf[Array[Byte]])
    }

    bindAdditionalColumns(stmt, additionalBindings, idx.next)

    if (useTimestampFromDb) {
      if (!settings.dbTimestampMonotonicIncreasing)
        stmt
          .bind(idx.next(), write.persistenceId)
          .bind(idx.next(), previousSeqNr)
    } else {
      if (settings.dbTimestampMonotonicIncreasing)
        stmt
          .bindTimestamp(idx.next(), write.dbTimestamp)
      else
        stmt
          .bindTimestamp(idx.next(), write.dbTimestamp)
          .bind(idx.next(), write.persistenceId)
          .bind(idx.next(), previousSeqNr)
    }

    stmt
  }

  override def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val result = executor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectHighestSequenceNrSql(entityType, slice))
            .bind(0, persistenceId)
            .bind(1, fromSequenceNr),
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  override def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val result = executor
      .select(s"select lowest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectLowestSequenceNrSql(entityType, slice))
            .bind(0, persistenceId),
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  private def highestSeqNrForDelete(persistenceId: String, toSequenceNr: Long): Future[Long] = {
    if (toSequenceNr == Long.MaxValue)
      readHighestSequenceNr(persistenceId, 0L)
    else
      Future.successful(toSequenceNr)
  }

  private def lowestSequenceNrForDelete(persistenceId: String, toSeqNr: Long, batchSize: Int): Future[Long] = {
    if (toSeqNr <= batchSize) {
      Future.successful(1L)
    } else {
      readLowestSequenceNr(persistenceId)
    }
  }
  protected def bindTimestampNow(stmt: Statement, getAndIncIndex: () => Int): Statement = stmt
  override def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)

    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val idx = Iterator.range(0, Int.MaxValue)
      val stmt = connection.createStatement(insertDeleteMarkerSql(entityType, slice))
      stmt
        .bind(idx.next(), slice)
        .bind(idx.next(), entityType)
        .bind(idx.next(), persistenceId)
        .bind(idx.next(), deleteMarkerSeqNr)

      bindTimestampNow(stmt, idx.next)
        .bind(idx.next(), "")
        .bind(idx.next(), "")
        .bind(idx.next(), 0)
        .bind(idx.next(), "")
        .bindPayloadOption(idx.next(), None)
        .bind(idx.next(), true)
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      (if (lastBatch && !resetSequenceNumber) {
         executor
           .update(s"delete [$persistenceId] and insert marker") { connection =>
             Vector(
               connection
                 .createStatement(deleteEventsSql(entityType, slice))
                 .bind(0, persistenceId)
                 .bind(1, from)
                 .bind(2, to),
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)
       } else {
         executor
           .updateOne(s"delete [$persistenceId]") { connection =>
             connection
               .createStatement(deleteEventsSql(entityType, slice))
               .bind(0, persistenceId)
               .bind(1, from)
               .bind(2, to)
           }
       }).map(deletedRows =>
        if (log.isDebugEnabled) {
          log.debug(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows,
            persistenceId,
            from,
            to)
        })(ExecutionContext.parasitic)
    }

    val batchSize = settings.cleanupSettings.eventsJournalDeleteBatchSize

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] = {
      if (from + batchSize > maxTo) {
        deleteBatch(from, maxTo, true)
      } else {
        val to = from + batchSize - 1
        deleteBatch(from, to, false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }
    }

    for {
      toSeqNr <- highestSeqNrForDelete(persistenceId, toSequenceNr)
      fromSeqNr <- lowestSequenceNrForDelete(persistenceId, toSeqNr, batchSize)
      _ <- deleteInBatches(fromSeqNr, toSeqNr)
    } yield ()
  }

  override def deleteEventsBefore(persistenceId: String, timestamp: Instant): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor
      .updateOne(s"delete [$persistenceId]") { connection =>
        connection
          .createStatement(deleteEventsByPersistenceIdBeforeTimestampSql(entityType, slice))
          .bind(0, persistenceId)
          .bindTimestamp(1, timestamp)
      }
      .map(deletedRows =>
        log.debug("Deleted [{}] events for persistenceId [{}], before [{}]", deletedRows, persistenceId, timestamp))(
        ExecutionContext.parasitic)
  }

  override def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): Future[Unit] = {
    val executor = executorProvider.executorFor(slice)
    executor
      .updateOne(s"delete [$entityType]") { connection =>
        connection
          .createStatement(deleteEventsBySliceBeforeTimestampSql(entityType, slice))
          .bind(0, slice)
          .bind(1, entityType)
          .bindTimestamp(2, timestamp)
      }
      .map(deletedRows =>
        log.debug(
          "Deleted [{}] events for entityType [{}], slice [{}], before [{}]",
          deletedRows,
          entityType,
          slice,
          timestamp))(ExecutionContext.parasitic)
  }

}
