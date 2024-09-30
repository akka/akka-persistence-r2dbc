/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
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
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresJournalDao])

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
  protected def log: Logger = PostgresJournalDao.log

  protected val persistenceExt: Persistence = Persistence(system)

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  protected def journalTable(slice: Int): String = settings.journalTableWithSchema(slice)

  protected def insertEventWithParameterTimestampSql(slice: Int): String =
    sqlCache.get(slice, "insertEventWithParameterTimestampSql") {
      val table = journalTable(slice)
      val baseSql = insertEvenBaseSql(table)
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql ?) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(?, ${timestampSubSelect(table)})) RETURNING db_timestamp"
    }

  private def insertEventWithTransactionTimestampSql(slice: Int) =
    sqlCache.get(slice, "insertEventWithTransactionTimestampSql") {
      val table = journalTable(slice)
      val baseSql = insertEvenBaseSql(table)
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql CURRENT_TIMESTAMP) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(CURRENT_TIMESTAMP, ${timestampSubSelect(table)})) RETURNING db_timestamp"
    }

  private def insertEvenBaseSql(table: String) = {
    s"INSERT INTO $table " +
    "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
  }

  // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
  // always increasing for a pid (time not going backwards).
  // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
  private def timestampSubSelect(table: String) =
    s"(SELECT db_timestamp + '1 microsecond'::interval FROM $table " +
    "WHERE persistence_id = ? AND seq_nr = ?)"

  private def selectHighestSequenceNrSql(slice: Int) =
    sqlCache.get(slice, "selectHighestSequenceNrSql") {
      sql"""
      SELECT MAX(seq_nr) from ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr >= ?"""
    }

  private def selectLowestSequenceNrSql(slice: Int) =
    sqlCache.get(slice, "selectLowestSequenceNrSql") {
      sql"""
      SELECT MIN(seq_nr) from ${journalTable(slice)}
      WHERE persistence_id = ?"""
    }

  private def deleteEventsSql(slice: Int) =
    sqlCache.get(slice, "deleteEventsSql") {
      sql"""
      DELETE FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""
    }

  protected def insertDeleteMarkerSql(slice: Int, timestamp: String = "CURRENT_TIMESTAMP"): String = {
    // timestamp param doesn't have to be part of cache key because it's just different for different dialects
    sqlCache.get(slice, "insertDeleteMarkerSql") {
      sql"""
      INSERT INTO ${journalTable(slice)}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
      VALUES (?, ?, ?, ?, $timestamp, ?, ?, ?, ?, ?, ?)"""
    }
  }

  private def deleteEventsByPersistenceIdBeforeTimestampSql(slice: Int) =
    sqlCache.get(slice, "deleteEventsByPersistenceIdBeforeTimestampSql") {
      sql"""
      DELETE FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND db_timestamp < ?"""
    }

  private def deleteEventsBySliceBeforeTimestampSql(slice: Int) =
    sqlCache.get(slice, "deleteEventsBySliceBeforeTimestampSql") {
      sql"""
      DELETE FROM ${journalTable(slice)}
      WHERE slice = ? AND entity_type = ? AND db_timestamp < ?"""
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
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql(slice)
      else insertEventWithParameterTimestampSql(slice)

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = executor.updateOneReturning(s"insert [$persistenceId]")(
        connection =>
          bindInsertStatement(connection.createStatement(insertSql), events.head, useTimestampFromDb, previousSeqNr),
        row => row.getTimestamp("db_timestamp"))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, persistenceId)
        }
      result
    } else {
      val result = executor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
        connection =>
          events.foldLeft(connection.createStatement(insertSql)) { (stmt, write) =>
            stmt.add()
            bindInsertStatement(stmt, write, useTimestampFromDb, previousSeqNr)
          },
        row => row.getTimestamp("db_timestamp"))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, persistenceId)
        }
      result.map(_.head)(ExecutionContext.parasitic)
    }
  }

  override def writeEventInTx(event: SerializedJournalRow, connection: Connection): Future[Instant] = {
    val persistenceId = event.persistenceId
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val previousSeqNr = event.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = event.dbTimestamp == Instant.EPOCH

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql(slice)
      else insertEventWithParameterTimestampSql(slice)

    val stmt = bindInsertStatement(connection.createStatement(insertSql), event, useTimestampFromDb, previousSeqNr)
    val result = R2dbcExecutor.updateOneReturningInTx(stmt, row => row.getTimestamp("db_timestamp"))
    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] event for persistenceId [{}]", 1, persistenceId)
      }
    result
  }

  private def bindInsertStatement(
      stmt: Statement,
      write: SerializedJournalRow,
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): Statement = {
    stmt
      .bind(0, write.slice)
      .bind(1, write.entityType)
      .bind(2, write.persistenceId)
      .bind(3, write.seqNr)
      .bind(4, write.writerUuid)
      .bind(5, "") // FIXME event adapter
      .bind(6, write.serId)
      .bind(7, write.serManifest)
      .bindPayload(8, write.payload.get)

    if (write.tags.isEmpty)
      stmt.bindTagsNull(9)
    else
      stmt.bindTags(9, write.tags)

    // optional metadata
    write.metadata match {
      case Some(m) =>
        stmt
          .bind(10, m.serId)
          .bind(11, m.serManifest)
          .bind(12, m.payload)
      case None =>
        stmt
          .bindNull(10, classOf[Integer])
          .bindNull(11, classOf[String])
          .bindNull(12, classOf[Array[Byte]])
    }

    if (useTimestampFromDb) {
      if (!settings.dbTimestampMonotonicIncreasing)
        stmt
          .bind(13, write.persistenceId)
          .bind(14, previousSeqNr)
    } else {
      if (settings.dbTimestampMonotonicIncreasing)
        stmt
          .bindTimestamp(13, write.dbTimestamp)
      else
        stmt
          .bindTimestamp(13, write.dbTimestamp)
          .bind(14, write.persistenceId)
          .bind(15, previousSeqNr)
    }

    stmt
  }

  override def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val result = executor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectHighestSequenceNrSql(slice))
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
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val result = executor
      .select(s"select lowest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectLowestSequenceNrSql(slice))
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
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)

    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val idx = Iterator.range(0, Int.MaxValue)
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val stmt = connection.createStatement(insertDeleteMarkerSql(slice))
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
               connection.createStatement(deleteEventsSql(slice)).bind(0, persistenceId).bind(1, from).bind(2, to),
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)
       } else {
         executor
           .updateOne(s"delete [$persistenceId]") { connection =>
             connection.createStatement(deleteEventsSql(slice)).bind(0, persistenceId).bind(1, from).bind(2, to)
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
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor
      .updateOne(s"delete [$persistenceId]") { connection =>
        connection
          .createStatement(deleteEventsByPersistenceIdBeforeTimestampSql(slice))
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
          .createStatement(deleteEventsBySliceBeforeTimestampSql(slice))
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
