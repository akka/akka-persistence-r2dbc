/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SerializedEventMetadata
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.LocalDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerJournalDao])

  val TRUE = 1

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
private[r2dbc] class SqlServerJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends JournalDao {

  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)
  import helper._

  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.use-app-timestamp=on")
  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.db-timestamp-monotonic-increasing = off")

  import JournalDao.SerializedJournalRow
  protected def log: Logger = SqlServerJournalDao.log

  private val persistenceExt = Persistence(system)

  protected val r2dbcExecutor =
    new R2dbcExecutor(
      connectionFactory,
      log,
      settings.logDbCallsExceeding,
      settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  protected val journalTable = settings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec

  private val insertEventWithParameterTimestampSql = sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
    OUTPUT inserted.db_timestamp
    VALUES (@slice, @entityType, @persistenceId, @seqNr, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @tags, @metaSerId, @metaSerManifest, @metaSerPayload, @dbTimestamp)"""

  private val selectHighestSequenceNrSql = sql"""
    SELECT MAX(seq_nr) as max_seq_nr from $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr >= @seqNr"""

  private val selectLowestSequenceNrSql =
    sql"""
    SELECT MIN(seq_nr) as min_seq_nr from $journalTable
    WHERE persistence_id = @persistenceId"""

  private val deleteEventsSql = sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to"""

  private val insertDeleteMarkerSql = sql"""
    INSERT INTO $journalTable(slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES(@slice, @entityType, @persistenceId, @deleteMarkerSeqNr, @now, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @deleted )"""

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
  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind("@slice", write.slice)
        .bind("@entityType", write.entityType)
        .bind("@persistenceId", write.persistenceId)
        .bind("@seqNr", write.seqNr)
        .bind("@writer", write.writerUuid)
        .bind("@adapterManifest", "") // FIXME event adapter
        .bind("@eventSerId", write.serId)
        .bind("@eventSerManifest", write.serManifest)
        .bindPayload("@eventPayload", write.payload.get)

      if (write.tags.isEmpty)
        stmt.bindNull("@tags", classOf[String])
      else
        stmt.bind("@tags", tagsToDb(write.tags))

      // optional metadata
      write.metadata match {
        case Some(m) =>
          stmt
            .bind("@metaSerId", m.serId)
            .bind("@metaSerManifest", m.serManifest)
            .bind("@metaSerPayload", m.payload)
        case None =>
          stmt
            .bindNull("@metaSerId", classOf[Integer])
            .bindNull("@metaSerManifest", classOf[String])
            .bindNull("@metaSerPayload", classOf[Array[Byte]])
      }
      stmt.bind("@dbTimestamp", toDbTimestamp(write.dbTimestamp))
    }

    val insertSql = insertEventWithParameterTimestampSql

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => bind(connection.createStatement(insertSql), events.head),
        row => fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      result
    } else {
      val result = r2dbcExecutor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
        connection =>
          events.foldLeft(connection.createStatement(insertSql)) { (stmt, write) =>
            stmt.add()
            bind(stmt, write)
          },
        row => fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, events.head.persistenceId)
        }
      result.map(_.head)(ExecutionContexts.parasitic)
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectHighestSequenceNrSql)
            .bind("@persistenceId", persistenceId)
            .bind("@seqNr", fromSequenceNr),
        row => {
          val seqNr = row.get("max_seq_nr", classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select lowest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectLowestSequenceNrSql)
            .bind("@persistenceId", persistenceId),
        row => {
          val seqNr = row.get("min_seq_nr", classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

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

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {

    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      connection
        .createStatement(insertDeleteMarkerSql)
        .bind("@slice", slice)
        .bind("@entityType", entityType)
        .bind("@persistenceId", persistenceId)
        .bind("@deleteMarkerSeqNr", deleteMarkerSeqNr)
        .bind("@writer", "")
        .bind("@adapterManifest", "")
        .bind("@eventSerId", 0)
        .bind("@eventSerManifest", "")
        .bindPayloadOption("@eventPayload", None)
        .bind("@deleted", SqlServerJournalDao.TRUE)
        .bind("@now", nowLocalDateTime())
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      (if (lastBatch && !resetSequenceNumber) {
         r2dbcExecutor
           .update(s"delete [$persistenceId] and insert marker") { connection =>
             Vector(
               connection
                 .createStatement(deleteEventsSql)
                 .bind("@persistenceId", persistenceId)
                 .bind("@from", from)
                 .bind("@to", to),
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)
       } else {
         r2dbcExecutor
           .updateOne(s"delete [$persistenceId]") { connection =>
             connection
               .createStatement(deleteEventsSql)
               .bind(0, persistenceId)
               .bind(1, from)
               .bind(2, to)
           }
       }).map(deletedRows =>
        if (log.isDebugEnabled) {
          log.debugN(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows,
            persistenceId,
            from,
            to)
        })(ExecutionContexts.parasitic)
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

}
