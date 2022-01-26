/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object JournalDao {
  val log: Logger = LoggerFactory.getLogger(classOf[JournalDao])
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedJournalRow(
      slice: Int,
      entityType: String,
      persistenceId: String,
      seqNr: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Option[Array[Byte]],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      metadata: Option[SerializedEventMetadata])
      extends BySliceQuery.SerializedRow

  final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

  def readMetadata(row: Row): Option[SerializedEventMetadata] = {
    row.get("meta_payload", classOf[Array[Byte]]) match {
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get("meta_ser_id", classOf[Integer]),
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
private[r2dbc] class JournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {

  import JournalDao.SerializedJournalRow
  import JournalDao.log

  private val persistenceExt = Persistence(system)

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, journalSettings.logDbCallsExceeding)(ec, system)

  private val journalTable = journalSettings.journalTableWithSchema

  private val (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql) = {
    val baseSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

    // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
    // always increasing for a pid (time not going backwards).
    // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
    def timestampSubSelect =
      s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable " +
      "WHERE persistence_id = ? AND seq_nr = ?)"

    val insertEventWithParameterTimestampSql = {
      if (journalSettings.dbTimestampMonotonicIncreasing)
        sql"$baseSql ?) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(?, $timestampSubSelect)) RETURNING db_timestamp"
    }

    val insertEventWithTransactionTimestampSql = {
      if (journalSettings.dbTimestampMonotonicIncreasing)
        sql"$baseSql transaction_timestamp()) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(transaction_timestamp(), $timestampSubSelect)) RETURNING db_timestamp"
    }

    (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql)
  }

  private val selectHighestSequenceNrSql = sql"""
    SELECT MAX(seq_nr) from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ?"""

  private val deleteEventsSql = sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr <= ?"""
  private val insertDeleteMarkerSql = sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES (?, ?, ?, ?, transaction_timestamp(), ?, ?, ?, ?, ?, ?)"""

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
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, write.slice)
        .bind(1, write.entityType)
        .bind(2, write.persistenceId)
        .bind(3, write.seqNr)
        .bind(4, write.writerUuid)
        .bind(5, "") // FIXME event adapter
        .bind(6, write.serId)
        .bind(7, write.serManifest)
        .bind(8, write.payload.get)

      // optional metadata
      write.metadata match {
        case Some(m) =>
          stmt
            .bind(9, m.serId)
            .bind(10, m.serManifest)
            .bind(11, m.payload)
        case None =>
          stmt
            .bindNull(9, classOf[Integer])
            .bindNull(10, classOf[String])
            .bindNull(11, classOf[Array[Byte]])
      }

      if (useTimestampFromDb) {
        if (!journalSettings.dbTimestampMonotonicIncreasing)
          stmt
            .bind(12, write.persistenceId)
            .bind(13, previousSeqNr)
      } else {
        if (journalSettings.dbTimestampMonotonicIncreasing)
          stmt
            .bind(12, write.dbTimestamp)
        else
          stmt
            .bind(12, write.dbTimestamp)
            .bind(13, write.persistenceId)
            .bind(14, previousSeqNr)
      }

      stmt
    }

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql
      else insertEventWithParameterTimestampSql

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => bind(connection.createStatement(insertSql), events.head),
        row => row.get(0, classOf[Instant]))
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
        row => row.get(0, classOf[Instant]))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      result.map(_.head)(ExecutionContext.parasitic)
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectHighestSequenceNrSql)
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

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    val deleteMarkerSeqNrFut =
      if (toSequenceNr == Long.MaxValue)
        readHighestSequenceNr(persistenceId, 0L)
      else
        Future.successful(toSequenceNr)

    deleteMarkerSeqNrFut.flatMap { deleteMarkerSeqNr =>
      def bindDeleteMarker(stmt: Statement): Statement = {
        stmt
          .bind(0, slice)
          .bind(1, entityType)
          .bind(2, persistenceId)
          .bind(3, deleteMarkerSeqNr)
          .bind(4, "")
          .bind(5, "")
          .bind(6, 0)
          .bind(7, "")
          .bind(8, Array.emptyByteArray)
          .bind(9, true)
      }

      val result = r2dbcExecutor.update(s"delete [$persistenceId]") { connection =>
        Vector(
          connection
            .createStatement(deleteEventsSql)
            .bind(0, persistenceId)
            .bind(1, toSequenceNr),
          bindDeleteMarker(connection.createStatement(insertDeleteMarkerSql)))
      }

      if (log.isDebugEnabled)
        result.foreach(updatedRows =>
          log.debug("Deleted [{}] events for persistenceId [{}]", updatedRows.head, persistenceId))

      result.map(_ => ())(ExecutionContext.parasitic)
    }
  }

}
