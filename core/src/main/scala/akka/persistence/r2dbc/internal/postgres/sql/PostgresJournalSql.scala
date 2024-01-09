/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.sql

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import io.r2dbc.spi.{ Row, Statement }

import java.time.Instant

class PostgresJournalSql(journalSettings: R2dbcSettings)(implicit statePayloadCodec: PayloadCodec)
    extends BaseJournalSql {

  private val journalTable = journalSettings.journalTableWithSchema

  private val (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql) = {
    val baseSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

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
        sql"$baseSql CURRENT_TIMESTAMP) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(CURRENT_TIMESTAMP, $timestampSubSelect)) RETURNING db_timestamp"
    }

    (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql)
  }

  override def insertSql(useTimestampFromDb: Boolean): String = if (useTimestampFromDb)
    insertEventWithTransactionTimestampSql
  else insertEventWithParameterTimestampSql

  private val deleteEventsBySliceBeforeTimestampSql =
    sql"""
    DELETE FROM $journalTable
    WHERE slice = ? AND entity_type = ? AND db_timestamp < ?"""

  def deleteEventsBySliceBeforeTimestamp(
      createStatement: String => Statement,
      slice: Int,
      entityType: String,
      timestamp: Instant): Statement = {
    createStatement(deleteEventsBySliceBeforeTimestampSql)
      .bind(0, slice)
      .bind(1, entityType)
      .bind(2, timestamp)
  }

  def deleteEventsByPersistenceIdBeforeTimestamp(
      createStatement: String => Statement,
      persistenceId: String,
      timestamp: Instant): Statement = {
    createStatement(deleteEventsByPersistenceIdBeforeTimestampSql)
      .bind(0, persistenceId)
      .bind(1, timestamp)
  }

  private val deleteEventsByPersistenceIdBeforeTimestampSql =
    sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND db_timestamp < ?"""

  def bindInsertForWriteEvent(
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
      stmt.bindNull(9, classOf[Array[String]])
    else
      stmt.bind(9, write.tags.toArray)

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
      if (!journalSettings.dbTimestampMonotonicIncreasing)
        stmt
          .bind(13, write.persistenceId)
          .bind(14, previousSeqNr)
    } else {
      if (journalSettings.dbTimestampMonotonicIncreasing)
        stmt
          .bind(13, write.dbTimestamp)
      else
        stmt
          .bind(13, write.dbTimestamp)
          .bind(14, write.persistenceId)
          .bind(15, previousSeqNr)
    }

    stmt

  }

  override def parseInsertForWriteEvent(row: Row): Instant = row.get(0, classOf[Instant])

  override val selectHighestSequenceNrSql =
    sql"""
    SELECT MAX(seq_nr) from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ?"""

  override def bindSelectHighestSequenceNrSql(stmt: Statement, persistenceId: String, fromSequenceNr: Long): Statement =
    stmt
      .bind(0, persistenceId)
      .bind(1, fromSequenceNr)

  override val selectLowestSequenceNrSql =
    sql"""
  SELECT MIN(seq_nr) from $journalTable
  WHERE persistence_id = ?"""

  override def bindSelectLowestSequenceNrSql(stmt: Statement, persistenceId: String): Statement =
    stmt
      .bind(0, persistenceId)

  override val insertDeleteMarkerSql =
    sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)"""

  override def bindForInsertDeleteMarkerSql(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      deleteMarkerSeqNr: Long): Statement = {
    stmt
      .bind(0, slice)
      .bind(1, entityType)
      .bind(2, persistenceId)
      .bind(3, deleteMarkerSeqNr)
      .bind(4, "")
      .bind(5, "")
      .bind(6, 0)
      .bind(7, "")
      .bindPayloadOption(8, None)
      .bind(9, true)
  }

  override val deleteEventsSql =
    sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""

  override def bindForDeleteEventsSql(stmt: Statement, persistenceId: String, from: Long, to: Long): Statement =
    stmt.bind(0, persistenceId).bind(1, from).bind(2, to)
}
