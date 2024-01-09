/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver.sql

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.{ JournalDao, PayloadCodec, TimestampCodec }
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichRow, RichStatement => TimestampRichStatement }
import akka.persistence.r2dbc.internal.postgres.sql.BaseJournalSql
import akka.persistence.r2dbc.internal.sqlserver.{ SqlServerDialectHelper, SqlServerJournalDao }
import io.r2dbc.spi.{ Row, Statement }

import java.time.{ Instant, LocalDateTime }

class SqlServerJournalSql(journalSettings: R2dbcSettings)(implicit
    statePayloadCodec: PayloadCodec,
    timestampCodec: TimestampCodec)
    extends BaseJournalSql {

  private val helper = new SqlServerDialectHelper(journalSettings.connectionFactorySettings.config)

  private val journalTable = journalSettings.journalTableWithSchema

  private val deleteEventsBySliceBeforeTimestampSql =
    sql"""
  DELETE FROM $journalTable
  WHERE slice = @slice AND entity_type = @entityType AND db_timestamp < @dbTimestamp"""

  override def deleteEventsBySliceBeforeTimestamp(
      createStatement: String => Statement,
      slice: Int,
      entityType: String,
      timestamp: Instant): Statement = {
    createStatement(deleteEventsBySliceBeforeTimestampSql)
      .bind("@slice", slice)
      .bind("@entityType", entityType)
      .bindTimestamp("@dbTimestamp", timestamp)
  }

  private val deleteEventsByPersistenceIdBeforeTimestampSql =
    sql"""
  DELETE FROM $journalTable
  WHERE persistence_id = @persistenceId AND db_timestamp < @timestamp"""

  // can this be inherited by PostgresJournalSql?
  def deleteEventsByPersistenceIdBeforeTimestamp(
      createStatement: String => Statement,
      persistenceId: String,
      timestamp: Instant): Statement = {
    createStatement(deleteEventsByPersistenceIdBeforeTimestampSql)
      .bind("@persistenceId", persistenceId)
      .bindTimestamp("@timestamp", timestamp)
  }

  override def bindInsertForWriteEvent(
      stmt: Statement,
      write: JournalDao.SerializedJournalRow,
      useTimestampFromDb: Boolean,
      previousSeqNr: Long): Statement = {

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
      stmt.bind("@tags", helper.tagsToDb(write.tags))

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
    stmt.bindTimestamp("@dbTimestamp", write.dbTimestamp)
  }

  /**
   * Param `useTimestampFromDb` is ignored in sqlserver
   */
  override def insertSql(useTimestampFromDb: Boolean): String =
    sql"""
        INSERT INTO $journalTable
        (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
        OUTPUT inserted.db_timestamp
        VALUES (@slice, @entityType, @persistenceId, @seqNr, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @tags, @metaSerId, @metaSerManifest, @metaSerPayload, @dbTimestamp)"""

  override def parseInsertForWriteEvent(row: Row): Instant = row.getTimestamp()

  override val selectHighestSequenceNrSql =
    sql"""
    SELECT MAX(seq_nr) as max_seq_nr from $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr >= @seqNr"""

  override def bindSelectHighestSequenceNrSql(stmt: Statement, persistenceId: String, fromSequenceNr: Long): Statement =
    stmt
      .bind("@persistenceId", persistenceId)
      .bind("@seqNr", fromSequenceNr)

  override val selectLowestSequenceNrSql =
    sql"""
    SELECT MIN(seq_nr) as min_seq_nr from $journalTable
    WHERE persistence_id = @persistenceId"""

  override def bindSelectLowestSequenceNrSql(stmt: Statement, persistenceId: String): Statement =
    stmt
      .bind("@persistenceId", persistenceId)

  override val insertDeleteMarkerSql =
    sql"""
    INSERT INTO $journalTable(slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES(@slice, @entityType, @persistenceId, @deleteMarkerSeqNr, @now, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @deleted )"""

  override def bindForInsertDeleteMarkerSql(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      deleteMarkerSeqNr: Long): Statement = {
    stmt
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
      .bind("@now", timestampCodec.now())
  }

  override val deleteEventsSql =
    sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to"""

  override def bindForDeleteEventsSql(stmt: Statement, persistenceId: String, from: Long, to: Long): Statement = stmt
    .bind("@persistenceId", persistenceId)
    .bind("@from", from)
    .bind("@to", to)
}
