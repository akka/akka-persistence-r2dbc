/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.sqlserver

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresQueryDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerQueryDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerQueryDao])

}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class SqlServerQueryDao(executorProvider: R2dbcExecutorProvider)
    extends PostgresQueryDao(executorProvider) {
  import settings.codecSettings.JournalImplicits._

  override def sqlFalse = "0"

  // def because of order of initialization
  override def log = SqlServerQueryDao.log
  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  override protected def sqlDbTimestamp = "SYSUTCDATETIME()"

  override protected def selectEventsSql(slice: Int): String =
    sqlCache.get(slice, "selectEventsSql") {
      sql"""
      SELECT TOP(@limit) slice, entity_type, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      from ${journalTable(slice)}
      WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to
      AND deleted = $sqlFalse
      ORDER BY seq_nr"""
    }

  override protected def selectEventsIncludeDeletedSql(slice: Int): String =
    sqlCache.get(slice, "selectEventsIncludeDeletedSql") {
      sql"""
      SELECT TOP(@limit) slice, entity_type, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags, deleted
      from ${journalTable(slice)}
      WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to
      ORDER BY seq_nr"""
    }

  /**
   * custom binding because the first param in the query is @limit (or '0' when using positional binding)
   *
   * Should we use positional binding instead? Named binding is preferred in sqlserver (and slightly cheaper), but this
   * project uses positional.
   */
  override protected def bindSelectEventsSql(
      stmt: Statement,
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      bufferSize: Int): Statement =
    stmt
      .bind("@limit", settings.querySettings.bufferSize)
      .bind("@persistenceId", persistenceId)
      .bind("@from", fromSequenceNr)
      .bind("@to", toSequenceNr)

  override protected def selectBucketsSql(minSlice: Int, maxSlice: Int): String =
    sqlCache.get(minSlice, s"selectBucketsSql-$minSlice-$maxSlice") {
      sql"""
        SELECT TOP(@limit) bucket, count(*) as count from
         (select DATEDIFF(s,'1970-01-01 00:00:00', db_timestamp)/10 as bucket
          FROM ${journalTable(minSlice)}
          WHERE entity_type = @entityType
          AND ${sliceCondition(minSlice, maxSlice)}
          AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
          AND deleted = $sqlFalse) as sub
        GROUP BY bucket ORDER BY bucket
        """
    }

  override protected def bindSelectBucketsSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement = {
    stmt
      .bind("@limit", limit)
      .bind("@entityType", entityType)
      .bindTimestamp("@fromTimestamp", fromTimestamp)
      .bindTimestamp("@toTimestamp", toTimestamp)
  }

  override protected def eventsBySlicesRangeSql(
      fromSeqNrParam: Boolean,
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    // not caching, too many combinations

    def fromSeqNrParamCondition =
      if (fromSeqNrParam) "AND (db_timestamp != @from OR seq_nr >= @fromSeqNr)" else ""

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= @until" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < DATEADD(ms, -${behindCurrentTime.toMillis}, SYSUTCDATETIME())"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id "
      else
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
        $selectColumns
        FROM ${journalTable(minSlice)}
        WHERE entity_type = @entityType
        AND ${sliceCondition(minSlice, maxSlice)}
        AND db_timestamp >= @from $fromSeqNrParamCondition $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
        AND deleted = $sqlFalse
        ORDER BY db_timestamp, seq_nr"""
  }

  override protected def bindEventsBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      fromSeqNr: Option[Long],
      toTimestamp: Option[Instant]): Statement = {
    stmt
      .bind("@limit", settings.querySettings.bufferSize)
      .bind("@entityType", entityType)
      .bindTimestamp("@from", fromTimestamp)
    fromSeqNr.foreach(seqNr => stmt.bind("@fromSeqNr", seqNr))
    toTimestamp.foreach(timestamp => stmt.bindTimestamp("@until", timestamp))
    stmt
  }

  override protected def selectLastEventSql(slice: Int): String =
    sqlCache.get(slice, "selectLastEventSql") {
      sql"""
      SELECT TOP(1) entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr <= ? AND deleted = $sqlFalse
      ORDER BY seq_nr DESC
      """
    }

  override protected def selectLastEventIncludeDeletedSql(slice: Int): String =
    sqlCache.get(slice, "selectLastEventIncludeDeletedSql") {
      sql"""
      SELECT TOP(1) entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags, deleted
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr <= ?
      ORDER BY seq_nr DESC
      """
    }

  override protected def persistenceIdsForEntityTypeAfterSql(minSlice: Int): String =
    sqlCache.get(minSlice, "persistenceIdsForEntityTypeAfterSql") {
      sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id LIKE @persistenceIdLike AND persistence_id > @persistenceId
         ) as sub  ORDER BY persistence_id"""
    }

  override protected def bindPersistenceIdsForEntityTypeAfterSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      afterPersistenceId: String,
      limit: Long): Statement = {
    stmt
      .bind("@limit", limit)
      .bind("@persistenceIdLike", entityType + likeStmtPostfix)
      .bind("@persistenceId", afterPersistenceId)
  }

  override protected def persistenceIdsForEntityTypeSql(minSlice: Int): String =
    sqlCache.get(minSlice, "persistenceIdsForEntityTypeSql") {
      sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id LIKE @persistenceIdLike
         ) as sub ORDER BY persistence_id"""
    }

  override protected def bindPersistenceIdsForEntityTypeSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      limit: Long): Statement = {
    stmt
      .bind("@limit", limit)
      .bind("@persistenceIdLike", entityType + likeStmtPostfix)
  }

  override protected def bindAllPersistenceIdsAfterSql(
      stmt: Statement,
      afterPersistenceId: String,
      limit: Long): Statement = {
    stmt
      .bind("@limit", limit)
      .bind("@persistenceId", afterPersistenceId)
  }
  override protected def allPersistenceIdsAfterSql(minSlice: Int): String =
    sqlCache.get(minSlice, "allPersistenceIdsAfterSql") {
      sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id > @persistenceId
         ) as sub  ORDER BY persistence_id"""
    }

  override protected def allPersistenceIdsSql(minSlice: Int): String =
    sqlCache.get(minSlice, "allPersistenceIdsSql") {
      sql"SELECT TOP(@limit) persistence_id FROM (SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)}) as sub  ORDER BY persistence_id"
    }

  override def currentDbTimestamp(slice: Int): Future[Instant] = Future.successful(InstantFactory.now())

}
