/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import java.time.Instant
import java.time.LocalDateTime

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.TimestampCodec.SqlServerTimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresQueryDao
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerQueryDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerQueryDao])

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresQueryDao(settings, connectionFactory) {

  override def sqlFalse = "0"

  // def because of order of initialization
  override def log = SqlServerQueryDao.log
  override protected def sqlDbTimestamp = "SYSUTCDATETIME()"

  override protected val selectEventsSql =
    sql"""
      SELECT TOP(@limit) slice, entity_type, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      from $journalTable
      WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to
      AND deleted = $sqlFalse
      ORDER BY seq_nr"""

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

  override protected def selectBucketsSql(minSlice: Int, maxSlice: Int): String = {
    sql"""
        SELECT TOP(@limit) bucket, count(*) as count from
         (select DATEDIFF(s,'1970-01-01 00:00:00', db_timestamp)/10 as bucket
          FROM $journalTable
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
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= @until" else ""

    // we know this is a LocalDateTime, so the cast should be ok
    def localNow: LocalDateTime = timestampCodec.encode(timestampCodec.instantNow()).asInstanceOf[LocalDateTime]

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < DATEADD(ms, -${behindCurrentTime.toMillis}, CAST('$localNow' as datetime2(6)))"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id "
      else
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
        $selectColumns
        FROM $journalTable
        WHERE entity_type = @entityType
        AND ${sliceCondition(minSlice, maxSlice)}
        AND db_timestamp >= @from $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
        AND deleted = $sqlFalse
        ORDER BY db_timestamp, seq_nr"""
  }

  override protected def bindEventsBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant]): Statement = {
    stmt
      .bind("@limit", settings.querySettings.bufferSize)
      .bind("@entityType", entityType)
      .bindTimestamp("@from", fromTimestamp)
    toTimestamp.foreach(timestamp => stmt.bindTimestamp("@until", timestamp))
    stmt
  }

  override protected val persistenceIdsForEntityTypeAfterSql: String =
    sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE @persistenceIdLike AND persistence_id > @persistenceId
         ) as sub  ORDER BY persistence_id"""

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

  override protected val persistenceIdsForEntityTypeSql: String =
    sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE @persistenceIdLike
         ) as sub ORDER BY persistence_id"""

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
  override protected val allPersistenceIdsAfterSql: String =
    sql"""
         SELECT TOP(@limit) persistence_id FROM (
          SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id > @persistenceId
         ) as sub  ORDER BY persistence_id"""

  override protected val allPersistenceIdsSql: String =
    sql"SELECT TOP(@limit) persistence_id FROM (SELECT DISTINCT(persistence_id) from $journalTable) as sub  ORDER BY persistence_id"

  override def currentDbTimestamp(): Future[Instant] = Future.successful(timestampCodec.instantNow())

}
