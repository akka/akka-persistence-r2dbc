/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.postgres.PostgresQueryDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class H2QueryDao(executorProvider: R2dbcExecutorProvider) extends PostgresQueryDao(executorProvider) {
  import settings.codecSettings.JournalImplicits._
  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2QueryDao])

  override protected def eventsBySlicesRangeSql(
      fromSeqNrParam: Boolean,
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    // not caching, too many combinations

    def fromSeqNrParamCondition =
      if (fromSeqNrParam) "AND (db_timestamp != ? OR seq_nr >= ?)" else ""

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags, event_ser_id "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
      $selectColumns
      FROM ${journalTable(minSlice)}
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $fromSeqNrParamCondition $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }
}
