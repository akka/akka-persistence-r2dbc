/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresQueryDao
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.query.scaladsl.QueryDao
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.{ ConnectionFactory, Row }
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
@InternalApi
object H2QueryDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[H2QueryDao])
  private def tagsFromDb(array: AnyRef): Set[String] = array match {
    case null              => Set.empty[String]
    case entries: Array[_] => entries.toSet.asInstanceOf[Set[String]]
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class H2QueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresQueryDao(settings, connectionFactory) {
  import H2QueryDao._
  import H2JournalDao.readMetadata

  override protected def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
      $selectColumns
      FROM $journalTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  override protected val selectOneEventSql = sql"""
    SELECT slice, entity_type, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = false"""

  override protected val selectEventsSql = sql"""
    SELECT slice, entity_type, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
    AND deleted = false
    ORDER BY seq_nr
    LIMIT ?"""

  override protected def tagsFromDb(row: Row, columnName: String): Set[String] =
    H2QueryDao.tagsFromDb(row.get("tags", classOf[AnyRef]))

}
