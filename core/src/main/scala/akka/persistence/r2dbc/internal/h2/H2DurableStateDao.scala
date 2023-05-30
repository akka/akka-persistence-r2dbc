/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class H2DurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, connectionFactory) {

  override protected val log: Logger = LoggerFactory.getLogger(classOf[H2DurableStateDao])

  override protected def stateBySlicesRangeSql(
      entityType: String,
      maxDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    def maxDbTimestampParamCondition =
      if (maxDbTimestampParam) s"AND db_timestamp < ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second" // FIXME h2 interval
      else ""

    val selectColumns =
      if (backtracking)
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id "
      else
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
      $selectColumns
      FROM $stateTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      ORDER BY db_timestamp, revision
      LIMIT ?"""
  }

}
