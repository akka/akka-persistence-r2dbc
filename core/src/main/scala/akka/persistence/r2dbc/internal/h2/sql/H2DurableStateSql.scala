/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2.sql

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichStatement => TimestampRichStatement }
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.internal.postgres.sql.PostgresDurableStateSql
import akka.persistence.r2dbc.internal.{ PayloadCodec, TimestampCodec }
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

import java.lang
import java.time.Instant
import scala.collection.immutable
import scala.concurrent.duration.{ Duration, FiniteDuration }

class H2DurableStateSql(settings: R2dbcSettings)(implicit
    statePayloadCodec: PayloadCodec,
    timestampCodec: TimestampCodec)
    extends PostgresDurableStateSql(settings) {
  protected override def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second"
    else ""

}
