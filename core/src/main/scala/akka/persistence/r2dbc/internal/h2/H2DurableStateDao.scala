/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.h2

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class H2DurableStateDao(executorProvider: R2dbcExecutorProvider, dialect: Dialect)
    extends PostgresDurableStateDao(executorProvider, dialect) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2DurableStateDao])

  protected override def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second"
    else ""
}
