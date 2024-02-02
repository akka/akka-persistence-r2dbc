/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class H2DurableStateDao(
    settings: R2dbcSettings,
    connectionFactory: ConnectionFactory,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, connectionFactory, dialect) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2DurableStateDao])

  protected override def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis.toDouble / 1000}' second"
    else ""
}
