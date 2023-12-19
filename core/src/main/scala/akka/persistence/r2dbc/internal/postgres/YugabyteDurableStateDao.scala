/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import scala.concurrent.ExecutionContext

import io.r2dbc.spi._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class YugabyteDurableStateDao(
    settings: R2dbcSettings,
    connectionFactory: ConnectionFactory,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, connectionFactory, dialect) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[YugabyteDurableStateDao])

  override protected def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    s"slice BETWEEN $minSlice AND $maxSlice"
  }

}
