/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import scala.concurrent.ExecutionContext

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class YugabyteDurableStateDao(
    settings: R2dbcSettings,
    executorProvider: R2dbcExecutorProvider,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, executorProvider, dialect) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[YugabyteDurableStateDao])

  override protected def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    s"slice BETWEEN $minSlice AND $maxSlice"
  }

}
