/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class YugabyteDurableStateDao(executorProvider: R2dbcExecutorProvider, dialect: Dialect)
    extends PostgresDurableStateDao(executorProvider, dialect) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[YugabyteDurableStateDao])

  override protected def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    s"slice BETWEEN $minSlice AND $maxSlice"
  }

}
