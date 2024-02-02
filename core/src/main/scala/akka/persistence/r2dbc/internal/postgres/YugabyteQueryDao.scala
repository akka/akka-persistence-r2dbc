/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] final class YugabyteQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresQueryDao(settings, connectionFactory) {

  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[YugabyteQueryDao])

  override protected def sliceCondition(minSlice: Int, maxSlice: Int): String = {
    s"slice BETWEEN $minSlice AND $maxSlice"
  }

}
