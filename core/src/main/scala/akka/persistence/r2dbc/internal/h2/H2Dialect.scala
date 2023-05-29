/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.query.scaladsl.QueryDao
import akka.persistence.r2dbc.snapshot.SnapshotDao
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao
import io.r2dbc.spi.ConnectionFactory

import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object H2Dialect extends Dialect {
  override def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao = new H2JournalDao(settings, connectionFactory)

  override def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): SnapshotDao = new H2SnapshotDao(settings, connectionFactory)

  override def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): QueryDao = new H2QueryDao(settings, connectionFactory)

  override def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): DurableStateDao = new H2DurableStateDao(settings, connectionFactory)
}
