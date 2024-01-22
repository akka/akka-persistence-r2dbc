/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.SnapshotDao
import com.typesafe.config.Config
import io.r2dbc.spi.ConnectionFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object YugabyteDialect extends Dialect {

  override def name: String = "yugabyte"

  override def createConnectionFactory(config: Config): ConnectionFactory =
    PostgresDialect.createConnectionFactory(config)

  override def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): JournalDao =
    new PostgresJournalDao(settings, connectionFactory)(system.executionContext, system)

  override def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): SnapshotDao =
    new YugabyteSnapshotDao(settings, connectionFactory)(system.executionContext, system)

  override def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): QueryDao =
    new YugabyteQueryDao(settings, connectionFactory)(system.executionContext, system)

  override def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): DurableStateDao =
    new YugabyteDurableStateDao(settings, connectionFactory, this)(system.executionContext, system)
}
