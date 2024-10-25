/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import scala.concurrent.ExecutionContext

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

import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object YugabyteDialect extends Dialect {

  override def name: String = "yugabyte"

  override def createConnectionFactory(
      config: Config,
      optionsProvider: ConnectionFactoryOptionsProvider): ConnectionFactory =
    PostgresDialect.createConnectionFactory(config, optionsProvider)

  override def daoExecutionContext(settings: R2dbcSettings, system: ActorSystem[_]): ExecutionContext =
    system.executionContext

  override def createJournalDao(executorProvider: R2dbcExecutorProvider): JournalDao =
    new PostgresJournalDao(executorProvider)

  override def createSnapshotDao(executorProvider: R2dbcExecutorProvider): SnapshotDao =
    new YugabyteSnapshotDao(executorProvider)

  override def createQueryDao(executorProvider: R2dbcExecutorProvider): QueryDao =
    new YugabyteQueryDao(executorProvider)

  override def createDurableStateDao(executorProvider: R2dbcExecutorProvider): DurableStateDao =
    new YugabyteDurableStateDao(executorProvider, this)
}
