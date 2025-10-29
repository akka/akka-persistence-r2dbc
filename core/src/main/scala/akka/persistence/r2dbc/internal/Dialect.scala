/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import akka.persistence.r2dbc.R2dbcSettings
import com.typesafe.config.Config
import io.r2dbc.spi.ConnectionFactory
import scala.concurrent.ExecutionContext

import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider

/**
 * INTERNAL API
 */
@InternalStableApi
private[r2dbc] trait Dialect {

  /**
   * Lower case, unique name of the dialect, should match the parsed name in R2DBSettings
   */
  def name: String

  /**
   * Possibly override settings from config with config that is specific to the dialect and where config could be
   * incorrect.
   */
  def adaptSettings(settings: R2dbcSettings): R2dbcSettings = settings

  def daoExecutionContext(settings: R2dbcSettings, system: ActorSystem[_]): ExecutionContext

  def createConnectionFactory(config: Config, optionsProvider: ConnectionFactoryOptionsProvider): ConnectionFactory

  def createJournalDao(executorProvider: R2dbcExecutorProvider): JournalDao

  def createQueryDao(executorProvider: R2dbcExecutorProvider): QueryDao

  def createSnapshotDao(executorProvider: R2dbcExecutorProvider): SnapshotDao

  def createDurableStateDao(executorProvider: R2dbcExecutorProvider): DurableStateDao
}
