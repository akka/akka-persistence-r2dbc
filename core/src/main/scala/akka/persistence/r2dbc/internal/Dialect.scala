/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalStableApi
import akka.persistence.r2dbc.R2dbcSettings
import com.typesafe.config.Config
import io.r2dbc.spi.ConnectionFactory

import scala.concurrent.ExecutionContext

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

  def createConnectionFactory(config: Config): ConnectionFactory

  def createJournalDao(settings: R2dbcSettings, executorProvider: R2dbcExecutorProvider)(implicit
      system: ActorSystem[_]): JournalDao

  def createQueryDao(settings: R2dbcSettings, executorProvider: R2dbcExecutorProvider)(implicit
      system: ActorSystem[_]): QueryDao

  def createSnapshotDao(settings: R2dbcSettings, executorProvider: R2dbcExecutorProvider)(implicit
      system: ActorSystem[_]): SnapshotDao

  def createDurableStateDao(settings: R2dbcSettings, executorProvider: R2dbcExecutorProvider)(implicit
      system: ActorSystem[_]): DurableStateDao
}
