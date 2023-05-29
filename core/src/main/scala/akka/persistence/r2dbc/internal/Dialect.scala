/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.query.scaladsl.QueryDao
import akka.persistence.r2dbc.snapshot.SnapshotDao
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao
import io.r2dbc.spi.ConnectionFactory

import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 */
@InternalStableApi
private[r2dbc] trait Dialect {
  def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao

  def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): QueryDao

  def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): SnapshotDao

  def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): DurableStateDao
}
