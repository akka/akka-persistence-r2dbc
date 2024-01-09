/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.{
  InstantFactory,
  JournalDao,
  PayloadCodec,
  R2dbcExecutor,
  SerializedEventMetadata
}
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao
import akka.persistence.r2dbc.internal.postgres.sql.BaseJournalSql
import akka.persistence.r2dbc.internal.sqlserver.sql.SqlServerJournalSql
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.LocalDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerJournalDao])

  val TRUE = 1

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresJournalDao(settings, connectionFactory) {

  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.use-app-timestamp=on")
  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.db-timestamp-monotonic-increasing = off")

  override def log: Logger = SqlServerJournalDao.log

  override val journalSql: BaseJournalSql = new SqlServerJournalSql(settings)

}
