/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import java.time.{ Duration => JDuration }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.util.JavaDurationConverters.JavaDurationOps
import com.typesafe.config.Config
import io.r2dbc.mssql.MssqlConnectionFactoryProvider
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions

import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerDialect extends Dialect {

  private[r2dbc] final class SqlServerConnectionFactorySettings(config: Config) {
    val urlOption: Option[String] =
      Option(config.getString("url"))
        .filter(_.trim.nonEmpty)

    val driver: String = config.getString("driver")
    val host: String = config.getString("host")
    val port: Int = config.getInt("port")
    val user: String = config.getString("user")
    val password: String = config.getString("password")
    val database: String = config.getString("database")
    val connectTimeout: FiniteDuration = config.getDuration("connect-timeout").asScala

  }

  override def name: String = "sqlserver"

  override def adaptSettings(settings: R2dbcSettings): R2dbcSettings = {

    val res = settings
      // app timestamp is db timestamp because sqlserver does not provide a transaction timestamp
      .withUseAppTimestamp(true)
      // saw flaky tests where the Instant.now was smaller then the db timestamp AFTER the insert
      .withDbTimestampMonotonicIncreasing(true)
    res
  }

  override def createConnectionFactory(
      config: Config,
      optionsProvider: ConnectionFactoryOptionsProvider): ConnectionFactory = {

    val settings = new SqlServerConnectionFactorySettings(config)
    val builder =
      settings.urlOption match {
        case Some(url) =>
          ConnectionFactoryOptions
            .builder()
            .from(ConnectionFactoryOptions.parse(url))
        case _ =>
          ConnectionFactoryOptions
            .builder()
            .option(ConnectionFactoryOptions.DRIVER, settings.driver)
            .option(ConnectionFactoryOptions.HOST, settings.host)
            .option(ConnectionFactoryOptions.PORT, Integer.valueOf(settings.port))
            .option(ConnectionFactoryOptions.USER, settings.user)
            .option(ConnectionFactoryOptions.PASSWORD, settings.password)
            .option(ConnectionFactoryOptions.DATABASE, settings.database)
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, JDuration.ofMillis(settings.connectTimeout.toMillis))
      }

    builder
      //the option below is necessary to avoid https://github.com/r2dbc/r2dbc-mssql/issues/276
      .option(MssqlConnectionFactoryProvider.PREFER_CURSORED_EXECUTION, false)

    val options = optionsProvider.buildOptions(builder, config)
    ConnectionFactories.get(options)
  }

  override def daoExecutionContext(settings: R2dbcSettings, system: ActorSystem[_]): ExecutionContext =
    system.executionContext

  override def createJournalDao(executorProvider: R2dbcExecutorProvider): JournalDao =
    new SqlServerJournalDao(executorProvider)

  override def createQueryDao(executorProvider: R2dbcExecutorProvider): QueryDao =
    new SqlServerQueryDao(executorProvider)

  override def createSnapshotDao(executorProvider: R2dbcExecutorProvider): SnapshotDao =
    new SqlServerSnapshotDao(executorProvider)

  override def createDurableStateDao(executorProvider: R2dbcExecutorProvider): DurableStateDao =
    new SqlServerDurableStateDao(executorProvider, this)
}
