/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.{ Duration => JDuration }
import java.util.Locale

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.SnapshotDao
import com.typesafe.config.Config
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions

import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresDialect extends Dialect {

  override def name: String = "postgres"

  private[r2dbc] final class PostgresConnectionFactorySettings(config: Config) {

    val urlOption: Option[String] =
      Option(config.getString("url"))
        .filter(_.trim.nonEmpty)

    val driver: String = config.getString("driver")
    val host: String = config.getString("host")
    val port: Int = config.getInt("port")
    val user: String = config.getString("user")
    val password: String = config.getString("password")
    val database: String = config.getString("database")

    val sslEnabled: Boolean = config.getBoolean("ssl.enabled")
    val sslMode: String = config.getString("ssl.mode")
    val sslRootCert: String = config.getString("ssl.root-cert")
    val sslCert: String = config.getString("ssl.cert")
    val sslKey: String = config.getString("ssl.key")
    val sslPassword: String = config.getString("ssl.password")

    val connectTimeout: FiniteDuration = config.getDuration("connect-timeout").toScala
    val statementCacheSize: Int = config.getInt("statement-cache-size")

    val statementTimeout: Option[FiniteDuration] =
      config.getString("statement-timeout").toLowerCase(Locale.ROOT) match {
        case "off" => None
        case _     => Some(config.getDuration("statement-timeout").toScala)
      }
  }

  override def createConnectionFactory(
      config: Config,
      optionsProvider: ConnectionFactoryOptionsProvider): ConnectionFactory = {
    val settings = new PostgresConnectionFactorySettings(config)
    val builder =
      settings.urlOption match {
        case Some(url) =>
          ConnectionFactoryOptions.builder().from(ConnectionFactoryOptions.parse(url))
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
      .option(PostgresqlConnectionFactoryProvider.FORCE_BINARY, java.lang.Boolean.TRUE)
      .option(PostgresqlConnectionFactoryProvider.PREFER_ATTACHED_BUFFERS, java.lang.Boolean.TRUE)
      .option(
        PostgresqlConnectionFactoryProvider.PREPARED_STATEMENT_CACHE_QUERIES,
        Integer.valueOf(settings.statementCacheSize))

    settings.statementTimeout.foreach { timeout =>
      import scala.jdk.DurationConverters._
      builder.option(PostgresqlConnectionFactoryProvider.STATEMENT_TIMEOUT, timeout.toJava)
    }

    if (settings.sslEnabled) {
      builder.option(ConnectionFactoryOptions.SSL, java.lang.Boolean.TRUE)

      if (settings.sslMode.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_MODE, SSLMode.fromValue(settings.sslMode))

      if (settings.sslRootCert.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_ROOT_CERT, settings.sslRootCert)

      if (settings.sslCert.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_CERT, settings.sslCert)

      if (settings.sslKey.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_KEY, settings.sslKey)

      if (settings.sslPassword.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_PASSWORD, settings.sslPassword)
    }

    val options = optionsProvider.buildOptions(builder, config)
    ConnectionFactories.get(options)
  }

  override def daoExecutionContext(settings: R2dbcSettings, system: ActorSystem[_]): ExecutionContext =
    system.executionContext

  override def createJournalDao(executorProvider: R2dbcExecutorProvider): JournalDao =
    new PostgresJournalDao(executorProvider)

  override def createSnapshotDao(executorProvider: R2dbcExecutorProvider): SnapshotDao =
    new PostgresSnapshotDao(executorProvider)

  override def createQueryDao(executorProvider: R2dbcExecutorProvider): QueryDao =
    new PostgresQueryDao(executorProvider)

  override def createDurableStateDao(executorProvider: R2dbcExecutorProvider): DurableStateDao =
    new PostgresDurableStateDao(executorProvider, this)
}
