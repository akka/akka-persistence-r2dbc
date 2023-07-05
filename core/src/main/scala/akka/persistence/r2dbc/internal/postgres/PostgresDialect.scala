/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.{ Duration => JDuration }
import java.util.Locale

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
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions

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

    val connectTimeout: FiniteDuration = config.getDuration("connect-timeout").asScala
    val statementCacheSize: Int = config.getInt("statement-cache-size")

    val statementTimeout: Option[FiniteDuration] =
      config.getString("statement-timeout").toLowerCase(Locale.ROOT) match {
        case "off" => None
        case _     => Some(config.getDuration("statement-timeout").asScala)
      }
  }

  override def createConnectionFactory(config: Config): ConnectionFactory = {
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
      import akka.util.JavaDurationConverters._
      builder.option(PostgresqlConnectionFactoryProvider.STATEMENT_TIMEOUT, timeout.asJava)
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

    ConnectionFactories.get(builder.build())
  }

  override def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): JournalDao =
    new PostgresJournalDao(settings, connectionFactory)(system.executionContext, system)

  override def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): SnapshotDao =
    new PostgresSnapshotDao(settings, connectionFactory)(system.executionContext, system)

  override def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): QueryDao =
    new PostgresQueryDao(settings, connectionFactory)(system.executionContext, system)

  override def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): DurableStateDao =
    new PostgresDurableStateDao(settings, connectionFactory)(system.executionContext, system)
}
