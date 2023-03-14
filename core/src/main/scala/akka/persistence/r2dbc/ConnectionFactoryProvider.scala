/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.time.{ Duration => JDuration }
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions

object ConnectionFactoryProvider extends ExtensionId[ConnectionFactoryProvider] {
  def createExtension(system: ActorSystem[_]): ConnectionFactoryProvider = new ConnectionFactoryProvider(system)

  // Java API
  def get(system: ActorSystem[_]): ConnectionFactoryProvider = apply(system)
}

class ConnectionFactoryProvider(system: ActorSystem[_]) extends Extension {

  import R2dbcExecutor.PublisherOps
  private val sessions = new ConcurrentHashMap[String, ConnectionPool]

  CoordinatedShutdown(system)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close connection pools") { () =>
      import system.executionContext
      Future
        .sequence(sessions.asScala.values.map(_.disposeLater().asFutureDone()))
        .map(_ => Done)
    }

  def connectionFactoryFor(configLocation: String): ConnectionFactory = {
    sessions
      .computeIfAbsent(
        configLocation,
        configLocation => {
          val config = system.settings.config.getConfig(configLocation)
          val settings = new ConnectionFactorySettings(config)
          createConnectionPoolFactory(settings)
        })
      .asInstanceOf[ConnectionFactory]
  }

  private def createConnectionFactory(settings: ConnectionFactorySettings): ConnectionFactory = {

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

    if (settings.sslEnabled) {
      builder.option(ConnectionFactoryOptions.SSL, java.lang.Boolean.TRUE)

      if (settings.sslMode.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_MODE, SSLMode.fromValue(settings.sslMode))

      if (settings.sslRootCert.nonEmpty)
        builder.option(PostgresqlConnectionFactoryProvider.SSL_ROOT_CERT, settings.sslRootCert)
    }

    ConnectionFactories.get(builder.build())
  }

  private def createConnectionPoolFactory(settings: ConnectionFactorySettings): ConnectionPool = {
    val connectionFactory = createConnectionFactory(settings)

    val evictionInterval = {
      import settings.{ maxIdleTime, maxLifeTime }
      if (maxIdleTime <= Duration.Zero && maxLifeTime <= Duration.Zero) {
        JDuration.ZERO
      } else if (maxIdleTime <= Duration.Zero) {
        JDuration.ofMillis((maxLifeTime / 4).toMillis)
      } else if (maxLifeTime <= Duration.Zero) {
        JDuration.ofMillis((maxIdleTime / 4).toMillis)
      } else {
        JDuration.ofMillis((maxIdleTime.min(maxIdleTime) / 4).toMillis)
      }
    }

    val poolConfiguration = ConnectionPoolConfiguration
      .builder(connectionFactory)
      .initialSize(settings.initialSize)
      .maxSize(settings.maxSize)
      // Don't use maxCreateConnectionTime because it can cause connection leaks, see issue #182
      // ConnectionFactoryOptions.CONNECT_TIMEOUT is used instead.
      .maxAcquireTime(JDuration.ofMillis(settings.acquireTimeout.toMillis))
      .acquireRetry(settings.acquireRetry)
      .maxIdleTime(JDuration.ofMillis(settings.maxIdleTime.toMillis))
      .maxLifeTime(JDuration.ofMillis(settings.maxLifeTime.toMillis))
      .backgroundEvictionInterval(evictionInterval)

    if (settings.validationQuery.nonEmpty)
      poolConfiguration.validationQuery(settings.validationQuery)

    val pool = new ConnectionPool(poolConfiguration.build())

    // eagerly create initialSize connections
    pool.warmup().asFutureDone() // don't wait for it

    pool
  }

}
