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
import akka.persistence.r2dbc.internal.postgres.PostgresDialect
import akka.persistence.r2dbc.internal.{ Dialect, R2dbcExecutor }
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

  def connectionFactoryFor(settings: R2dbcSettings, configLocation: String): ConnectionFactory = {
    sessions
      .computeIfAbsent(
        configLocation,
        configLocation => {
          val config = system.settings.config.getConfig(configLocation)
          val connectionFactory = settings.dialect.createConnectionFactory(settings, config)
          // pool settings are common to all dialects but defined in the same block for backwards compatibility
          val poolSettings = new ConnectionPoolSettings(config)
          createConnectionPoolFactory(poolSettings, connectionFactory)
        })
      .asInstanceOf[ConnectionFactory]
  }

  private def createConnectionPoolFactory(
      settings: ConnectionPoolSettings,
      connectionFactory: ConnectionFactory): ConnectionPool = {
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
