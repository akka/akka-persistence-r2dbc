/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.r2dbc.internal.ConnectionFactorySettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactory
import java.time.{ Duration => JDuration }
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

import com.typesafe.config.Config
import io.r2dbc.spi.ConnectionFactoryOptions

import akka.annotation.InternalApi
import akka.annotation.InternalStableApi

object ConnectionFactoryProvider extends ExtensionId[ConnectionFactoryProvider] {
  def createExtension(system: ActorSystem[_]): ConnectionFactoryProvider = new ConnectionFactoryProvider(system)

  // Java API
  def get(system: ActorSystem[_]): ConnectionFactoryProvider = apply(system)

  trait ConnectionFactoryOptionsProvider {
    def buildOptions(
        builder: ConnectionFactoryOptions.Builder,
        connectionFactoryConfig: Config): ConnectionFactoryOptions
  }

  private object DefaultConnectionFactoryOptionsProvider extends ConnectionFactoryOptionsProvider {
    override def buildOptions(
        builder: ConnectionFactoryOptions.Builder,
        connectionFactoryConfig: Config): ConnectionFactoryOptions =
      builder.build()
  }
}

class ConnectionFactoryProvider(system: ActorSystem[_]) extends Extension {
  import ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
  import ConnectionFactoryProvider.DefaultConnectionFactoryOptionsProvider

  import R2dbcExecutor.PublisherOps
  private val sessions = new ConcurrentHashMap[String, ConnectionPool]
  private val connectionFactorySettings = new ConcurrentHashMap[String, ConnectionFactorySettings]

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
          val settings = connectionFactorySettingsFor(configLocation)
          val optionsProvider = connectionFactoryOptionsProvider(settings)
          val connectionFactory = settings.dialect.createConnectionFactory(settings.config, optionsProvider)
          createConnectionPoolFactory(settings.poolSettings, connectionFactory)
        })
      .asInstanceOf[ConnectionFactory]
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[r2dbc] def connectionFactorySettingsFor(configLocation: String): ConnectionFactorySettings = {
    connectionFactorySettings.get(configLocation) match {
      case null =>
        val settings = ConnectionFactorySettings(system.settings.config.getConfig(configLocation))
        // it's just a cache so no need for guarding concurrent updates
        connectionFactorySettings.put(configLocation, settings)
        settings
      case settings => settings
    }
  }

  private def connectionFactoryOptionsProvider(
      settings: ConnectionFactorySettings): ConnectionFactoryOptionsProvider = {
    settings.optionsProvider match {
      case "" => DefaultConnectionFactoryOptionsProvider
      case fqcn =>
        system.dynamicAccess.createInstanceFor[ConnectionFactoryOptionsProvider](fqcn, Nil) match {
          case Success(provider) => provider
          case Failure(_) =>
            system.dynamicAccess
              .createInstanceFor[ConnectionFactoryOptionsProvider](fqcn, List(classOf[ActorSystem[_]] -> system))
              .get

        }
    }
  }

  /**
   * INTERNAL API
   */
  @InternalStableApi
  def connectionPoolSettingsFor(configLocation: String): ConnectionPoolSettings =
    connectionFactorySettingsFor(configLocation).poolSettings

  private def createConnectionPoolFactory(
      settings: ConnectionPoolSettings,
      connectionFactory: ConnectionFactory): ConnectionPool = {
    val evictionInterval = {
      import settings.maxIdleTime
      import settings.maxLifeTime
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
