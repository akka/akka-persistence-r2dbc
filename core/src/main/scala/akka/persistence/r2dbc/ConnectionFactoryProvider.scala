/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.time.{ Duration => JDuration }
import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions

object ConnectionFactoryProvider extends ExtensionId[ConnectionFactoryProvider] {
  def createExtension(system: ActorSystem[_]): ConnectionFactoryProvider = new ConnectionFactoryProvider(system)

  // Java API
  def get(system: ActorSystem[_]): ConnectionFactoryProvider = apply(system)
}

class ConnectionFactoryProvider(system: ActorSystem[_]) extends Extension {

  private val sessions = new ConcurrentHashMap[String, ConnectionFactory]

  def connectionFactoryFor(configLocation: String): ConnectionFactory = {
    sessions.computeIfAbsent(
      configLocation,
      configLocation => {
        val config = system.settings.config.getConfig(configLocation)
        val settings = new ConnectionFactorySettings(config)
        createConnectionPoolFactory(settings)
      })
  }

  private def createConnectionFactory(settings: ConnectionFactorySettings): ConnectionFactory = {
    ConnectionFactories.get(
      ConnectionFactoryOptions
        .builder()
        .option(ConnectionFactoryOptions.DRIVER, settings.driver)
        .option(ConnectionFactoryOptions.HOST, settings.host)
        .option(ConnectionFactoryOptions.PORT, Integer.valueOf(settings.port))
        .option(ConnectionFactoryOptions.USER, settings.user)
        .option(ConnectionFactoryOptions.PASSWORD, settings.password)
        .option(ConnectionFactoryOptions.DATABASE, settings.database)
        .build())
  }

  private def createConnectionPoolFactory(settings: ConnectionFactorySettings): ConnectionFactory = {
    val connectionFactory = createConnectionFactory(settings)

    val poolConfiguration = ConnectionPoolConfiguration
      .builder(connectionFactory)
      .initialSize(settings.initialSize)
      .maxSize(settings.maxSize)
      .maxCreateConnectionTime(JDuration.ofMillis(settings.createTimeout.toMillis))
      .maxAcquireTime(JDuration.ofMillis(settings.acquireTimeout.toMillis))
      .acquireRetry(3)
      // FIXME more properties?
//      .maxLifeTime(Duration.ZERO)
//      .maxIdleTime(Duration.ofMinutes(30))
//      .preRelease(_.rollbackTransaction)
//      .validationQuery("SELECT 1")
      .build()

    val pool = new ConnectionPool(poolConfiguration)

    // eagerly create initialSize connections
    import R2dbcExecutor.PublisherOps
    pool.warmup().asFutureDone() // don't wait for it

    pool
  }

}
