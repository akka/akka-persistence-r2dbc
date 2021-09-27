/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.r2dbc.internal.DummyConnectionPool
import com.typesafe.config.Config
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
//        createConnectionFactory(config)
        new DummyConnectionPool(createConnectionFactory(config), 20)(system)
      })
  }

  private def createConnectionFactory(config: Config): ConnectionFactory = {
    // FIXME config
    ConnectionFactories.get(
      ConnectionFactoryOptions
        .builder()
        .option(ConnectionFactoryOptions.DRIVER, "postgresql")
        .option(ConnectionFactoryOptions.HOST, "localhost")
        .option(ConnectionFactoryOptions.PORT, Integer.valueOf(5432))
        .option(ConnectionFactoryOptions.USER, "postgres")
        .option(ConnectionFactoryOptions.PASSWORD, "postgres")
        .option(ConnectionFactoryOptions.DATABASE, "postgres")
        .build())
  }

  private def createConnectionPoolFactory(config: Config): ConnectionFactory = {
    // FIXME config
    val connectionFactory = createConnectionFactory(config)

    // FIXME connection pool is not working: "PostgresConnectionClosedException: Cannot exchange messages because the connection is closed"
    val poolConfiguration = ConnectionPoolConfiguration
      .builder(connectionFactory)
      .initialSize(1)
      .maxSize(1)
//      .preRelease(_.rollbackTransaction)
//      .validationQuery("SELECT 1")
      .acquireRetry(3)
      .maxAcquireTime(Duration.ofMillis(3000))
      .build()

    val pool = new ConnectionPool(poolConfiguration)

//    pool.warmup().asFutureDone()

    pool
  }

}
