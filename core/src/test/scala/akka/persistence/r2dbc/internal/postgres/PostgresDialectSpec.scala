/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.postgres

import java.util.concurrent.atomic.AtomicReference

import akka.persistence.r2dbc.ConnectionFactoryProvider.ConnectionFactoryOptionsProvider
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.spi.ConnectionFactoryOptions
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PostgresDialectSpec extends AnyWordSpec with TestSuite with Matchers {

  private def connectionFactoryConfig(settings: String): Config =
    ConfigFactory
      .parseString(s"akka.persistence.r2dbc.connection-factory { $settings }")
      .withFallback(ConfigFactory.load("application-postgres.conf"))
      .getConfig("akka.persistence.r2dbc.connection-factory")

  private def builtOptions(config: Config): ConnectionFactoryOptions = {
    val recorded = new AtomicReference[ConnectionFactoryOptions]
    val optionsProvider: ConnectionFactoryOptionsProvider = { (builder, _) =>
      val options = builder.build()
      recorded.set(options)
      options
    }
    PostgresDialect.createConnectionFactory(config, optionsProvider)
    recorded.get
  }

  "PostgresDialect" should {

    "enable TCP keepalive by default" in {
      val options = builtOptions(connectionFactoryConfig(""))
      options.getValue(PostgresqlConnectionFactoryProvider.TCP_KEEPALIVE) shouldBe java.lang.Boolean.TRUE
    }

    "enable TCP keepalive by default with url" in {
      val options =
        builtOptions(
          connectionFactoryConfig("""url = "r2dbc:postgresql://postgres:postgres@localhost:5432/postgres""""))
      options.getValue(ConnectionFactoryOptions.HOST) shouldBe "localhost"
      options.getValue(PostgresqlConnectionFactoryProvider.TCP_KEEPALIVE) shouldBe java.lang.Boolean.TRUE
    }

    "disable TCP keepalive with tcp-keep-alive = off" in {
      val options = builtOptions(connectionFactoryConfig("tcp-keep-alive = off"))
      options.getValue(PostgresqlConnectionFactoryProvider.TCP_KEEPALIVE) shouldBe java.lang.Boolean.FALSE
    }
  }
}
