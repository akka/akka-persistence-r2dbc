/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.persistence.r2dbc.internal.postgres.PostgresDialect.PostgresConnectionFactorySettings
import com.typesafe.config.{ Config, ConfigFactory }
import io.r2dbc.postgresql.client.SSLMode
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class R2dbcSettingsSpec extends AnyWordSpec with TestSuite with Matchers {

  def postgresConnectionFactorySettings(config: Config) =
    new PostgresConnectionFactorySettings(config.getConfig("akka.persistence.r2dbc.connection-factory"))

  "Settings" should {
    "have table names with schema" in {
      val config = ConfigFactory.parseString("akka.persistence.r2dbc.schema=s1").withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.journalTableWithSchema shouldBe "s1.event_journal"
      settings.snapshotsTableWithSchema shouldBe "s1.snapshot"
      settings.durableStateTableWithSchema shouldBe "s1.durable_state"

      // by default connection is configured with options
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings shouldBe a[PostgresConnectionFactorySettings]
      connectionFactorySettings.urlOption should not be defined
    }

    "support connection settings build from url" in {
      val config =
        ConfigFactory
          .parseString("akka.persistence.r2dbc.connection-factory.url=whatever-url")
          .withFallback(ConfigFactory.load())

      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings shouldBe a[PostgresConnectionFactorySettings]
      connectionFactorySettings.urlOption shouldBe defined
    }

    "support ssl-mode as enum name" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=VERIFY_FULL")
        .withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "VERIFY_FULL"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }

    "support ssl-mode values in lower and dashes" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=verify-full")
        .withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "verify-full"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }
  }
}
