/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.ConfigFactory
import io.r2dbc.postgresql.client.SSLMode
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class R2dbcSettingsSpec extends AnyWordSpec with TestSuite with Matchers {

  "Settings" should {
    "have table names with schema" in {
      val config = ConfigFactory.parseString("akka.persistence.r2dbc.schema=s1").withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.journalTableWithSchema shouldBe "s1.event_journal"
      settings.snapshotsTableWithSchema shouldBe "s1.snapshot"
      settings.durableStateTableWithSchema shouldBe "s1.durable_state"

      // by default connection is configured with options
      settings.connectionFactorySettings shouldBe a[ConnectionFactorySettings]
      settings.connectionFactorySettings.urlOption should not be defined
    }

    "support connection settings build from url" in {
      val config =
        ConfigFactory
          .parseString("akka.persistence.r2dbc.connection-factory.url=whatever-url")
          .withFallback(ConfigFactory.load())

      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings shouldBe a[ConnectionFactorySettings]
      settings.connectionFactorySettings.urlOption shouldBe defined
    }

    "support ssl-mode as enum name" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=VERIFY_FULL")
        .withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings.sslMode shouldBe "VERIFY_FULL"
      SSLMode.fromValue(settings.connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }

    "support ssl-mode values in lower and dashes" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=verify-full")
        .withFallback(ConfigFactory.load())
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings.sslMode shouldBe "verify-full"
      SSLMode.fromValue(settings.connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }
  }
}
