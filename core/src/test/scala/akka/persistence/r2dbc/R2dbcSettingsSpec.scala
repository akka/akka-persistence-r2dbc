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

  "Settings for postgres" should {
    "have table names with schema" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.schema=s1
          akka.persistence.r2dbc.journal.table-data-partitions = 1
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.journalTableWithSchema(0) shouldBe "s1.event_journal"
      settings.snapshotsTableWithSchema shouldBe "s1.snapshot"
      settings.durableStateTableWithSchema shouldBe "s1.durable_state"

      // by default connection is configured with options
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings shouldBe a[PostgresConnectionFactorySettings]
      connectionFactorySettings.urlOption should not be defined
    }

    "have table names with data partition suffix" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.schema=s1
          akka.persistence.r2dbc.journal.table-data-partitions = 4
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.journalTableWithSchema(slice = 0) shouldBe "s1.event_journal_0"
      settings.journalTableWithSchema(slice = 17) shouldBe "s1.event_journal_0"
      settings.journalTableWithSchema(slice = 256) shouldBe "s1.event_journal_1"
      settings.journalTableWithSchema(slice = 511) shouldBe "s1.event_journal_1"
      settings.journalTableWithSchema(slice = 512) shouldBe "s1.event_journal_2"
      settings.journalTableWithSchema(slice = 767) shouldBe "s1.event_journal_2"
      settings.journalTableWithSchema(slice = 768) shouldBe "s1.event_journal_3"
      settings.journalTableWithSchema(slice = 1023) shouldBe "s1.event_journal_3"
    }

    "verify slice range within same data partition" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.journal.table-data-partitions = 4
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.isJournalSliceRangeWithinSameDataPartition(0, 255) shouldBe true
      settings.isJournalSliceRangeWithinSameDataPartition(256, 511) shouldBe true
      settings.isJournalSliceRangeWithinSameDataPartition(512, 767) shouldBe true
      settings.isJournalSliceRangeWithinSameDataPartition(768, 1023) shouldBe true

      settings.isJournalSliceRangeWithinSameDataPartition(0, 1023) shouldBe false
      settings.isJournalSliceRangeWithinSameDataPartition(0, 511) shouldBe false
      settings.isJournalSliceRangeWithinSameDataPartition(512, 1023) shouldBe false
      settings.isJournalSliceRangeWithinSameDataPartition(511, 512) shouldBe false
    }

    "support connection settings build from url" in {
      val config =
        ConfigFactory
          .parseString("akka.persistence.r2dbc.connection-factory.url=whatever-url")
          .withFallback(ConfigFactory.load("application-postgres.conf"))

      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings shouldBe a[PostgresConnectionFactorySettings]
      connectionFactorySettings.urlOption shouldBe defined
    }

    "support ssl-mode as enum name" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=VERIFY_FULL")
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "VERIFY_FULL"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }

    "support ssl-mode values in lower and dashes" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=verify-full")
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "verify-full"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }
  }
}
