/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.ConfigException

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
          akka.persistence.r2dbc.data-partition.number-of-partitions = 1
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

  "data-partition settings" should {
    "report invalid values" in {
      val baseConfig = ConfigFactory.load("application-postgres.conf")
      def settingsWith(numberOfPartitions: Int, numberOfDatabases: Int = 1): R2dbcSettings = {
        val config = ConfigFactory
          .parseString(s"""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = $numberOfPartitions
            number-of-databases = $numberOfDatabases
          }
          """)
          .withFallback(baseConfig)
        R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      }

      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 0))
      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 1025))
      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 6))

      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 8, numberOfDatabases = 0))
      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 8, numberOfDatabases = 1025))
      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 8, numberOfDatabases = 6))
      intercept[IllegalArgumentException](settingsWith(numberOfPartitions = 8, numberOfDatabases = 16))

      intercept[ConfigException.Missing](settingsWith(numberOfPartitions = 8, numberOfDatabases = 2))
    }

    "result in table names with data partition suffix" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.schema=s1
          akka.persistence.r2dbc.data-partition.number-of-partitions = 4
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
          akka.persistence.r2dbc.data-partition.number-of-partitions = 4
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.isSliceRangeWithinSameDataPartition(0, 255) shouldBe true
      settings.isSliceRangeWithinSameDataPartition(256, 511) shouldBe true
      settings.isSliceRangeWithinSameDataPartition(512, 767) shouldBe true
      settings.isSliceRangeWithinSameDataPartition(768, 1023) shouldBe true

      settings.isSliceRangeWithinSameDataPartition(0, 1023) shouldBe false
      settings.isSliceRangeWithinSameDataPartition(0, 511) shouldBe false
      settings.isSliceRangeWithinSameDataPartition(512, 1023) shouldBe false
      settings.isSliceRangeWithinSameDataPartition(511, 512) shouldBe false
    }

    "use connection-factory per database when same number of databases as partitions" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 2
            number-of-databases = 2
          }
          akka.persistence.r2dbc.connection-factory-0-0 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-0-0.host = hostA
          akka.persistence.r2dbc.connection-factory-1-1 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-1-1.host = hostB

          # FIXME maybe we should support a convenience syntax for this case:
          # akka.persistence.r2dbc.connection-factory-0 = ${akka.persistence.r2dbc.postgres}
          # akka.persistence.r2dbc.connection-factory-1 = ${akka.persistence.r2dbc.postgres}
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
        .resolve()
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings(slice = 0).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 17).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 511).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 512).config.getString("host") shouldBe "hostB"
      settings.connectionFactorySettings(slice = 700).config.getString("host") shouldBe "hostB"
      settings.connectionFactorySettings(slice = 1023).config.getString("host") shouldBe "hostB"
    }

    "use connection-factory per database when less databases than partitions" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 8
            number-of-databases = 2
          }
          akka.persistence.r2dbc.connection-factory-0-3 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-0-3.host = hostA
          akka.persistence.r2dbc.connection-factory-4-7 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-4-7.host = hostB
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
        .resolve()
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings(slice = 0).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 17).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 511).config.getString("host") shouldBe "hostA"
      settings.connectionFactorySettings(slice = 512).config.getString("host") shouldBe "hostB"
      settings.connectionFactorySettings(slice = 700).config.getString("host") shouldBe "hostB"
      settings.connectionFactorySettings(slice = 1023).config.getString("host") shouldBe "hostB"
    }

    "derive connection-factory config property from number of partitions and databases" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 8
            number-of-databases = 2
          }
          akka.persistence.r2dbc.connection-factory-0-3 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-0-3.host = hostA
          akka.persistence.r2dbc.connection-factory-4-7 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-4-7.host = hostB
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
        .resolve()
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 0) shouldBe "a.b.connection-factory-0-3"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 17) shouldBe "a.b.connection-factory-0-3"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 511) shouldBe "a.b.connection-factory-0-3"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 512) shouldBe "a.b.connection-factory-4-7"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 700) shouldBe "a.b.connection-factory-4-7"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 1023) shouldBe "a.b.connection-factory-4-7"
    }

    "use default connection-factory config property when one database" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 8
            number-of-databases = 1
          }
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.resolveConnectionFactoryConfigPath("a.b.connection-factory", slice = 0) shouldBe "a.b.connection-factory"
      settings.resolveConnectionFactoryConfigPath(
        "a.b.connection-factory",
        slice = 1023) shouldBe "a.b.connection-factory"
    }

  }
}
