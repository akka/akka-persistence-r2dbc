/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
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
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.journalTableWithSchema(0) shouldBe "s1.event_journal"
      settings.snapshotTableWithSchema(0) shouldBe "s1.snapshot"
      settings.durableStateTableWithSchema(0) shouldBe "s1.durable_state"

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

      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings shouldBe a[PostgresConnectionFactorySettings]
      connectionFactorySettings.urlOption shouldBe defined
    }

    "support ssl-mode as enum name" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=VERIFY_FULL")
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "VERIFY_FULL"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }

    "support ssl-mode values in lower and dashes" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.ssl.mode=verify-full")
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val connectionFactorySettings = postgresConnectionFactorySettings(config)
      connectionFactorySettings.sslMode shouldBe "verify-full"
      SSLMode.fromValue(connectionFactorySettings.sslMode) shouldBe SSLMode.VERIFY_FULL
    }

    "support options-provider" in {
      val config = ConfigFactory
        .parseString("akka.persistence.r2dbc.connection-factory.options-provider=my.OptProvider")
        .withFallback(ConfigFactory.load("application-postgres.conf"))
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings(0).optionsProvider shouldBe "my.OptProvider"
    }
  }

  "data-partition settings" should {
    "have no data partitions by default" in {
      val config = ConfigFactory.load("application-postgres.conf")
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.numberOfDataPartitions shouldBe 1
      settings.numberOfDatabases shouldBe 1
      settings.dataPartitionSliceRanges.size shouldBe 1
      settings.dataPartitionSliceRanges.head shouldBe (0 until 1024)
      settings.connectionFactorSliceRanges.size shouldBe 1
      settings.connectionFactorSliceRanges.head shouldBe (0 until 1024)
    }

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

      settings.snapshotTableWithSchema(slice = 0) shouldBe "s1.snapshot_0"
      settings.snapshotTableWithSchema(slice = 17) shouldBe "s1.snapshot_0"
      settings.snapshotTableWithSchema(slice = 256) shouldBe "s1.snapshot_1"
      settings.snapshotTableWithSchema(slice = 511) shouldBe "s1.snapshot_1"
      settings.snapshotTableWithSchema(slice = 512) shouldBe "s1.snapshot_2"
      settings.snapshotTableWithSchema(slice = 767) shouldBe "s1.snapshot_2"
      settings.snapshotTableWithSchema(slice = 768) shouldBe "s1.snapshot_3"
      settings.snapshotTableWithSchema(slice = 1023) shouldBe "s1.snapshot_3"

      settings.durableStateTableWithSchema(slice = 0) shouldBe "s1.durable_state_0"
      settings.durableStateTableWithSchema(slice = 17) shouldBe "s1.durable_state_0"
      settings.durableStateTableWithSchema(slice = 256) shouldBe "s1.durable_state_1"
      settings.durableStateTableWithSchema(slice = 511) shouldBe "s1.durable_state_1"
      settings.durableStateTableWithSchema(slice = 512) shouldBe "s1.durable_state_2"
      settings.durableStateTableWithSchema(slice = 767) shouldBe "s1.durable_state_2"
      settings.durableStateTableWithSchema(slice = 768) shouldBe "s1.durable_state_3"
      settings.durableStateTableWithSchema(slice = 1023) shouldBe "s1.durable_state_3"
      settings.getDurableStateTableWithSchema("TestEntity", slice = 511) shouldBe "s1.durable_state_1"
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

      settings.dataPartitionSliceRanges.size shouldBe 4
      settings.dataPartitionSliceRanges(0) should be(0 until 256)
      settings.dataPartitionSliceRanges(1) should be(256 until 512)
      settings.dataPartitionSliceRanges(2) should be(512 until 768)
      settings.dataPartitionSliceRanges(3) should be(768 until 1024)
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

      settings.connectionFactorSliceRanges.size shouldBe 2
      settings.connectionFactorSliceRanges(0) should be(0 until 512)
      settings.connectionFactorSliceRanges(1) should be(512 until 1024)
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

      settings.connectionFactorSliceRanges.size shouldBe 2
      settings.connectionFactorSliceRanges(0) should be(0 until 512)
      settings.connectionFactorSliceRanges(1) should be(512 until 1024)
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

      settings.connectionFactorSliceRanges.size shouldBe 2
      settings.connectionFactorSliceRanges(0) should be(0 until 512)
      settings.connectionFactorSliceRanges(1) should be(512 until 1024)

      val configPaths =
        R2dbcSettings.connectionFactoryConfigPaths(
          "a.b.connection-factory",
          numberOfDataPartitions = 8,
          numberOfDatabases = 2)
      configPaths.size shouldBe 2
      configPaths(0) shouldBe "a.b.connection-factory-0-3"
      configPaths(1) shouldBe "a.b.connection-factory-4-7"
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

      settings.connectionFactorSliceRanges.size shouldBe 1
      settings.connectionFactorSliceRanges(0) should be(0 until 1024)
    }

    "support options-provider" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.postgres.options-provider=my.OptProvider
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 2
            number-of-databases = 2
          }
          akka.persistence.r2dbc.connection-factory-0-0 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-0-0.host = hostA
          akka.persistence.r2dbc.connection-factory-1-1 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-1-1.host = hostB
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
        .resolve()
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings(0).optionsProvider shouldBe "my.OptProvider"
      settings.connectionFactorySettings(1).optionsProvider shouldBe "my.OptProvider"
    }

    "support options-provider per db" in {
      val config = ConfigFactory
        .parseString("""
          akka.persistence.r2dbc.data-partition {
            number-of-partitions = 2
            number-of-databases = 2
          }
          akka.persistence.r2dbc.connection-factory-0-0 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-0-0.host = hostA
          akka.persistence.r2dbc.connection-factory-0-0.options-provider=my.OptProvider0
          akka.persistence.r2dbc.connection-factory-1-1 = ${akka.persistence.r2dbc.postgres}
          akka.persistence.r2dbc.connection-factory-1-1.host = hostB
          akka.persistence.r2dbc.connection-factory-1-1.options-provider=my.OptProvider1
          """)
        .withFallback(ConfigFactory.load("application-postgres.conf"))
        .resolve()
      val settings = R2dbcSettings(config.getConfig("akka.persistence.r2dbc"))
      settings.connectionFactorySettings(0).optionsProvider shouldBe "my.OptProvider0"
      settings.connectionFactorySettings(1023).optionsProvider shouldBe "my.OptProvider1"
    }

  }
}
