/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.ConfigFactory
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
  }
}
