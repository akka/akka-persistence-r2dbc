/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc

import java.time.{ Duration => JDuration }

import com.typesafe.config.ConfigFactory
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConnectionFactoryProviderSpec extends AnyWordSpec with TestSuite with Matchers {

  private def evictionInterval(maxIdleTime: String, maxLifeTime: String): JDuration =
    ConnectionFactoryProvider.evictionInterval(
      new ConnectionPoolSettings(
        ConfigFactory
          .parseString(s"""
            max-idle-time = $maxIdleTime
            max-life-time = $maxLifeTime
            """)
          .withFallback(ConfigFactory.defaultReference().getConfig("akka.persistence.r2dbc.default-connection-pool"))))

  "ConnectionFactoryProvider" should {
    "use a quarter of the shorter of max-idle-time and max-life-time as eviction interval" in {
      evictionInterval("30 minutes", "60 minutes") shouldBe JDuration.ofSeconds(450)
      evictionInterval("30 minutes", "10 minutes") shouldBe JDuration.ofSeconds(150)
    }

    "use the other setting for the eviction interval when one is zero" in {
      evictionInterval("0", "60 minutes") shouldBe JDuration.ofMinutes(15)
      evictionInterval("30 minutes", "0") shouldBe JDuration.ofSeconds(450)
      evictionInterval("0", "0") shouldBe JDuration.ZERO
    }
  }
}
