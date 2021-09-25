/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = ConfigFactory.load(ConfigFactory.parseString("""
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor {
      serialization-bindings {
        "akka.persistence.r2dbc.CborSerializable" = jackson-cbor
      }
    }
    """))
}
