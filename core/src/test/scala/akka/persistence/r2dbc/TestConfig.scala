/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()
    val dialect = defaultConfig.getString("akka.persistence.r2dbc.connection-factory.dialect")

    val dialectConfig = dialect match {
      case "postgres" =>
        // defaults are fine
        ConfigFactory.empty()
      case "yugabyte" =>
        // defaults are fine
        ConfigFactory.empty()
      case "h2" =>
        ConfigFactory.parseString(s"""
          akka.persistence.r2dbc.connection-factory {
            protocol = "file"
            database = "./target/h2-test-db"
            trace-logging = on
          }
          """)
      case "sqlserver" =>
        // defaults are fine
        ConfigFactory.empty()
    }

    // fallback to default here so that connection-factory can be overridden
    dialectConfig
      .withFallback(ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor {
      serialization-bindings {
        "akka.persistence.r2dbc.CborSerializable" = jackson-cbor
        "akka.persistence.r2dbc.JsonSerializable" = jackson-json
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """))
      .withFallback(defaultConfig)
  }

  val backtrackingDisabledConfig: Config =
    ConfigFactory.parseString("akka.persistence.r2dbc.query.backtracking.enabled = off")
}
