/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()
    val dialect = defaultConfig.getString("akka.persistence.r2dbc.dialect")

    val dialectConfig = dialect match {
      case "postgres" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5432
            user = "postgres"
            password = "postgres"
            database = "postgres"
          }
          """)
      case "yugabyte" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5433
            user = "yugabyte"
            password = "yugabyte"
            database = "yugabyte"
          }
          """)
      case "h2" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
           driver = "h2"
           #url = "r2dbc:h2:mem:///test-db;DB_CLOSE_DELAY=-1"
           url = "r2dbc:h2:file:///./target/h2-test-db;DB_CLOSE_DELAY=-1"
           host = ""
           port = 0
           user = ""
           password = ""
           database = ""
          }
          akka.persistence.r2dbc {
           db-timestamp-monotonic-increasing = on
           use-app-timestamp = on
          }
          """)
    }

    // using load here so that connection-factory can be overridden
    ConfigFactory.load(dialectConfig.withFallback(ConfigFactory.parseString("""
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
    """)))
  }

  val backtrackingDisabledConfig: Config =
    ConfigFactory.parseString("akka.persistence.r2dbc.query.backtracking.enabled = off")
}
