/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.ConnectionPoolSettings
import akka.persistence.r2dbc.internal.h2.H2Dialect
import akka.persistence.r2dbc.internal.sqlserver.SqlServerDialect
import akka.persistence.r2dbc.internal.postgres.PostgresDialect
import akka.persistence.r2dbc.internal.postgres.YugabyteDialect
import akka.util.Helpers.toRootLowerCase
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object ConnectionFactorySettings {

  def apply(system: ActorSystem[_]): ConnectionFactorySettings =
    apply(system.settings.config.getConfig("akka.persistence.r2dbc.connection-factory"))

  def apply(config: Config): ConnectionFactorySettings = {
    val dialect: Dialect = toRootLowerCase(config.getString("dialect")) match {
      case "yugabyte"  => YugabyteDialect: Dialect
      case "postgres"  => PostgresDialect: Dialect
      case "h2"        => H2Dialect: Dialect
      case "sqlserver" => SqlServerDialect: Dialect
      case other =>
        throw new IllegalArgumentException(
          s"Unknown dialect [$other]. Supported dialects are [postgres, yugabyte, h2, sqlserver].")
    }

    // pool settings are common to all dialects but defined inline in the connection factory block
    // for backwards compatibility/convenience
    val poolSettings = new ConnectionPoolSettings(config)

    ConnectionFactorySettings(dialect, config, poolSettings)
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] case class ConnectionFactorySettings(
    dialect: Dialect,
    config: Config,
    poolSettings: ConnectionPoolSettings)
