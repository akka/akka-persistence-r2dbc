/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalStableApi
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalStableApi
final class R2dbcSettings(config: Config) {
  val schema: Option[String] = Option(config.getString("schema")).filterNot(_.trim.isEmpty)

  val journalTable: String = config.getString("journal.table")
  val journalTableWithSchema: String = schema.map("." + _).getOrElse("") + journalTable

  val snapshotsTable: String = config.getString("snapshot.table")
  val snapshotsTableWithSchema: String = schema.map("." + _).getOrElse("") + snapshotsTable

  val durableStateTable: String = config.getString("state.table")
  val durableStateTableWithSchema: String = schema.map("." + _).getOrElse("") + durableStateTable

  val maxNumberOfSlices = 128 // FIXME config

  val dialect: String = config.getString("dialect")

  val querySettings = new QuerySettings(config.getConfig("query"))

  val connectionFactorySettings = new ConnectionFactorySettings(config.getConfig("connection-factory"))

}

/**
 * INTERNAL API
 */
@InternalStableApi
final class QuerySettings(config: Config) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").asScala
  val behindCurrentTime: FiniteDuration = config.getDuration("behind-current-time").asScala
  val backtrackingEnabled: Boolean = config.getBoolean("backtracking.enabled")
  val backtrackingWindow: FiniteDuration = config.getDuration("backtracking.window").asScala
  val backtrackingBehindCurrentTime: FiniteDuration = config.getDuration("backtracking.behind-current-time").asScala
  val bufferSize: Int = config.getInt("buffer-size")
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class ConnectionFactorySettings(config: Config) {
  val driver: String = config.getString("driver")
  val host: String = config.getString("host")
  val port: Int = config.getInt("port")
  val user: String = config.getString("user")
  val password: String = config.getString("password")
  val database: String = config.getString("database")

  val sslEnabled: Boolean = config.getBoolean("ssl.enabled")
  val sslMode: String = config.getString("ssl.mode")
  val sslRootCert: String = config.getString("ssl.root-cert")

  val initialSize: Int = config.getInt("initial-size")
  val maxSize: Int = config.getInt("max-size")
  val createTimeout: FiniteDuration = config.getDuration("create-timeout").asScala
  val acquireTimeout: FiniteDuration = config.getDuration("acquire-timeout").asScala
}
