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
final class QuerySettings(config: Config) {
  val refreshInterval: FiniteDuration = config.getDuration("refresh-interval").asScala
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class R2dbcSettings(config: Config) {
  val journalTable = config.getString("journal.table")

  val snapshotsTable = config.getString("snapshot.table")

  val querySettings = new QuerySettings(config.getConfig("query"))

}
