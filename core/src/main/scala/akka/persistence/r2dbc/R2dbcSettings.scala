/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.util.Locale

import scala.collection.immutable
import scala.concurrent.duration._

import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config
import akka.util.Helpers.toRootLowerCase

/**
 * INTERNAL API
 */
@InternalStableApi
object R2dbcSettings {
  def apply(config: Config): R2dbcSettings =
    new R2dbcSettings(config)
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class R2dbcSettings(config: Config) {
  val schema: Option[String] = Option(config.getString("schema")).filterNot(_.trim.isEmpty)

  val journalTable: String = config.getString("journal.table")
  val journalTableWithSchema: String = schema.map(_ + ".").getOrElse("") + journalTable

  val journalPublishEvents: Boolean = config.getBoolean("journal.publish-events")

  val snapshotsTable: String = config.getString("snapshot.table")
  val snapshotsTableWithSchema: String = schema.map(_ + ".").getOrElse("") + snapshotsTable

  val durableStateTable: String = config.getString("state.table")
  val durableStateTableWithSchema: String = schema.map(_ + ".").getOrElse("") + durableStateTable

  private val durableStateTableByEntityType: Map[String, String] =
    configToMap(config.getConfig("state.custom-table"))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val durableStateTableByEntityTypeWithSchema: Map[String, String] =
    durableStateTableByEntityType.map { case (entityType, table) =>
      entityType -> (schema.map(_ + ".").getOrElse("") + table)
    }

  def getDurableStateTable(entityType: String): String =
    durableStateTableByEntityType.getOrElse(entityType, durableStateTable)

  def getDurableStateTableWithSchema(entityType: String): String =
    durableStateTableByEntityTypeWithSchema.getOrElse(entityType, durableStateTableWithSchema)

  private def configToMap(cfg: Config): Map[String, String] = {
    import akka.util.ccompat.JavaConverters._
    cfg.root.unwrapped.asScala.toMap.map { case (k, v) => k -> v.toString }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val durableStateAdditionalColumnClasses: Map[String, immutable.IndexedSeq[String]] = {
    import akka.util.ccompat.JavaConverters._
    val cfg = config.getConfig("state.additional-columns")
    cfg.root.unwrapped.asScala.toMap.map {
      case (k, v: java.util.List[_]) => k -> v.iterator.asScala.map(_.toString).toVector
      case (k, v)                    => k -> Vector(v.toString)
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val durableStateChangeHandlerClasses: Map[String, String] =
    configToMap(config.getConfig("state.change-handler"))

  val durableStateAssertSingleWriter: Boolean = config.getBoolean("state.assert-single-writer")

  val dialect: Dialect = toRootLowerCase(config.getString("dialect")) match {
    case "yugabyte" => Dialect.Yugabyte
    case "postgres" => Dialect.Postgres
    case other =>
      throw new IllegalArgumentException(s"Unknown dialect [$other]. Supported dialects are [yugabyte, postgres].")
  }

  val querySettings = new QuerySettings(config.getConfig("query"))

  val connectionFactorySettings = new ConnectionFactorySettings(config.getConfig("connection-factory"))

  val dbTimestampMonotonicIncreasing: Boolean = config.getBoolean("db-timestamp-monotonic-increasing")

  /**
   * INTERNAL API FIXME remove when https://github.com/yugabyte/yugabyte-db/issues/10995 has been resolved
   */
  @InternalApi private[akka] val useAppTimestamp: Boolean = config.getBoolean("use-app-timestamp")

  val logDbCallsExceeding: FiniteDuration =
    config.getString("log-db-calls-exceeding").toLowerCase(Locale.ROOT) match {
      case "off" => -1.millis
      case _     => config.getDuration("log-db-calls-exceeding").asScala
    }

  val cleanupSettings = new CleanupSettings(config.getConfig("cleanup"))
}

/**
 * INTERNAL API
 */
@InternalStableApi
sealed trait Dialect

/**
 * INTERNAL API
 */
@InternalStableApi
object Dialect {
  case object Postgres extends Dialect
  case object Yugabyte extends Dialect
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
  val persistenceIdsBufferSize: Int = config.getInt("persistence-ids.buffer-size")
  val deduplicateCapacity: Int = config.getInt("deduplicate-capacity")
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class ConnectionFactorySettings(config: Config) {

  val urlOption: Option[String] =
    Option(config.getString("url"))
      .filter(_.trim.nonEmpty)

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
  val maxIdleTime: FiniteDuration = config.getDuration("max-idle-time").asScala
  val maxLifeTime: FiniteDuration = config.getDuration("max-life-time").asScala

  val connectTimeout: FiniteDuration = config.getDuration("connect-timeout").asScala
  val acquireTimeout: FiniteDuration = config.getDuration("acquire-timeout").asScala
  val acquireRetry: Int = config.getInt("acquire-retry")

  val validationQuery: String = config.getString("validation-query")

  val statementCacheSize: Int = config.getInt("statement-cache-size")
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class PublishEventsDynamicSettings(config: Config) {
  val throughputThreshold: Int = config.getInt("throughput-threshold")
  val throughputCollectInterval: FiniteDuration = config.getDuration("throughput-collect-interval").asScala
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class CleanupSettings(config: Config) {
  val logProgressEvery: Int = config.getInt("log-progress-every")
  val eventsJournalDeleteBatchSize: Int = config.getInt("events-journal-delete-batch-size")
}
