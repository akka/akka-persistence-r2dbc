/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.util.Locale
import scala.collection.immutable
import scala.concurrent.duration._
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.r2dbc.internal.{ Dialect, PayloadCodec }
import akka.persistence.r2dbc.internal.h2.H2Dialect
import akka.persistence.r2dbc.internal.postgres.PostgresDialect
import akka.persistence.r2dbc.internal.postgres.YugabyteDialect
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config
import akka.util.Helpers.toRootLowerCase

/**
 * INTERNAL API
 */
@InternalStableApi
object R2dbcSettings {

  private def configToMap(cfg: Config): Map[String, String] = {
    import akka.util.ccompat.JavaConverters._
    cfg.root.unwrapped.asScala.toMap.map { case (k, v) => k -> v.toString }
  }

  def apply(config: Config): R2dbcSettings = {
    val schema: Option[String] = Option(config.getString("schema")).filterNot(_.trim.isEmpty)

    val journalTable: String = config.getString("journal.table")

    def useJsonPayload(prefix: String) = config.getString(s"$prefix.payload-column-type").toUpperCase match {
      case "BYTEA"          => false
      case "JSONB" | "JSON" => true
      case t =>
        throw new IllegalStateException(
          s"Expected akka.persistence.r2dbc.$prefix.payload-column-type to be one of 'BYTEA', 'JSON' or 'JSONB' but found '$t'")
    }

    val journalPayloadCodec: PayloadCodec =
      if (useJsonPayload("journal")) PayloadCodec.JsonCodec else PayloadCodec.ByteArrayCodec

    val journalPublishEvents: Boolean = config.getBoolean("journal.publish-events")

    val snapshotsTable: String = config.getString("snapshot.table")

    val snapshotPayloadCodec: PayloadCodec =
      if (useJsonPayload("snapshot")) PayloadCodec.JsonCodec else PayloadCodec.ByteArrayCodec

    val durableStateTable: String = config.getString("state.table")

    val durableStatePayloadCodec: PayloadCodec =
      if (useJsonPayload("state")) PayloadCodec.JsonCodec else PayloadCodec.ByteArrayCodec

    val durableStateTableByEntityType: Map[String, String] =
      configToMap(config.getConfig("state.custom-table"))

    val durableStateAdditionalColumnClasses: Map[String, immutable.IndexedSeq[String]] = {
      import akka.util.ccompat.JavaConverters._
      val cfg = config.getConfig("state.additional-columns")
      cfg.root.unwrapped.asScala.toMap.map {
        case (k, v: java.util.List[_]) => k -> v.iterator.asScala.map(_.toString).toVector
        case (k, v)                    => k -> Vector(v.toString)
      }
    }

    val durableStateChangeHandlerClasses: Map[String, String] =
      configToMap(config.getConfig("state.change-handler"))

    val durableStateAssertSingleWriter: Boolean = config.getBoolean("state.assert-single-writer")

    val dialect: Dialect = toRootLowerCase(config.getString("dialect")) match {
      case "yugabyte" => YugabyteDialect: Dialect
      case "postgres" => PostgresDialect: Dialect
      case "h2"       => H2Dialect: Dialect
      case other =>
        throw new IllegalArgumentException(
          s"Unknown dialect [$other]. Supported dialects are [postgres, yugabyte, h2].")
    }

    val querySettings = new QuerySettings(config.getConfig("query"))

    val dbTimestampMonotonicIncreasing: Boolean = config.getBoolean("db-timestamp-monotonic-increasing")

    // FIXME remove when https://github.com/yugabyte/yugabyte-db/issues/10995 has been resolved
    val useAppTimestamp: Boolean = config.getBoolean("use-app-timestamp")

    val logDbCallsExceeding: FiniteDuration =
      config.getString("log-db-calls-exceeding").toLowerCase(Locale.ROOT) match {
        case "off" => -1.millis
        case _     => config.getDuration("log-db-calls-exceeding").asScala
      }

    val cleanupSettings = new CleanupSettings(config.getConfig("cleanup"))
    val settingsFromConfig = new R2dbcSettings(
      schema,
      journalTable,
      journalPayloadCodec,
      journalPublishEvents,
      snapshotsTable,
      snapshotPayloadCodec,
      durableStateTable,
      durableStatePayloadCodec,
      durableStateAssertSingleWriter,
      logDbCallsExceeding,
      querySettings,
      dbTimestampMonotonicIncreasing,
      cleanupSettings,
      dialect,
      durableStateTableByEntityType,
      durableStateAdditionalColumnClasses,
      durableStateChangeHandlerClasses,
      useAppTimestamp)

    settingsFromConfig.dialect.adaptSettings(settingsFromConfig)
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi
final class R2dbcSettings private (
    val schema: Option[String],
    val journalTable: String,
    val journalPayloadCodec: PayloadCodec,
    val journalPublishEvents: Boolean,
    val snapshotsTable: String,
    val snapshotPayloadCodec: PayloadCodec,
    val durableStateTable: String,
    val durableStatePayloadCodec: PayloadCodec,
    val durableStateAssertSingleWriter: Boolean,
    val logDbCallsExceeding: FiniteDuration,
    val querySettings: QuerySettings,
    val dbTimestampMonotonicIncreasing: Boolean,
    val cleanupSettings: CleanupSettings,
    _dialect: Dialect,
    _durableStateTableByEntityType: Map[String, String],
    _durableStateAdditionalColumnClasses: Map[String, immutable.IndexedSeq[String]],
    _durableStateChangeHandlerClasses: Map[String, String],
    _useAppTimestamp: Boolean) {

  val journalTableWithSchema: String = schema.map(_ + ".").getOrElse("") + journalTable
  val snapshotsTableWithSchema: String = schema.map(_ + ".").getOrElse("") + snapshotsTable
  val durableStateTableWithSchema: String = schema.map(_ + ".").getOrElse("") + durableStateTable

  /**
   * One of the supported dialects 'postgres', 'yugabyte' or 'h2'
   */
  val dialectName: String = _dialect.name

  def getDurableStateTable(entityType: String): String =
    _durableStateTableByEntityType.getOrElse(entityType, durableStateTable)

  def getDurableStateTableWithSchema(entityType: String): String =
    durableStateTableByEntityTypeWithSchema.getOrElse(entityType, durableStateTableWithSchema)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def withDbTimestampMonotonicIncreasing(
      dbTimestampMonotonicIncreasing: Boolean): R2dbcSettings =
    copy(dbTimestampMonotonicIncreasing = dbTimestampMonotonicIncreasing)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def withUseAppTimestamp(useAppTimestamp: Boolean): R2dbcSettings =
    copy(useAppTimestamp = useAppTimestamp)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val durableStateTableByEntityTypeWithSchema: Map[String, String] =
    _durableStateTableByEntityType.map { case (entityType, table) =>
      entityType -> (schema.map(_ + ".").getOrElse("") + table)
    }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def dialect: Dialect = _dialect

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def durableStateChangeHandlerClasses: Map[String, String] =
    _durableStateChangeHandlerClasses

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def durableStateAdditionalColumnClasses: Map[String, immutable.IndexedSeq[String]] =
    _durableStateAdditionalColumnClasses

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def useAppTimestamp: Boolean = _useAppTimestamp

  private def copy(
      schema: Option[String] = schema,
      journalTable: String = journalTable,
      journalPayloadCodec: PayloadCodec = journalPayloadCodec,
      journalPublishEvents: Boolean = journalPublishEvents,
      snapshotsTable: String = snapshotsTable,
      snapshotPayloadCodec: PayloadCodec = snapshotPayloadCodec,
      durableStateTable: String = durableStateTable,
      durableStatePayloadCodec: PayloadCodec = durableStatePayloadCodec,
      durableStateAssertSingleWriter: Boolean = durableStateAssertSingleWriter,
      logDbCallsExceeding: FiniteDuration = logDbCallsExceeding,
      querySettings: QuerySettings = querySettings,
      dbTimestampMonotonicIncreasing: Boolean = dbTimestampMonotonicIncreasing,
      cleanupSettings: CleanupSettings = cleanupSettings,
      dialect: Dialect = _dialect,
      durableStateTableByEntityType: Map[String, String] = _durableStateTableByEntityType,
      durableStateAdditionalColumnClasses: Map[String, immutable.IndexedSeq[String]] =
        _durableStateAdditionalColumnClasses,
      durableStateChangeHandlerClasses: Map[String, String] = _durableStateChangeHandlerClasses,
      useAppTimestamp: Boolean = _useAppTimestamp): R2dbcSettings =
    new R2dbcSettings(
      schema,
      journalTable,
      journalPayloadCodec,
      journalPublishEvents,
      snapshotsTable,
      snapshotPayloadCodec,
      durableStateTable,
      durableStatePayloadCodec,
      durableStateAssertSingleWriter,
      logDbCallsExceeding,
      querySettings,
      dbTimestampMonotonicIncreasing,
      cleanupSettings,
      _dialect,
      _durableStateTableByEntityType,
      _durableStateAdditionalColumnClasses,
      _durableStateChangeHandlerClasses,
      useAppTimestamp)

  override def toString =
    s"R2dbcSettings(dialectName=$dialectName, schema=$schema, journalTable=$journalTable, snapshotsTable=$snapshotsTable, durableStateTable=$durableStateTable, logDbCallsExceeding=$logDbCallsExceeding, dbTimestampMonotonicIncreasing=$dbTimestampMonotonicIncreasing, useAppTimestamp=$useAppTimestamp)"
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
final class ConnectionPoolSettings(config: Config) {
  val initialSize: Int = config.getInt("initial-size")
  val maxSize: Int = config.getInt("max-size")
  val maxIdleTime: FiniteDuration = config.getDuration("max-idle-time").asScala
  val maxLifeTime: FiniteDuration = config.getDuration("max-life-time").asScala

  val acquireTimeout: FiniteDuration = config.getDuration("acquire-timeout").asScala
  val acquireRetry: Int = config.getInt("acquire-retry")

  val validationQuery: String = config.getString("validation-query")
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
