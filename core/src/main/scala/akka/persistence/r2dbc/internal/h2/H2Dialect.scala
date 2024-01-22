/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.util.ccompat.JavaConverters._
import com.typesafe.config.Config
import io.r2dbc.h2.H2ConnectionConfiguration
import io.r2dbc.h2.H2ConnectionFactory
import io.r2dbc.h2.H2ConnectionOption
import io.r2dbc.spi.ConnectionFactory
import java.util.Locale

import scala.concurrent.ExecutionContext

import akka.persistence.r2dbc.internal.codec.IdentityAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object H2Dialect extends Dialect {

  override def name: String = "h2"

  override def adaptSettings(settings: R2dbcSettings): R2dbcSettings = {
    val res = settings
      // app timestamp is db timestamp because same process
      .withUseAppTimestamp(true)
      .withDbTimestampMonotonicIncreasing(true)
    res
  }

  override def createConnectionFactory(config: Config): ConnectionFactory = {
    // starting point for both url and regular configs,
    // to allow url to override anything but provide sane defaults
    val builder = H2ConnectionConfiguration.builder()
    val url = config.getString("url")
    if (url.nonEmpty) {
      builder.url(url)
    } else {
      val db = config.getString("database")
      config.getString("protocol").toLowerCase(Locale.ROOT) match {
        case "file" => builder.file(db)
        case "mem"  => builder.inMemory(db)
      }

      val createSliceIndexes = config.getBoolean("create-slice-indexes")
      builder
        // create schema on first connect
        .property(H2ConnectionOption.INIT, dbSchema(config, createSliceIndexes, config.getString("additional-init")))
        // don't auto close connections
        .property(H2ConnectionOption.DB_CLOSE_DELAY, "-1")

      // workaround for https://github.com/akka/akka-projection/issues/992
      builder.option("OPTIMIZE_REUSE_RESULTS=FALSE")

      if (config.getBoolean("trace-logging"))
        // log to SLF4J instead of print to stdout, logger name will be 'h2database'
        builder.property(H2ConnectionOption.TRACE_LEVEL_FILE, "4")

      // Arbitrary config pass through/override for non-url setup
      config
        .getConfig("additional-options")
        .entrySet()
        .iterator()
        .asScala
        .foreach(entry => builder.option(s"${entry.getKey}=${entry.getValue.render()}"))
    }

    val h2Config = builder.build()
    new H2ConnectionFactory(h2Config)
  }

  override def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): JournalDao =
    new H2JournalDao(settings, connectionFactory)(ecForDaos(system, settings), system)

  override def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): SnapshotDao =
    new H2SnapshotDao(settings, connectionFactory)(ecForDaos(system, settings), system)

  override def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): QueryDao =
    new H2QueryDao(settings, connectionFactory)(ecForDaos(system, settings), system)

  override def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      system: ActorSystem[_]): DurableStateDao =
    new H2DurableStateDao(settings, connectionFactory, this)(ecForDaos(system, settings), system)

  private def ecForDaos(system: ActorSystem[_], settings: R2dbcSettings): ExecutionContext = {
    // H2 R2DBC driver blocks in surprising places (Mono.toFuture in stmt.execute().asFuture())
    system.dispatchers.lookup(
      DispatcherSelector.fromConfig(settings.connectionFactorySettings.config.getString("use-dispatcher")))
  }

  private def dbSchema(config: Config, createSliceIndexes: Boolean, additionalInit: String): String = {
    def optionalConfString(name: String): Option[String] = {
      val s = config.getString(name)
      if (s.isEmpty) None
      else Some(s)
    }
    val schema = optionalConfString("schema")
    val journalTable = config.getString("journal-table")
    val journalTableWithSchema = schema.map(_ + ".").getOrElse("") + journalTable
    val snapshotTable = config.getString("snapshot-table")
    val snapshotTableWithSchema = schema.map(_ + ".").getOrElse("") + snapshotTable
    val durableStateTable = config.getString("state-table")
    val durableStateTableWithSchema = schema.map(_ + ".").getOrElse("") + durableStateTable

    implicit val queryAdapter: QueryAdapter = IdentityAdapter

    val sliceIndexes = if (createSliceIndexes) {
      val sliceIndexWithSchema = journalTableWithSchema + "_slice_idx"
      val snapshotSliceIndexWithSchema = snapshotTableWithSchema + "_slice_idx"
      val durableStateSliceIndexWithSchema = durableStateTableWithSchema + "_slice_idx"
      Seq(
        sql"""CREATE INDEX IF NOT EXISTS $sliceIndexWithSchema ON $journalTableWithSchema(slice, entity_type, db_timestamp, seq_nr)""",
        sql"""CREATE INDEX IF NOT EXISTS $snapshotSliceIndexWithSchema ON $snapshotTableWithSchema(slice, entity_type, db_timestamp)""",
        sql"""CREATE INDEX IF NOT EXISTS $durableStateSliceIndexWithSchema ON durable_state(slice, entity_type, db_timestamp, revision)""")
    } else Seq.empty[String]

    (Seq(
      sql"""CREATE TABLE IF NOT EXISTS $journalTableWithSchema (
        slice INT NOT NULL,
        entity_type VARCHAR(255) NOT NULL,
        persistence_id VARCHAR(255) NOT NULL,
        seq_nr BIGINT NOT NULL,
        db_timestamp timestamp with time zone NOT NULL,

        event_ser_id INTEGER NOT NULL,
        event_ser_manifest VARCHAR(255) NOT NULL,
        event_payload BYTEA NOT NULL,

        deleted BOOLEAN DEFAULT FALSE NOT NULL,
        writer VARCHAR(255) NOT NULL,
        adapter_manifest VARCHAR(255),
        tags TEXT ARRAY,

        meta_ser_id INTEGER,
        meta_ser_manifest VARCHAR(255),
        meta_payload BYTEA,

        PRIMARY KEY(persistence_id, seq_nr)
      )""",
      sql"""
        CREATE TABLE IF NOT EXISTS $snapshotTableWithSchema (
          slice INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          persistence_id VARCHAR(255) NOT NULL,
          seq_nr BIGINT NOT NULL,
          db_timestamp timestamp with time zone,
          write_timestamp BIGINT NOT NULL,
          ser_id INTEGER NOT NULL,
          ser_manifest VARCHAR(255) NOT NULL,
          snapshot BYTEA NOT NULL,
          tags TEXT ARRAY,
          meta_ser_id INTEGER,
          meta_ser_manifest VARCHAR(255),
          meta_payload BYTEA,

          PRIMARY KEY(persistence_id)
        )""",
      sql"""
        CREATE TABLE IF NOT EXISTS $durableStateTableWithSchema (
          slice INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          persistence_id VARCHAR(255) NOT NULL,
          revision BIGINT NOT NULL,
          db_timestamp timestamp with time zone NOT NULL,

          state_ser_id INTEGER NOT NULL,
          state_ser_manifest VARCHAR(255),
          state_payload BYTEA NOT NULL,
          tags TEXT ARRAY,

          PRIMARY KEY(persistence_id, revision)
        )
      """) ++ sliceIndexes ++ (if (additionalInit.trim.nonEmpty) Seq(additionalInit) else Seq.empty[String]))
      .mkString(";") // r2dbc h2 driver replaces with '\;' as needed for INIT
  }
}
