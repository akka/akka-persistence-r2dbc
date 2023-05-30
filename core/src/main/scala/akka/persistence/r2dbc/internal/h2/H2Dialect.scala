/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.query.scaladsl.QueryDao
import akka.persistence.r2dbc.snapshot.SnapshotDao
import akka.persistence.r2dbc.state.scaladsl.DurableStateDao
import com.typesafe.config.Config
import io.r2dbc.spi.{ ConnectionFactories, ConnectionFactory, ConnectionFactoryOptions }

import scala.concurrent.ExecutionContext
import io.r2dbc.h2.{ H2ConnectionFactoryProvider, H2ConnectionOption }

import akka.util.ccompat.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object H2Dialect extends Dialect {

  override def name: String = "h2"

  override def createConnectionFactory(settings: R2dbcSettings, config: Config): ConnectionFactory = {
    def r2option[T](h2Option: H2ConnectionOption): io.r2dbc.spi.Option[T] =
      io.r2dbc.spi.Option.valueOf[T](h2Option.getKey)

    val url = config.getString("url")
    val builder =
      if (url.isEmpty) {
        ConnectionFactoryOptions
          .builder()
          .option(ConnectionFactoryOptions.DRIVER, "h2")
          // note: protocol other than mem or file is validated in r2db driver
          .option(ConnectionFactoryOptions.PROTOCOL, config.getString("protocol"))
          .option(ConnectionFactoryOptions.DATABASE, config.getString("database"))
          .option(r2option(H2ConnectionOption.DB_CLOSE_DELAY), config.getString("dbCloseDelay"))
      } else {
        ConnectionFactoryOptions
          .parse(url)
          .mutate()
      }

    // for both url and regular configs
    builder
      // log to SLF4J instead of print to stdout, logger name will be 'h2database'
      .option(r2option(H2ConnectionOption.TRACE_LEVEL_FILE), "4")
      // create schema on first connect
      .option(r2option(H2ConnectionOption.INIT), dbSchema(settings))
      .option(io.r2dbc.spi.Option.valueOf("LOCK_TIMEOUT"), "10000")

    ConnectionFactories.get(builder.build())
  }

  override def createJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): JournalDao = new H2JournalDao(settings, connectionFactory)

  override def createSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): SnapshotDao = new H2SnapshotDao(settings, connectionFactory)

  override def createQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): QueryDao = new H2QueryDao(settings, connectionFactory)

  override def createDurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): DurableStateDao = new H2DurableStateDao(settings, connectionFactory)

  private def dbSchema(settings: R2dbcSettings): String =
    Seq(
      sql"""CREATE TABLE IF NOT EXISTS ${settings.journalTableWithSchema}(
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
        CREATE TABLE IF NOT EXISTS ${settings.snapshotsTableWithSchema}(
          slice INT NOT NULL,
          entity_type VARCHAR(255) NOT NULL,
          persistence_id VARCHAR(255) NOT NULL,
          seq_nr BIGINT NOT NULL,
          write_timestamp BIGINT NOT NULL,
          ser_id INTEGER NOT NULL,
          ser_manifest VARCHAR(255) NOT NULL,
          snapshot BYTEA NOT NULL,
          meta_ser_id INTEGER,
          meta_ser_manifest VARCHAR(255),
          meta_payload BYTEA,

          PRIMARY KEY(persistence_id)
        )""",
      sql"""
        CREATE TABLE IF NOT EXISTS ${settings.durableStateTableWithSchema} (
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
      """).mkString(";") // r2dbc h2 driver replaces with '\;' as needed for INIT
}
