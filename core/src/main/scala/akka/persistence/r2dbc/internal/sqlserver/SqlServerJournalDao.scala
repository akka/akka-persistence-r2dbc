/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerJournalDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerJournalDao])
  val TRUE = "1"
}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresJournalDao(settings, connectionFactory) {

  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.use-app-timestamp=on")
  require(
    settings.dbTimestampMonotonicIncreasing,
    "SqlServer requires akka.persistence.r2dbc.db-timestamp-monotonic-increasing=on")

  override def log = SqlServerJournalDao.log

  override protected val insertEventWithParameterTimestampSql =
    sql"""
      INSERT INTO $journalTable
      (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
      OUTPUT inserted.db_timestamp
      VALUES (@slice, @entityType, @persistenceId, @seqNr, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @tags, @metaSerId, @metaSerManifest, @metaSerPayload, @dbTimestamp)"""

  override protected def bindTimestampNow(stmt: Statement, getAndIncIndex: () => Int): Statement =
    stmt.bind(getAndIncIndex(), timestampCodec.now())

  override def insertDeleteMarkerSql(timestamp: String): String = super.insertDeleteMarkerSql("?")
}
