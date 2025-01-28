/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerJournalDao])
  val TRUE = "1"
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class SqlServerJournalDao(executorProvider: R2dbcExecutorProvider)
    extends PostgresJournalDao(executorProvider) {
  import settings.codecSettings.JournalImplicits._

  require(settings.useAppTimestamp, "SqlServer requires akka.persistence.r2dbc.use-app-timestamp=on")
  require(
    settings.dbTimestampMonotonicIncreasing,
    "SqlServer requires akka.persistence.r2dbc.db-timestamp-monotonic-increasing=on")

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  override def log = SqlServerJournalDao.log

  override protected def insertEventWithParameterTimestampSql(slice: Int) =
    sqlCache.get(slice, "insertEventWithParameterTimestampSql") {
      sql"""
      INSERT INTO ${journalTable(slice)}
      (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
      OUTPUT inserted.db_timestamp
      VALUES (@slice, @entityType, @persistenceId, @seqNr, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @tags, @metaSerId, @metaSerManifest, @metaSerPayload, @dbTimestamp)"""
    }

  override protected def bindTimestampNow(stmt: Statement, getAndIncIndex: () => Int): Statement =
    stmt.bindTimestamp(getAndIncIndex(), InstantFactory.now())

  override def insertDeleteMarkerSql(slice: Int, timestamp: String): String =
    super.insertDeleteMarkerSql(slice, "?")
}
