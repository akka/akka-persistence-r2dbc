/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.sqlserver

import scala.collection.immutable

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
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao.EvaluatedAdditionalColumnBindings

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

  override protected def insertEventWithParameterTimestampSql(
      entityType: String,
      slice: Int,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    def createSql = {
      // Reuse the base column/parameter assembly. The base emits positional `?` placeholders for the additional
      // columns; the `sql` interpolator rewrites them to `@p1, @p2, …`. They bind positionally by index, the same way
      // as the explicit `@slice`/`@dbTimestamp` placeholders, mirroring SqlServerDurableStateDao.
      val additionalCols = additionalInsertColumns(additionalBindings)
      val additionalParams = additionalInsertParameters(additionalBindings)
      sql"""
      INSERT INTO ${journalTable(entityType, slice)}
      (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload$additionalCols, db_timestamp)
      OUTPUT inserted.db_timestamp
      VALUES (@slice, @entityType, @persistenceId, @seqNr, @writer, @adapterManifest, @eventSerId, @eventSerManifest, @eventPayload, @tags, @metaSerId, @metaSerManifest, @metaSerPayload$additionalParams, @dbTimestamp)"""
    }
    if (additionalBindings.isEmpty)
      sqlCache.get(slice, s"insertEventWithParameterTimestampSql-${settings.journalTableCacheKey(entityType)}")(
        createSql)
    else
      createSql
  }

  override protected def bindTimestampNow(stmt: Statement, getAndIncIndex: () => Int): Statement =
    stmt.bindTimestamp(getAndIncIndex(), InstantFactory.now())

  override def insertDeleteMarkerSql(entityType: String, slice: Int, timestamp: String): String =
    super.insertDeleteMarkerSql(entityType, slice, "?")
}
