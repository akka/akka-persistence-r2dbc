/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.h2

import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao.EvaluatedAdditionalColumnBindings

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class H2JournalDao(executorProvider: R2dbcExecutorProvider)
    extends PostgresJournalDao(executorProvider) {
  import settings.codecSettings.JournalImplicits._

  import JournalDao.SerializedJournalRow
  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2JournalDao])
  // always app timestamp (db is same process) monotonic increasing
  require(settings.useAppTimestamp)
  require(settings.dbTimestampMonotonicIncreasing)

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  private def insertSql(
      entityType: String,
      slice: Int,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    def createSql = {
      val additionalCols = additionalInsertColumns(additionalBindings)
      val additionalParams = additionalInsertParameters(additionalBindings)
      sql"INSERT INTO ${journalTable(entityType, slice)} " +
      s"(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload$additionalCols, db_timestamp) " +
      s"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?$additionalParams, ?)"
    }
    if (additionalBindings.isEmpty)
      sqlCache.get(slice, s"insertSql-${settings.journalTableCacheKey(entityType)}")(createSql)
    else
      createSql
  }

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be the `db_timestamp` column and it is used in published events when that feature is
   * enabled.
   *
   * Note for implementing future database dialects: If a database dialect can't efficiently return the timestamp column
   * it can return `JournalDao.EmptyDbTimestamp` when the pub-sub feature is disabled. When enabled it would have to use
   * a select (in same transaction).
   */
  override def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId
    val entityType = events.head.entityType
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)

    evaluateBatchBindings(entityType, events) match {
      case Failure(exc) => Future.failed(exc)
      case Success((headBindings, perEventBindings)) =>
        val totalEvents = events.size
        val result =
          if (totalEvents == 1) {
            executor.updateOne(s"insert [$persistenceId]")(connection =>
              bindInsertStatement(
                connection.createStatement(insertSql(entityType, slice, headBindings)),
                events.head,
                headBindings))
          } else {
            executor.updateInBatch(s"batch insert [$persistenceId], [$totalEvents] events")(connection =>
              events.iterator.zipWithIndex
                .foldLeft(connection.createStatement(insertSql(entityType, slice, headBindings))) {
                  case (stmt, (write, idx)) =>
                    stmt.add()
                    val bindings = if (perEventBindings.isEmpty) headBindings else perEventBindings(idx)
                    bindInsertStatement(stmt, write, bindings)
                })
          }

        if (log.isDebugEnabled())
          result.foreach { _ =>
            log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
          }
        result.map(_ => events.head.dbTimestamp)(ExecutionContext.parasitic)
    }
  }

  override def writeEventInTx(event: SerializedJournalRow, connection: Connection): Future[Instant] = {
    val persistenceId = event.persistenceId
    val entityType = event.entityType
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val bindings = additionalBindings(entityType, event)

    val stmt = bindInsertStatement(connection.createStatement(insertSql(entityType, slice, bindings)), event, bindings)
    val result = R2dbcExecutor.updateOneInTx(stmt)

    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] event for persistenceId [{}]", 1, persistenceId)
      }
    result.map(_ => event.dbTimestamp)(ExecutionContext.parasitic)
  }

  private def bindInsertStatement(
      stmt: Statement,
      write: SerializedJournalRow,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
    val idx = Iterator.range(0, Int.MaxValue)
    stmt
      .bind(idx.next(), write.slice)
      .bind(idx.next(), write.entityType)
      .bind(idx.next(), write.persistenceId)
      .bind(idx.next(), write.seqNr)
      .bind(idx.next(), write.writerUuid)
      .bind(idx.next(), "") // FIXME event adapter
      .bind(idx.next(), write.serId)
      .bind(idx.next(), write.serManifest)
      .bindPayload(idx.next(), write.payload.get)

    val tagsIdx = idx.next()
    if (write.tags.isEmpty)
      stmt.bindNull(tagsIdx, classOf[Array[String]])
    else
      stmt.bind(tagsIdx, write.tags.toArray)

    // optional metadata
    write.metadata match {
      case Some(m) =>
        stmt
          .bind(idx.next(), m.serId)
          .bind(idx.next(), m.serManifest)
          .bind(idx.next(), m.payload)
      case None =>
        stmt
          .bindNull(idx.next(), classOf[Integer])
          .bindNull(idx.next(), classOf[String])
          .bindNull(idx.next(), classOf[Array[Byte]])
    }

    bindAdditionalColumns(stmt, additionalBindings, idx.next)

    stmt.bind(idx.next(), write.dbTimestamp)
    stmt
  }

}
