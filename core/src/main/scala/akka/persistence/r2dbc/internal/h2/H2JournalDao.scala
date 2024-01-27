/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.JournalDao
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.r2dbc.spi.Connection

import akka.persistence.r2dbc.internal.R2dbcExecutor

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class H2JournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresJournalDao(journalSettings, connectionFactory) {
  import JournalDao.SerializedJournalRow
  override protected lazy val log: Logger = LoggerFactory.getLogger(classOf[H2JournalDao])
  // always app timestamp (db is same process) monotonic increasing
  require(journalSettings.useAppTimestamp)
  require(journalSettings.dbTimestampMonotonicIncreasing)

  private val insertSql = sql"INSERT INTO $journalTable " +
    "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

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

    val totalEvents = events.size
    val result =
      if (totalEvents == 1) {
        r2dbcExecutor.updateOne(s"insert [$persistenceId]")(connection =>
          bindInsertStatement(connection.createStatement(insertSql), events.head))
      } else {
        r2dbcExecutor.updateInBatch(s"batch insert [$persistenceId], [$totalEvents] events")(connection =>
          events.foldLeft(connection.createStatement(insertSql)) { (stmt, write) =>
            stmt.add()
            bindInsertStatement(stmt, write)
          })
      }

    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
      }
    result.map(_ => events.head.dbTimestamp)(ExecutionContexts.parasitic)
  }

  override def writeEventInTx(event: SerializedJournalRow, connection: Connection): Future[Instant] = {
    val persistenceId = event.persistenceId

    val stmt = bindInsertStatement(connection.createStatement(insertSql), event)
    val result = R2dbcExecutor.updateOneInTx(stmt)

    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] event for persistenceId [{}]", 1, persistenceId)
      }
    result.map(_ => event.dbTimestamp)(ExecutionContexts.parasitic)
  }

  private def bindInsertStatement(stmt: Statement, write: SerializedJournalRow): Statement = {
    stmt
      .bind(0, write.slice)
      .bind(1, write.entityType)
      .bind(2, write.persistenceId)
      .bind(3, write.seqNr)
      .bind(4, write.writerUuid)
      .bind(5, "") // FIXME event adapter
      .bind(6, write.serId)
      .bind(7, write.serManifest)
      .bindPayload(8, write.payload.get)

    if (write.tags.isEmpty)
      stmt.bindNull(9, classOf[Array[String]])
    else
      stmt.bind(9, write.tags.toArray)

    // optional metadata
    write.metadata match {
      case Some(m) =>
        stmt
          .bind(10, m.serId)
          .bind(11, m.serManifest)
          .bind(12, m.payload)
      case None =>
        stmt
          .bindNull(10, classOf[Integer])
          .bindNull(11, classOf[String])
          .bindNull(12, classOf[Array[Byte]])
    }
    stmt.bind(13, write.dbTimestamp)
    stmt
  }

}
