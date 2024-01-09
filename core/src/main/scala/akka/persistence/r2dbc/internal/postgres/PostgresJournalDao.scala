/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.{
  JournalDao,
  PayloadCodec,
  R2dbcExecutor,
  SerializedEventMetadata,
  TimestampCodec
}
import akka.persistence.r2dbc.internal.TimestampCodec.RichRow
import akka.persistence.r2dbc.internal.postgres.sql.{ BaseJournalSql, PostgresJournalSql }
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresJournalDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresJournalDao])

  def readMetadata(row: Row): Option[SerializedEventMetadata] = {
    row.get("meta_payload", classOf[Array[Byte]]) match {
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get[Integer]("meta_ser_id", classOf[Integer]),
            serManifest = row.get("meta_ser_manifest", classOf[String]),
            metaPayload))
    }
  }

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class PostgresJournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends JournalDao {

  import JournalDao.SerializedJournalRow

  protected def log: Logger = PostgresJournalDao.log

  private val persistenceExt = Persistence(system)

  protected val r2dbcExecutor =
    new R2dbcExecutor(
      connectionFactory,
      log,
      journalSettings.logDbCallsExceeding,
      journalSettings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  protected val journalTable = journalSettings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = journalSettings.journalPayloadCodec
  protected implicit val timestampCodec: TimestampCodec = journalSettings.timestampCodec

  protected val journalSql: BaseJournalSql = new PostgresJournalSql(journalSettings)

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
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    val insertSql = journalSql.insertSql(useTimestampFromDb)

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => {
          journalSql.bindInsertForWriteEvent(
            connection.createStatement(insertSql),
            events.head,
            useTimestampFromDb,
            previousSeqNr)
        },
        journalSql.parseInsertForWriteEvent)
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, persistenceId)
        }
      result
    } else {
      val result = r2dbcExecutor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
        connection =>
          events.foldLeft(connection.createStatement(insertSql)) { (stmt, write) =>
            stmt.add()
            journalSql.bindInsertForWriteEvent(stmt, write, useTimestampFromDb, previousSeqNr)
          },
        journalSql.parseInsertForWriteEvent)
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", totalEvents, persistenceId)
        }
      result.map(_.head)(ExecutionContexts.parasitic)
    }
  }

  override def writeEventInTx(event: SerializedJournalRow, connection: Connection): Future[Instant] = {
    val persistenceId = event.persistenceId
    val previousSeqNr = event.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = event.dbTimestamp == Instant.EPOCH

    val insertSql = journalSql.insertSql(useTimestampFromDb)

    val stmt = journalSql.bindInsertForWriteEvent(
      connection.createStatement(insertSql),
      event,
      useTimestampFromDb,
      previousSeqNr)
    val result = R2dbcExecutor.updateOneReturningInTx(stmt, row => row.getTimestamp())
    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote [{}] event for persistenceId [{}]", 1, persistenceId)
      }
    result
  }

  override def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        { connection =>
          val stmt = connection.createStatement(journalSql.selectHighestSequenceNrSql)
          journalSql.bindSelectHighestSequenceNrSql(stmt, persistenceId, fromSequenceNr)
        },
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  override def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select lowest seqNr [$persistenceId]")(
        { connection =>
          val stmt = connection.createStatement(journalSql.selectLowestSequenceNrSql)
          journalSql.bindSelectLowestSequenceNrSql(stmt, persistenceId)
        },
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContexts.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  private def highestSeqNrForDelete(persistenceId: String, toSequenceNr: Long): Future[Long] = {
    if (toSequenceNr == Long.MaxValue)
      readHighestSequenceNr(persistenceId, 0L)
    else
      Future.successful(toSequenceNr)
  }

  private def lowestSequenceNrForDelete(persistenceId: String, toSeqNr: Long, batchSize: Int): Future[Long] = {
    if (toSeqNr <= batchSize) {
      Future.successful(1L)
    } else {
      readLowestSequenceNr(persistenceId)
    }
  }

  override def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit] = {

    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      val stmt = connection
        .createStatement(journalSql.insertDeleteMarkerSql)
      journalSql.bindForInsertDeleteMarkerSql(stmt, slice, entityType, persistenceId, deleteMarkerSeqNr)
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      (if (lastBatch && !resetSequenceNumber) {
         r2dbcExecutor
           .update(s"delete [$persistenceId] and insert marker") { connection =>
             Vector(
               {
                 val stmt = connection.createStatement(journalSql.deleteEventsSql)
                 journalSql.bindForDeleteEventsSql(stmt, persistenceId, from, to)
               },
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)
       } else {
         r2dbcExecutor
           .updateOne(s"delete [$persistenceId]") { connection =>
             val stmt = connection.createStatement(journalSql.deleteEventsSql)
             journalSql.bindForDeleteEventsSql(stmt, persistenceId, from, to)
           }
       }).map(deletedRows =>
        if (log.isDebugEnabled) {
          log.debugN(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows,
            persistenceId,
            from,
            to)
        })(ExecutionContexts.parasitic)
    }

    val batchSize = journalSettings.cleanupSettings.eventsJournalDeleteBatchSize

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] = {
      if (from + batchSize > maxTo) {
        deleteBatch(from, maxTo, true)
      } else {
        val to = from + batchSize - 1
        deleteBatch(from, to, false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }
    }

    for {
      toSeqNr <- highestSeqNrForDelete(persistenceId, toSequenceNr)
      fromSeqNr <- lowestSequenceNrForDelete(persistenceId, toSeqNr, batchSize)
      _ <- deleteInBatches(fromSeqNr, toSeqNr)
    } yield ()
  }

  override def deleteEventsBefore(persistenceId: String, timestamp: Instant): Future[Unit] = {
    r2dbcExecutor
      .updateOne(s"delete [$persistenceId]") { connection =>
        journalSql.deleteEventsByPersistenceIdBeforeTimestamp(connection.createStatement, persistenceId, timestamp)
      }
      .map(deletedRows =>
        log.debugN("Deleted [{}] events for persistenceId [{}], before [{}]", deletedRows, persistenceId, timestamp))(
        ExecutionContexts.parasitic)
  }

  override def deleteEventsBefore(entityType: String, slice: Int, timestamp: Instant): Future[Unit] = {
    r2dbcExecutor
      .updateOne(s"delete [$entityType]") { connection =>
        journalSql.deleteEventsBySliceBeforeTimestamp(connection.createStatement, slice, entityType, timestamp)
      }
      .map(deletedRows =>
        log.debugN(
          "Deleted [{}] events for entityType [{}], slice [{}], before [{}]",
          deletedRows,
          entityType,
          slice,
          timestamp))(ExecutionContexts.parasitic)
  }

}
