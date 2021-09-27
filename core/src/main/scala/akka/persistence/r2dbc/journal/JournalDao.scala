/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.serialization.Serialization
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object JournalDao {
  val log: Logger = LoggerFactory.getLogger(classOf[JournalDao])

  final case class SerializedJournalRow(
      persistenceId: String,
      sequenceNr: Long,
      payload: Array[Byte],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      timestamp: Long,
      tags: Set[String],
      metadata: Option[SerializedEventMetadata])

  final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

  object Schema {
    object Journal {
      def journalTable(settings: R2dbcSettings): String =
        s"""CREATE TABLE ${settings.journalTable} IF NOT EXISTS (
           |  persistence_id VARCHAR(255) NOT NULL,
           |  sequence_number BIGINT NOT NULL,
           |  deleted BOOLEAN DEFAULT FALSE NOT NULL,
           |  writer VARCHAR(255) NOT NULL,
           |  write_timestamp BIGINT,
           |  adapter_manifest VARCHAR(255),
           |  event_ser_id INTEGER NOT NULL,
           |  event_ser_manifest VARCHAR(255) NOT NULL,
           |  event_payload BYTEA NOT NULL,
           |  meta_ser_id INTEGER,
           |  meta_ser_manifest VARCHAR(255),
           |  meta_payload BYTEA,
           |  PRIMARY KEY(persistence_id, sequence_number)
           |)""".stripMargin

      def deserializeRow(
          settings: R2dbcSettings,
          serialization: Serialization,
          row: SerializedJournalRow): PersistentRepr = {
        val payload = serialization.deserialize(row.payload, row.serId, row.serManifest).get
        val repr = PersistentRepr(
          payload,
          row.sequenceNr,
          row.persistenceId,
          writerUuid = row.writerUuid,
          manifest = "", // FIXME
          deleted = false,
          sender = ActorRef.noSender).withTimestamp(row.timestamp)

        val reprWithMeta = row.metadata match {
          case Some(meta) =>
            repr.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
          case None => repr
        }
        reprWithMeta
      }
    }

  }
}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class JournalDao(journalSettings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {

  import JournalDao.Schema
  import JournalDao.SerializedJournalRow
  import JournalDao.log

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(ec, system)

  private val insertEventSql = s"INSERT INTO ${journalSettings.journalTable} " +
    "(persistence_id, sequence_number, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

  private val selectHighestSequenceNrSql = s"SELECT MAX(sequence_number) from ${journalSettings.journalTable} " +
    "WHERE persistence_id = $1 AND sequence_number >= $2"

  private val selectEventsSql = s"SELECT * from ${journalSettings.journalTable} " +
    "WHERE persistence_id = $1 AND sequence_number >= $2 AND sequence_number <= $3 " +
    "AND deleted = false " +
    "ORDER BY sequence_number"
  private val selectEventsWithLimitSql = selectEventsSql + " LIMIT $4"

  private val deleteEventsSql = s"DELETE FROM ${journalSettings.journalTable} " +
    "WHERE persistence_id = $1 AND sequence_number <= $2"
  private val insertDeleteMarkerSql = s"INSERT INTO ${journalSettings.journalTable} " +
    "(persistence_id, sequence_number, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

  def writeEvents(events: Seq[SerializedJournalRow]): Future[Unit] = {
    require(events.nonEmpty)
    val persistenceId = events.head.persistenceId

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind("$1", write.persistenceId)
        .bind("$2", write.sequenceNr)
        .bind("$3", write.writerUuid)
        .bind("$4", write.timestamp)
        .bind("$5", "") // FIXME
        .bind("$6", write.serId)
        .bind("$7", write.serManifest)
        .bind("$8", write.payload)
    }

    val result = {
      if (events.size == 1) {
        r2dbcExecutor.updateOne(s"insert [$persistenceId]") { connection =>
          val stmt =
            connection.createStatement(insertEventSql)
          if (events.size == 1)
            bind(stmt, events.head)
          else
            events.foldLeft(stmt) { (s, write) =>
              bind(s, write).add()
            }
        }
      } else {
        // TODO batch statements doesn't work stmt.bind().add().bind().execute()
        r2dbcExecutor
          .update(s"insert [$persistenceId]") { connection =>
            events.map { write =>
              val stmt =
                connection.createStatement(insertEventSql)
              bind(stmt, write)
            }.toIndexedSeq
          }
          .map(_.sum)
      }
    }

    if (log.isDebugEnabled())
      result.foreach(updatedRows =>
        log.debug("Wrote [{}] events for persistenceId [{}]", updatedRows, events.head.persistenceId))

    result.map(_ => ())(ExecutionContext.parasitic)
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection.createStatement(selectHighestSequenceNrSql).bind("$1", persistenceId).bind("$2", fromSequenceNr),
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def replayJournal(
      serialization: Serialization,
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long)(replay: PersistentRepr => Unit): Future[Unit] = {
    def replayRow(row: SerializedJournalRow): Unit = {
      val repr = Schema.Journal.deserializeRow(journalSettings, serialization, row)
      log.trace("replaying {}", repr)
      replay(repr)
    }

    val result = r2dbcExecutor.select(s"select replay [$persistenceId]")(
      connection => {
        val stmt = connection
          .createStatement(if (max == Long.MaxValue) selectEventsSql else selectEventsWithLimitSql)
          .bind("$1", persistenceId)
          .bind("$2", fromSequenceNr)
          .bind("$3", toSequenceNr)
        if (max != Long.MaxValue)
          stmt.bind("$$4", max)
        else
          stmt
      },
      row =>
        replayRow(
          SerializedJournalRow(
            persistenceId = persistenceId,
            sequenceNr = row.get("sequence_number", classOf[java.lang.Long]),
            payload = row.get("event_payload", classOf[Array[Byte]]),
            serId = row.get("event_ser_id", classOf[java.lang.Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = row.get("writer", classOf[String]),
            timestamp = row.get("write_timestamp", classOf[java.lang.Long]),
            tags = Set.empty, // not needed here
            metadata = None // FIXME
          )))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Replayed persistenceId [{}], [{}] events", persistenceId, rows.size))

    result.map(_ => ())(ExecutionContext.parasitic)
  }

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val deleteMarkerSeqNrFut =
      if (toSequenceNr == Long.MaxValue)
        readHighestSequenceNr(persistenceId, 0L)
      else
        Future.successful(toSequenceNr)

    deleteMarkerSeqNrFut.flatMap { deleteMarkerSeqNr =>
      def bindDeleteMarker(stmt: Statement): Statement = {
        stmt
          .bind("$1", persistenceId)
          .bind("$2", deleteMarkerSeqNr)
          .bind("$3", "")
          .bind("$4", System.currentTimeMillis())
          .bind("$5", "")
          .bind("$6", 0)
          .bind("$7", "")
          .bind("$8", Array.emptyByteArray)
          .bind("$9", true)
      }

      val result = r2dbcExecutor.update(s"delete [$persistenceId]") { connection =>
        Vector(
          connection.createStatement(deleteEventsSql).bind("$1", persistenceId).bind("$2", toSequenceNr),
          bindDeleteMarker(connection.createStatement(insertDeleteMarkerSql)))
      }

      if (log.isDebugEnabled)
        result.foreach(updatedRows =>
          log.debug("Deleted [{}] events for persistenceId [{}]", updatedRows.head, persistenceId))

      result.map(_ => ())(ExecutionContext.parasitic)
    }
  }

}
