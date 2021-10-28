/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.PersistentRepr
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SliceUtils
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
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedJournalRow(
      persistenceId: String,
      sequenceNr: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Array[Byte],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      timestamp: Long,
      tags: Set[String],
      metadata: Option[SerializedEventMetadata])
      extends BySliceQuery.SerializedRow

  final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

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
      case None => repr
      case Some(meta) =>
        repr.withMetadata(serialization.deserialize(meta.payload, meta.serId, meta.serManifest).get)
    }
    reprWithMeta
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

  import JournalDao.SerializedJournalRow
  import JournalDao.deserializeRow
  import JournalDao.log

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(ec, system)

  private val journalTable = journalSettings.journalTableWithSchema

  // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
  // always increasing for a pid (time not going backwards).
  // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
  private val insertEventSql = s"INSERT INTO $journalTable " +
    "(slice, entity_type_hint, persistence_id, sequence_number, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, db_timestamp) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, GREATEST(transaction_timestamp(), " +
    s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable WHERE slice = $$11 AND entity_type_hint = $$12 AND persistence_id = $$13 AND sequence_number = $$14)))"

  private val selectHighestSequenceNrSql = s"SELECT MAX(sequence_number) from $journalTable " +
    "WHERE slice = $1 AND entity_type_hint = $2 AND persistence_id = $3 AND sequence_number >= $4"

  private val selectEventsSql = s"SELECT * from $journalTable " +
    "WHERE slice = $1 AND entity_type_hint = $2 AND persistence_id = $3 AND sequence_number >= $4 AND sequence_number <= $5 " +
    "AND deleted = false " +
    "ORDER BY sequence_number"
  private val selectEventsWithLimitSql = selectEventsSql + " LIMIT $6"

  private val deleteEventsSql = s"DELETE FROM $journalTable " +
    "WHERE slice = $1 AND entity_type_hint = $2 AND persistence_id = $3 AND sequence_number <= $4"
  private val insertDeleteMarkerSql = s"INSERT INTO $journalTable " +
    "(slice, entity_type_hint, persistence_id, sequence_number, db_timestamp, writer, write_timestamp, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted) " +
    "VALUES ($1, $2, $3, $4, transaction_timestamp(), $5, $6, $7, $8, $9, $10, $11)"

  def writeEvents(events: Seq[SerializedJournalRow]): Future[Unit] = {
    require(events.nonEmpty)
    val persistenceId = events.head.persistenceId

    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, journalSettings.maxNumberOfSlices)
    val previousSeqNr = events.head.sequenceNr - 1

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, slice)
        .bind(1, entityTypeHint)
        .bind(2, write.persistenceId)
        .bind(3, write.sequenceNr)
        .bind(4, write.writerUuid)
        .bind(5, write.timestamp)
        .bind(6, "") // FIXME
        .bind(7, write.serId)
        .bind(8, write.serManifest)
        .bind(9, write.payload)
        .bind(10, slice)
        .bind(11, entityTypeHint)
        .bind(12, write.persistenceId)
        .bind(13, previousSeqNr)
    }

    val result = {
      if (events.size == 1) {
        r2dbcExecutor.updateOne(s"insert [$persistenceId]") { connection =>
          val stmt =
            connection.createStatement(insertEventSql)
          if (events.size == 1)
            bind(stmt, events.head)
          else
            // TODO this is not used yet, batch statements doesn't work stmt.bind().add().bind().execute()
            events.foldLeft(stmt) { (s, write) =>
              bind(s, write).add()
            }
        }
      } else {
        // TODO batch statements doesn't work, see above
        r2dbcExecutor
          .update(s"insert [$persistenceId]") { connection =>
            events.map { write =>
              val stmt = connection.createStatement(insertEventSql)
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
    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, journalSettings.maxNumberOfSlices)
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectHighestSequenceNrSql)
            .bind(0, slice)
            .bind(1, entityTypeHint)
            .bind(2, persistenceId)
            .bind(3, fromSequenceNr),
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
    def replayRow(row: SerializedJournalRow): SerializedJournalRow = {
      val repr = deserializeRow(journalSettings, serialization, row)
      replay(repr)
      row
    }

    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, journalSettings.maxNumberOfSlices)

    val result = r2dbcExecutor.select(s"select replay [$persistenceId]")(
      connection => {
        val stmt = connection
          .createStatement(if (max == Long.MaxValue) selectEventsSql else selectEventsWithLimitSql)
          .bind(0, slice)
          .bind(1, entityTypeHint)
          .bind(2, persistenceId)
          .bind(3, fromSequenceNr)
          .bind(4, toSequenceNr)
        if (max != Long.MaxValue)
          stmt.bind(5, max)
        else
          stmt
      },
      row =>
        replayRow(
          SerializedJournalRow(
            persistenceId = persistenceId,
            sequenceNr = row.get("sequence_number", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = Instant.EPOCH, // not needed here
            payload = row.get("event_payload", classOf[Array[Byte]]),
            serId = row.get("event_ser_id", classOf[java.lang.Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = row.get("writer", classOf[String]),
            timestamp = row.get("write_timestamp", classOf[java.lang.Long]),
            tags = Set.empty, // not needed here
            metadata = None // FIXME
          )))

    if (log.isDebugEnabled)
      result.foreach { rows =>
        log.debug("Replayed persistenceId [{}], [{}] events", persistenceId, rows.size)
        if (log.isTraceEnabled)
          rows.foreach { row: SerializedJournalRow =>
            log.debug(
              "Replayed persistenceId [{}], seqNr [{}], dbTimestamp [{}]",
              persistenceId,
              row.sequenceNr,
              row.dbTimestamp)
          }
      }

    result.map(_ => ())(ExecutionContext.parasitic)
  }

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, journalSettings.maxNumberOfSlices)

    val deleteMarkerSeqNrFut =
      if (toSequenceNr == Long.MaxValue)
        readHighestSequenceNr(persistenceId, 0L)
      else
        Future.successful(toSequenceNr)

    deleteMarkerSeqNrFut.flatMap { deleteMarkerSeqNr =>
      def bindDeleteMarker(stmt: Statement): Statement = {
        stmt
          .bind(0, slice)
          .bind(1, entityTypeHint)
          .bind(2, persistenceId)
          .bind(3, deleteMarkerSeqNr)
          .bind(4, "")
          .bind(5, System.currentTimeMillis())
          .bind(6, "")
          .bind(7, 0)
          .bind(8, "")
          .bind(9, Array.emptyByteArray)
          .bind(10, true)
      }

      val result = r2dbcExecutor.update(s"delete [$persistenceId]") { connection =>
        Vector(
          connection
            .createStatement(deleteEventsSql)
            .bind(0, slice)
            .bind(1, entityTypeHint)
            .bind(2, persistenceId)
            .bind(3, toSequenceNr),
          bindDeleteMarker(connection.createStatement(insertDeleteMarkerSql)))
      }

      if (log.isDebugEnabled)
        result.foreach(updatedRows =>
          log.debug("Deleted [{}] events for persistenceId [{}]", updatedRows.head, persistenceId))

      result.map(_ => ())(ExecutionContext.parasitic)
    }
  }

}
