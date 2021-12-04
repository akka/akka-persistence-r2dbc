/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.Sql.Interpolation
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
private[r2dbc] object SnapshotDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SnapshotDao])

  final case class SerializedSnapshotRow(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Long,
      snapshot: Array[Byte],
      serializerId: Int,
      serializerManifest: String,
      metadata: Option[SerializedSnapshotMetadata])

  final case class SerializedSnapshotMetadata(payload: Array[Byte], serializerId: Int, serializerManifest: String)

  private def collectSerializedSnapshot(row: Row): SerializedSnapshotRow =
    SerializedSnapshotRow(
      row.get("persistence_id", classOf[String]),
      row.get("seq_nr", classOf[java.lang.Long]),
      row.get("write_timestamp", classOf[java.lang.Long]),
      row.get("snapshot", classOf[Array[Byte]]),
      row.get("ser_id", classOf[Integer]),
      row.get("ser_manifest", classOf[String]), {
        val metaSerializerId = row.get("meta_ser_id", classOf[Integer])
        if (metaSerializerId eq null) None
        else
          Some(
            SerializedSnapshotMetadata(
              row.get("meta_payload", classOf[Array[Byte]]),
              metaSerializerId,
              row.get("meta_ser_manifest", classOf[String])))
      })

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] final class SnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import SnapshotDao._

  private val snapshotTable = settings.snapshotsTableWithSchema
  private val persistenceExt = Persistence(system)
  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  private val upsertSql = sql"""
    INSERT INTO $snapshotTable
    (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (slice, entity_type, persistence_id)
    DO UPDATE SET
      seq_nr = excluded.seq_nr,
      write_timestamp = excluded.write_timestamp,
      snapshot = excluded.snapshot,
      ser_id = excluded.ser_id,
      ser_manifest = excluded.ser_manifest,
      meta_payload = excluded.meta_payload,
      meta_ser_id = excluded.meta_ser_id,
      meta_ser_manifest = excluded.meta_ser_manifest"""

  private def selectSql(criteria: SnapshotSelectionCriteria): String = {
    val maxSeqNrCondition =
      if (criteria.maxSequenceNr != Long.MaxValue) " AND seq_nr <= ?"
      else ""

    val minSeqNrCondition =
      if (criteria.minSequenceNr > 0L) " AND seq_nr >= ?"
      else ""

    val maxTimestampCondition =
      if (criteria.maxTimestamp != Long.MaxValue) " AND write_timestamp <= ?"
      else ""

    val minTimestampCondition =
      if (criteria.minTimestamp != 0L) " AND write_timestamp >= ?"
      else ""

    sql"""
      SELECT persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE slice = ? AND entity_type = ? AND persistence_id = ?
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
      LIMIT 1"""
  }

  private def deleteSql(criteria: SnapshotSelectionCriteria): String = {
    val maxSeqNrCondition =
      if (criteria.maxSequenceNr != Long.MaxValue) " AND seq_nr <= ?"
      else ""

    val minSeqNrCondition =
      if (criteria.minSequenceNr > 0L) " AND seq_nr >= ?"
      else ""

    val maxTimestampCondition =
      if (criteria.maxTimestamp != Long.MaxValue) " AND write_timestamp <= ?"
      else ""

    val minTimestampCondition =
      if (criteria.minTimestamp != 0L) " AND write_timestamp >= ?"
      else ""

    sql"""
      DELETE FROM $snapshotTable
      WHERE slice = ? AND entity_type = ? AND persistence_id = ?
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition"""
  }

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    r2dbcExecutor
      .select(s"select snapshot [$persistenceId], criteria: [$criteria]")(
        { connection =>
          val statement = connection
            .createStatement(selectSql(criteria))
            .bind(0, slice)
            .bind(1, entityType)
            .bind(2, persistenceId)

          var bindIdx = 2
          if (criteria.maxSequenceNr != Long.MaxValue) {
            bindIdx += 1
            statement.bind(bindIdx, criteria.maxSequenceNr)
          }
          if (criteria.minSequenceNr > 0L) {
            bindIdx += 1
            statement.bind(bindIdx, criteria.minSequenceNr)
          }
          if (criteria.maxTimestamp != Long.MaxValue) {
            bindIdx += 1
            statement.bind(bindIdx, criteria.maxTimestamp)
          }
          if (criteria.minTimestamp > 0L) {
            bindIdx += 1
            statement.bind(bindIdx, criteria.minTimestamp)
          }
          statement
        },
        collectSerializedSnapshot)
      .map(_.headOption)(ExecutionContexts.parasitic)

  }

  def store(serializedRow: SerializedSnapshotRow): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(serializedRow.persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(serializedRow.persistenceId)

    r2dbcExecutor
      .updateOne(s"upsert snapshot [${serializedRow.persistenceId}], sequence number [${serializedRow.seqNr}]") {
        connection =>
          val statement =
            connection
              .createStatement(upsertSql)
              .bind(0, slice)
              .bind(1, entityType)
              .bind(2, serializedRow.persistenceId)
              .bind(3, serializedRow.seqNr)
              .bind(4, serializedRow.writeTimestamp)
              .bind(5, serializedRow.snapshot)
              .bind(6, serializedRow.serializerId)
              .bind(7, serializedRow.serializerManifest)

          serializedRow.metadata match {
            case Some(SerializedSnapshotMetadata(serializedMeta, serializerId, serializerManifest)) =>
              statement
                .bind(8, serializedMeta)
                .bind(9, serializerId)
                .bind(10, serializerManifest)
            case None =>
              statement
                .bindNull(8, classOf[Array[Byte]])
                .bindNull(9, classOf[Integer])
                .bindNull(10, classOf[String])
          }

          statement
      }
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    r2dbcExecutor.updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
      val statement = connection
        .createStatement(deleteSql(criteria))
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)

      var bindIdx = 2
      if (criteria.maxSequenceNr != Long.MaxValue) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.maxSequenceNr)
      }
      if (criteria.minSequenceNr > 0L) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.minSequenceNr)
      }
      if (criteria.maxTimestamp != Long.MaxValue) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.maxTimestamp)
      }
      if (criteria.minTimestamp > 0L) {
        bindIdx += 1
        statement.bind(bindIdx, criteria.minTimestamp)
      }
      statement
    }
  }.map(_ => ())(ExecutionContexts.parasitic)

}
