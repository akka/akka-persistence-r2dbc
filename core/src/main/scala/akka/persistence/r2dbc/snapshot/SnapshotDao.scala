/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SliceUtils
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
      row.get("ser_id", classOf[java.lang.Integer]),
      row.get("ser_manifest", classOf[String]), {
        val metaSerializerId = row.get("meta_ser_id", classOf[java.lang.Integer])
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
  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(ec, system)

  private val upsertSql =
    s"INSERT INTO $snapshotTable " +
    "(slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)" +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) " +
    "ON CONFLICT (slice, entity_type, persistence_id) " +
    "DO UPDATE SET " +
    "seq_nr = excluded.seq_nr, " +
    "write_timestamp = excluded.write_timestamp, " +
    "snapshot = excluded.snapshot, " +
    "ser_id = excluded.ser_id, " +
    "ser_manifest = excluded.ser_manifest, " +
    "meta_payload = excluded.meta_payload, " +
    "meta_ser_id = excluded.meta_ser_id, " +
    "meta_ser_manifest = excluded.meta_ser_manifest"

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]] = {
    val entityType = SliceUtils.extractEntityTypeFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, settings.maxNumberOfSlices)

    var paramIdx = 3
    val selectSnapshots = s"SELECT * FROM $snapshotTable " +
      s"WHERE slice = $$1 AND entity_type = $$2 AND persistence_id = $$3" +
      (if (criteria.maxSequenceNr != Long.MaxValue) {
         paramIdx += 1
         s" AND seq_nr <= $$$paramIdx"
       } else "") +
      (if (criteria.minSequenceNr > 0L) {
         paramIdx += 1
         s" AND seq_nr >= $$$paramIdx"
       } else "") +
      (if (criteria.maxTimestamp != Long.MaxValue) {
         paramIdx += 1
         s" AND write_timestamp <= $$$paramIdx"
       } else "") +
      (if (criteria.minTimestamp != 0L) {
         paramIdx += 1
         s" AND write_timestamp >= $$$paramIdx"
       } else "") +
      " ORDER BY seq_nr DESC LIMIT 1"

    r2dbcExecutor
      .select(s"select snapshot [$persistenceId], criteria: [$criteria]")(
        { connection =>
          val statement = connection
            .createStatement(selectSnapshots)
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
    val entityType = SliceUtils.extractEntityTypeFromPersistenceId(serializedRow.persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(serializedRow.persistenceId, settings.maxNumberOfSlices)

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
                .bindNull(9, classOf[java.lang.Integer])
                .bindNull(10, classOf[String])
          }

          statement
      }
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val entityType = SliceUtils.extractEntityTypeFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, settings.maxNumberOfSlices)

    var paramIdx = 3
    val deleteSnapshots = s"DELETE FROM $snapshotTable " +
      s"WHERE slice = $$1 AND entity_type = $$2 AND persistence_id = $$3" +
      (if (criteria.maxSequenceNr != Long.MaxValue) {
         paramIdx += 1
         s" AND seq_nr <= $$$paramIdx"
       } else "") +
      (if (criteria.minSequenceNr > 0L) {
         paramIdx += 1
         s" AND seq_nr >= $$$paramIdx"
       } else "") +
      (if (criteria.maxTimestamp != Long.MaxValue) {
         paramIdx += 1
         s" AND write_timestamp <= $$$paramIdx"
       } else "") +
      (if (criteria.minTimestamp != 0L) {
         paramIdx += 1
         s" AND write_timestamp >= $$$paramIdx"
       } else "")

    r2dbcExecutor.updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
      val statement = connection
        .createStatement(deleteSnapshots)
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
