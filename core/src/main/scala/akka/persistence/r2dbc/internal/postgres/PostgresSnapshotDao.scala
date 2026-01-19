/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
private[r2dbc] object PostgresSnapshotDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresSnapshotDao])
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class PostgresSnapshotDao(executorProvider: R2dbcExecutorProvider) extends SnapshotDao {
  protected val settings: R2dbcSettings = executorProvider.settings
  protected val system: ActorSystem[_] = executorProvider.system
  implicit protected val ec: ExecutionContext = executorProvider.ec
  import settings.codecSettings.SnapshotImplicits._

  import SnapshotDao._

  protected def log: Logger = PostgresSnapshotDao.log

  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  protected val persistenceExt: Persistence = Persistence(system)

  protected def snapshotTable(slice: Int): String = settings.snapshotTableWithSchema(slice)

  protected def upsertSql(slice: Int): String = {
    sqlCache.get(slice, "upsertSql") {
      // db_timestamp and tags columns were added in 1.2.0
      if (settings.querySettings.startFromSnapshotEnabled)
        sql"""
        INSERT INTO ${snapshotTable(slice)}
        (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, db_timestamp, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (persistence_id)
        DO UPDATE SET
          seq_nr = excluded.seq_nr,
          db_timestamp = excluded.db_timestamp,
          write_timestamp = excluded.write_timestamp,
          snapshot = excluded.snapshot,
          ser_id = excluded.ser_id,
          tags = excluded.tags,
          ser_manifest = excluded.ser_manifest,
          meta_payload = excluded.meta_payload,
          meta_ser_id = excluded.meta_ser_id,
          meta_ser_manifest = excluded.meta_ser_manifest"""
      else
        sql"""
        INSERT INTO ${snapshotTable(slice)}
        (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (persistence_id)
        DO UPDATE SET
          seq_nr = excluded.seq_nr,
          write_timestamp = excluded.write_timestamp,
          snapshot = excluded.snapshot,
          ser_id = excluded.ser_id,
          ser_manifest = excluded.ser_manifest,
          meta_payload = excluded.meta_payload,
          meta_ser_id = excluded.meta_ser_id,
          meta_ser_manifest = excluded.meta_ser_manifest"""
    }
  }

  protected def selectSql(slice: Int, criteria: SnapshotSelectionCriteria): String = {
    def createSql = {
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

      // db_timestamp and tags columns were added in 1.2.0
      if (settings.querySettings.startFromSnapshotEnabled)
        sql"""
        SELECT slice, persistence_id, seq_nr, db_timestamp, write_timestamp, snapshot, ser_id, ser_manifest, tags, meta_payload, meta_ser_id, meta_ser_manifest
        FROM ${snapshotTable(slice)}
        WHERE persistence_id = ?
        $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
        LIMIT 1"""
      else
        sql"""
        SELECT slice, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest
        FROM ${snapshotTable(slice)}
        WHERE persistence_id = ?
        $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
        LIMIT 1"""
    }

    if (criteria == SnapshotSelectionCriteria.Latest)
      sqlCache.get(slice, "selectSql-latest")(createSql) // normal case
    else
      createSql // no cache
  }

  private def deleteSql(slice: Int, criteria: SnapshotSelectionCriteria): String = {
    // not caching, too many combinations

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
      DELETE FROM ${snapshotTable(slice)}
      WHERE persistence_id = ?
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition"""
  }

  private def collectSerializedSnapshot(entityType: String, row: Row): SerializedSnapshotRow = {
    val writeTimestamp = row.get[java.lang.Long]("write_timestamp", classOf[java.lang.Long])

    // db_timestamp and tags columns were added in 1.2.0
    val dbTimestamp =
      if (settings.querySettings.startFromSnapshotEnabled)
        row.getTimestamp("db_timestamp") match {
          case null => Instant.ofEpochMilli(writeTimestamp)
          case t    => t
        }
      else
        Instant.ofEpochMilli(writeTimestamp)
    val tags =
      if (settings.querySettings.startFromSnapshotEnabled)
        row.getTags("tags")
      else
        Set.empty[String]

    SerializedSnapshotRow(
      slice = row.get[Integer]("slice", classOf[Integer]),
      entityType,
      persistenceId = row.get("persistence_id", classOf[String]),
      seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
      dbTimestamp,
      writeTimestamp,
      snapshot = row.getPayload("snapshot"),
      serializerId = row.get[Integer]("ser_id", classOf[Integer]),
      serializerManifest = row.get("ser_manifest", classOf[String]),
      tags,
      metadata = {
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

  override def load(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor
      .select(s"select snapshot [$persistenceId], criteria: [$criteria]")(
        { connection =>
          val statement = connection
            .createStatement(selectSql(slice, criteria))
            .bind(0, persistenceId)

          var bindIdx = 0
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
        collectSerializedSnapshot(entityType, _))
      .map(_.headOption)(ExecutionContext.parasitic)
  }

  protected def bindUpsertSql(statement: Statement, serializedRow: SerializedSnapshotRow): Statement = {
    statement
      .bind(0, serializedRow.slice)
      .bind(1, serializedRow.entityType)
      .bind(2, serializedRow.persistenceId)
      .bind(3, serializedRow.seqNr)
      .bind(4, serializedRow.writeTimestamp)
      .bindPayload(5, serializedRow.snapshot)
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

    // db_timestamp and tags columns were added in 1.2.0
    if (settings.querySettings.startFromSnapshotEnabled) {
      statement
        .bindTimestamp(11, serializedRow.dbTimestamp)
        .bindTags(12, serializedRow.tags)
    }
    statement
  }

  def store(serializedRow: SerializedSnapshotRow): Future[Unit] = {
    val slice = persistenceExt.sliceForPersistenceId(serializedRow.persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor
      .updateOne(s"upsert snapshot [${serializedRow.persistenceId}], sequence number [${serializedRow.seqNr}]") {
        connection =>
          val statement =
            connection
              .createStatement(upsertSql(slice))

          bindUpsertSql(statement, serializedRow)

      }
      .map(_ => ())(ExecutionContext.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor.updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
      val statement = connection
        .createStatement(deleteSql(slice, criteria))
        .bind(0, persistenceId)

      var bindIdx = 0
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
  }.map(_ => ())(ExecutionContext.parasitic)

}
