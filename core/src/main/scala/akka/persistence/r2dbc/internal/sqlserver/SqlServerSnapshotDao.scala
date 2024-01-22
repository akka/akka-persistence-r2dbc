/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotMetadata
import akka.persistence.r2dbc.internal.SnapshotDao.SerializedSnapshotRow
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresSnapshotDao
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerSnapshotDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerSnapshotDao])
}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresSnapshotDao(settings, connectionFactory) {

  override def log: Logger = SqlServerSnapshotDao.log

  override def selectSql(criteria: SnapshotSelectionCriteria): String = {
    val maxSeqNrCondition =
      if (criteria.maxSequenceNr != Long.MaxValue) " AND seq_nr <= @maxSeqNr"
      else ""

    val minSeqNrCondition =
      if (criteria.minSequenceNr > 0L) " AND seq_nr >= @minSeqNr"
      else ""

    val maxTimestampCondition =
      if (criteria.maxTimestamp != Long.MaxValue) " AND write_timestamp <= @maxTimestamp"
      else ""

    val minTimestampCondition =
      if (criteria.minTimestamp != 0L) " AND write_timestamp >= @minTimestamp"
      else ""

    if (settings.querySettings.startFromSnapshotEnabled)
      sql"""
        SELECT TOP(1) slice, persistence_id, seq_nr, db_timestamp, write_timestamp, snapshot, ser_id, ser_manifest, tags, meta_payload, meta_ser_id, meta_ser_manifest
        FROM $snapshotTable
        WHERE persistence_id = @persistenceId
        $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
        """
    else
      sql"""
      SELECT TOP (1) slice, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE persistence_id = @persistenceId
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
      """
  }

  override protected def createUpsertSql: String = {
    if (settings.querySettings.startFromSnapshotEnabled)
      sql"""
        UPDATE $snapshotTable SET
          seq_nr = @seqNr,
          db_timestamp = @dbTimestamp,
          write_timestamp = @writeTimestamp,
          snapshot = @snapshot,
          ser_id = @serId,
          tags = @tags,
          ser_manifest = @serManifest,
          meta_payload = @metaPayload,
          meta_ser_id = @metaSerId,
          meta_ser_manifest = @metaSerManifest
        where persistence_id = @persistenceId
        if @@ROWCOUNT = 0
          INSERT INTO $snapshotTable
             (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, db_timestamp, tags)
             VALUES (@slice, @entityType, @persistenceId, @seqNr, @writeTimestamp, @snapshot, @serId, @serManifest, @metaPayload, @metaSerId, @metaSerManifest, @dbTimestamp, @tags)
          """
    else
      sql"""
      UPDATE $snapshotTable SET
        seq_nr = @seqNr,
        write_timestamp = @writeTimestamp,
        snapshot = @snapshot,
        ser_id = @serId,
        ser_manifest = @serManifest,
        meta_payload = @metaPayload,
        meta_ser_id = @metaSerId,
        meta_ser_manifest = @metaSerManifest,
        tags = @tags
      where persistence_id = @persistenceId
      if @@ROWCOUNT = 0
        INSERT INTO $snapshotTable
          (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, tags)
          VALUES (@slice, @entityType, @persistenceId, @seqNr, @writeTimestamp, @snapshot, @serId, @serManifest, @metaPayload, @metaSerId, @metaSerManifest, @tags)
        """
  }

  override protected def bindUpsertSql(statement: Statement, serializedRow: SerializedSnapshotRow): Statement = {
    statement
      .bind("@slice", serializedRow.slice)
      .bind("@entityType", serializedRow.entityType)
      .bind("@persistenceId", serializedRow.persistenceId)
      .bind("@seqNr", serializedRow.seqNr)
      .bind("@writeTimestamp", serializedRow.writeTimestamp)
      .bindPayload("@snapshot", serializedRow.snapshot)
      .bind("@serId", serializedRow.serializerId)
      .bind("@serManifest", serializedRow.serializerManifest)
      .bindTags("@tags", serializedRow.tags)

    serializedRow.metadata match {
      case Some(SerializedSnapshotMetadata(serializedMeta, serializerId, serializerManifest)) =>
        statement
          .bind("@metaPayload", serializedMeta)
          .bind("@metaSerId", serializerId)
          .bind("@metaSerManifest", serializerManifest)
      case None =>
        statement
          .bindNull("@metaPayload", classOf[Array[Byte]])
          .bindNull("@metaSerId", classOf[Integer])
          .bindNull("@metaSerManifest", classOf[String])
    }

    if (settings.querySettings.startFromSnapshotEnabled) {
      statement
        .bindTimestamp("@dbTimestamp", serializedRow.dbTimestamp)
        .bindTags("@tags", serializedRow.tags)
    }

    statement
  }

  override protected def bindSelectBucketsSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement = {
    stmt
      .bind("@limit", limit)
      .bind("@entityType", entityType)
      .bindTimestamp("@fromTimestamp", fromTimestamp)
      .bindTimestamp("@toTimestamp", toTimestamp)
  }

  override protected def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    // group by column alias (bucket) needs a sub query
    val subQuery =
      s"""
          select TOP(@limit) CAST(DATEDIFF(s,'1970-01-01 00:00:00',db_timestamp) AS BIGINT) / 10 AS bucket
          FROM $stateTable
          WHERE entity_type = @entityType
          AND ${sliceCondition(minSlice, maxSlice)}
          AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
         """
    sql"""
     SELECT bucket,  count(*) as count from ($subQuery) as sub
     GROUP BY bucket ORDER BY bucket
     """
  }

  override protected def bindSnapshotsBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      bufferSize: Int): Statement = {
    stmt
      .bind("@bufferSize", settings.querySettings.bufferSize)
      .bind("@entityType", entityType)
      .bindTimestamp("@fromTimestamp", fromTimestamp)
  }

  override protected def snapshotsBySlicesRangeSql(minSlice: Int, maxSlice: Int): String =
    sql"""
      SELECT TOP(@bufferSize) slice, persistence_id, seq_nr, db_timestamp, write_timestamp, snapshot, ser_id, ser_manifest, tags, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE entity_type = @entityType
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= @fromTimestamp
      ORDER BY db_timestamp, seq_nr
      """

  override def currentDbTimestamp(): Future[Instant] = Future.successful(timestampCodec.instantNow())

}
