/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.TagsCodec
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichStatement
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source

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
private[r2dbc] class PostgresSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends SnapshotDao {
  import SnapshotDao._

  protected def log: Logger = PostgresSnapshotDao.log

  protected val snapshotTable: String = settings.snapshotsTableWithSchema

  protected implicit val snapshotPayloadCodec: PayloadCodec = settings.snapshotPayloadCodec
  protected implicit val timestampCodec: TimestampCodec = settings.timestampCodec
  protected implicit val tagsCodec: TagsCodec = settings.tagsCodec
  protected implicit val queryAdapter: QueryAdapter = settings.queryAdapter

  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  protected def createUpsertSql: String = {
    // db_timestamp and tags columns were added in 1.2.0
    if (settings.querySettings.startFromSnapshotEnabled)
      sql"""
        INSERT INTO $snapshotTable
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
      INSERT INTO $snapshotTable
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

  private val upsertSql = createUpsertSql

  protected def selectSql(criteria: SnapshotSelectionCriteria): String = {
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
        FROM $snapshotTable
        WHERE persistence_id = ?
        $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition
        LIMIT 1"""
    else
      sql"""
      SELECT slice, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE persistence_id = ?
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
      WHERE persistence_id = ?
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition"""
  }

  private val currentDbTimestampSql =
    sql"SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def snapshotsBySlicesRangeSql(minSlice: Int, maxSlice: Int): String = {

    sql"""
      SELECT slice, persistence_id, seq_nr, db_timestamp, write_timestamp, snapshot, ser_id, ser_manifest, tags, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ?
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  protected def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    sql"""
     SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
     FROM $snapshotTable
     WHERE entity_type = ?
     AND ${sliceCondition(minSlice, maxSlice)}
     AND db_timestamp >= ? AND db_timestamp <= ?
     GROUP BY bucket ORDER BY bucket LIMIT ?
     """
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

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
    r2dbcExecutor
      .select(s"select snapshot [$persistenceId], criteria: [$criteria]")(
        { connection =>
          val statement = connection
            .createStatement(selectSql(criteria))
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
      .map(_.headOption)(ExecutionContexts.parasitic)
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
    r2dbcExecutor
      .updateOne(s"upsert snapshot [${serializedRow.persistenceId}], sequence number [${serializedRow.seqNr}]") {
        connection =>
          val statement =
            connection
              .createStatement(upsertSql)

          bindUpsertSql(statement, serializedRow)

      }
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    r2dbcExecutor.updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
      val statement = connection
        .createStatement(deleteSql(criteria))
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
  }.map(_ => ())(ExecutionContexts.parasitic)

  /**
   * This is used from `BySliceQuery`, i.e. only if settings.querySettings.startFromSnapshotEnabled
   */
  override def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.getTimestamp("db_timestamp"))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  protected def bindSnapshotsBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      bufferSize: Int): Statement = {
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
      .bind(2, settings.querySettings.bufferSize)
  }

  /**
   * This is used from `BySliceQuery`, i.e. only if settings.querySettings.startFromSnapshotEnabled
   */
  override def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedSnapshotRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select snapshotsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection.createStatement(snapshotsBySlicesRangeSql(minSlice, maxSlice))
        bindSnapshotsBySlicesRangeSql(stmt, entityType, fromTimestamp, settings.querySettings.bufferSize)
      },
      collectSerializedSnapshot(entityType, _))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] snapshots from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  /**
   * Counts for a bucket may become inaccurate when existing snapshots are updated since the timestamp is changed. This
   * is used from `BySliceQuery`, i.e. only if settings.querySettings.startFromSnapshotEnabled
   */
  override def countBucketsMayChange: Boolean = true

  protected def bindSelectBucketsSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement = {
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
      .bindTimestamp(2, toTimestamp)
      .bind(3, limit)
  }

  /**
   * This is used from `BySliceQuery`, i.e. only if settings.querySettings.startFromSnapshotEnabled
   */
  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val now = InstantFactory.now() // not important to use database time
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection.createStatement(selectBucketsSql(entityType, minSlice, maxSlice))
        bindSelectBucketsSql(stmt, entityType, fromTimestamp, toTimestamp, limit)
      },
      row => {
        val bucketStartEpochSeconds = row.get("bucket", classOf[java.lang.Long]).toLong * 10
        val count = row.get[java.lang.Long]("count", classOf[java.lang.Long]).toLong
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result

  }

}
