/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SnapshotDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.LocalDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
private[r2dbc] object SqlServerSnapshotDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerSnapshotDao])
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class SqlServerSnapshotDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends SnapshotDao {
  import SnapshotDao._

  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)
  import helper._

  protected def log: Logger = SqlServerSnapshotDao.log

  protected val snapshotTable = settings.snapshotsTableWithSchema
  private implicit val snapshotPayloadCodec: PayloadCodec = settings.snapshotPayloadCodec
  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  protected def createUpsertSql: String = {
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
        tags = @tags,
        ser_manifest = @serManifest,
        meta_payload = @metaPayload,
        meta_ser_id = @metaSerId,
        meta_ser_manifest = @metaSerManifest
      where persistence_id = @persistenceId
      if @@ROWCOUNT = 0
        INSERT INTO $snapshotTable
          (slice, entity_type, persistence_id, seq_nr, write_timestamp, snapshot, ser_id, ser_manifest, meta_payload, meta_ser_id, meta_ser_manifest, tags)
          VALUES (@slice, @entityType, @persistenceId, @seqNr, @writeTimestamp, @snapshot, @serId, @serManifest, @metaPayload, @metaSerId, @metaSerManifest, @tags)
        """
  }

  private val upsertSql = createUpsertSql

  private def selectSql(criteria: SnapshotSelectionCriteria, pid: String): String = {
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

  private def deleteSql(criteria: SnapshotSelectionCriteria): String = {
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

    sql"""
      DELETE FROM $snapshotTable
      WHERE persistence_id = @persistenceId
      $maxSeqNrCondition $minSeqNrCondition $maxTimestampCondition $minTimestampCondition"""
  }

  protected def snapshotsBySlicesRangeSql(minSlice: Int, maxSlice: Int): String =
    sql"""
      SELECT TOP(@bufferSize) slice, persistence_id, seq_nr, db_timestamp, write_timestamp, snapshot, ser_id, ser_manifest, tags, meta_payload, meta_ser_id, meta_ser_manifest
      FROM $snapshotTable
      WHERE entity_type = @entityType
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= @fromTimestamp
      ORDER BY db_timestamp, seq_nr
      """

  private def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val subQuery =
      s"""
        select TOP(@limit) CAST(DATEDIFF(s,'1970-01-01 00:00:00',db_timestamp) AS BIGINT) / 10 AS bucket
             FROM $snapshotTable
             WHERE entity_type = @entityType
             AND ${sliceCondition(minSlice, maxSlice)}
             AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
        """.stripMargin
    sql"""
     SELECT bucket, count(*) as count from ($subQuery) as sub
     GROUP BY bucket ORDER BY bucket
     """
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  private def collectSerializedSnapshot(entityType: String, row: Row): SerializedSnapshotRow = {
    val writeTimestamp = row.get[java.lang.Long]("write_timestamp", classOf[java.lang.Long])
    val dbTimestamp =
      if (settings.querySettings.startFromSnapshotEnabled)
        fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])) match {
          case null => Instant.ofEpochMilli(writeTimestamp)
          case t    => t
        }
      else
        Instant.ofEpochMilli(writeTimestamp)
    val tags =
      if (settings.querySettings.startFromSnapshotEnabled)
        tagsFromDb(row)
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
          val sql = selectSql(criteria, persistenceId)
          val statement = connection
            .createStatement(sql)
            .bind("@persistenceId", persistenceId)

          if (criteria.maxSequenceNr != Long.MaxValue) statement.bind("@maxSeqNr", criteria.maxSequenceNr)
          if (criteria.minSequenceNr > 0L) statement.bind("@minSeqNr", criteria.minSequenceNr)
          if (criteria.maxTimestamp != Long.MaxValue) statement.bind("@maxTimestamp", criteria.maxTimestamp)
          if (criteria.minTimestamp > 0L) statement.bind("@minTimestamp", criteria.minTimestamp)

          statement
        },
        collectSerializedSnapshot(entityType, _))
      .map(_.headOption)(ExecutionContexts.parasitic)

  }

  def store(serializedRow: SerializedSnapshotRow): Future[Unit] = {
    r2dbcExecutor
      .updateOne(s"upsert snapshot [${serializedRow.persistenceId}], sequence number [${serializedRow.seqNr}]") {
        connection =>
          val statement =
            connection
              .createStatement(upsertSql)
              .bind("@slice", serializedRow.slice)
              .bind("@entityType", serializedRow.entityType)
              .bind("@persistenceId", serializedRow.persistenceId)
              .bind("@seqNr", serializedRow.seqNr)
              .bind("@writeTimestamp", serializedRow.writeTimestamp)
              .bindPayload("@snapshot", serializedRow.snapshot)
              .bind("@serId", serializedRow.serializerId)
              .bind("@serManifest", serializedRow.serializerManifest)
              .bind("@tags", tagsToDb(serializedRow.tags))

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
              .bind("@dbTimestamp", toDbTimestamp(serializedRow.dbTimestamp))
              .bind("@tags", tagsToDb(serializedRow.tags))
          }

          statement
      }
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    r2dbcExecutor.updateOne(s"delete snapshot [$persistenceId], criteria [$criteria]") { connection =>
      val statement = connection
        .createStatement(deleteSql(criteria))
        .bind("@persistenceId", persistenceId)

      if (criteria.maxSequenceNr != Long.MaxValue) {
        statement.bind("@maxSeqNr", criteria.maxSequenceNr)
      }
      if (criteria.minSequenceNr > 0L) {
        statement.bind("@minSeqNr", criteria.minSequenceNr)
      }
      if (criteria.maxTimestamp != Long.MaxValue) {
        statement.bind("@maxTimestamp", criteria.maxTimestamp)
      }
      if (criteria.minTimestamp > 0L) {
        statement.bind("@minTimestamp", criteria.minTimestamp)
      }
      statement
    }
  }.map(_ => ())(ExecutionContexts.parasitic)

  /**
   * This is used from `BySliceQuery`, i.e. only if settings.querySettings.startFromSnapshotEnabled
   */
  override def currentDbTimestamp(): Future[Instant] = Future.successful(nowInstant())

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
        val stmt = connection
          .createStatement(snapshotsBySlicesRangeSql(minSlice, maxSlice))
          .bind("@entityType", entityType)
          .bind("@fromTimestamp", toDbTimestamp(fromTimestamp))
          .bind("@bufferSize", settings.querySettings.bufferSize)
        stmt
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
      connection =>
        connection
          .createStatement(selectBucketsSql(entityType, minSlice, maxSlice))
          .bind("@entityType", entityType)
          .bind("@fromTimestamp", toDbTimestamp(fromTimestamp))
          .bind("@toTimestamp", toDbTimestamp(toTimestamp))
          .bind("@limit", limit),
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
