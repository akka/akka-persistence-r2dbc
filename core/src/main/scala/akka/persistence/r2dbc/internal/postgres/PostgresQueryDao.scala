/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import akka.persistence.r2dbc.internal.codec.TagsCodec
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object PostgresQueryDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[PostgresQueryDao])
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class PostgresQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends QueryDao {
  import PostgresJournalDao.readMetadata

  protected def log: Logger = PostgresQueryDao.log
  protected val journalTable: String = settings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec
  protected implicit val timestampCodec: TimestampCodec = settings.timestampCodec
  protected implicit val tagsCodec: TagsCodec = settings.tagsCodec
  implicit val queryAdapter: QueryAdapter = settings.queryAdapter

  protected def sqlFalse: String = "false"
  protected def sqlDbTimestamp = "CURRENT_TIMESTAMP"
  private val currentDbTimestampSql =
    "SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags, event_ser_id "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
      $selectColumns
      FROM $journalTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  protected def selectBucketsSql(minSlice: Int, maxSlice: Int): String = {
    sql"""
      SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
      FROM $journalTable
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? AND db_timestamp <= ?
      AND deleted = false
      GROUP BY bucket ORDER BY bucket LIMIT ?
      """
  }

  private val selectTimestampOfEventSql = sql"""
    SELECT db_timestamp FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""

  protected val selectOneEventSql = sql"""
    SELECT slice, entity_type, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""

  private val selectOneEventWithoutPayloadSql = sql"""
    SELECT slice, entity_type, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, event_ser_id, event_ser_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""

  protected val selectEventsSql = sql"""
    SELECT slice, entity_type, persistence_id, seq_nr, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
    AND deleted = false
    ORDER BY seq_nr
    LIMIT ?"""

  protected def bindSelectEventsSql(
      stmt: Statement,
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      bufferSize: Int): Statement = stmt
    .bind(0, persistenceId)
    .bind(1, fromSequenceNr)
    .bind(2, toSequenceNr)
    .bind(3, settings.querySettings.bufferSize)

  protected val allPersistenceIdsSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable ORDER BY persistence_id LIMIT ?"

  protected val persistenceIdsForEntityTypeSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"

  protected val allPersistenceIdsAfterSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

  protected val persistenceIdsForEntityTypeAfterSql =
    sql"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"

  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.getTimestamp("db_timestamp"))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  protected def bindEventsBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant]): Statement = {
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
    toTimestamp match {
      case Some(until) =>
        stmt.bindTimestamp(2, until)
        stmt.bind(3, settings.querySettings.bufferSize)
      case None =>
        stmt.bind(2, settings.querySettings.bufferSize)
    }
  }

  override def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedJournalRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(
            eventsBySlicesRangeSql(
              toDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
        bindEventsBySlicesRangeSql(stmt, entityType, fromTimestamp, toTimestamp)
      },
      row =>
        if (backtracking)
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = None, // lazy loaded for backtracking
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = "",
            writerUuid = "", // not need in this query
            tags = row.getTags("tags"),
            metadata = None)
        else
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = Some(row.getPayload("event_payload")),
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = "", // not need in this query
            tags = row.getTags("tags"),
            metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

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
        val stmt = connection.createStatement(selectBucketsSql(minSlice, maxSlice))
        bindSelectBucketsSql(stmt, entityType, fromTimestamp, toTimestamp, limit)
      },
      row => {
        val bucketStartEpochSeconds = row.get[java.lang.Long]("bucket", classOf[java.lang.Long]).toLong * 10
        val count = row.get[java.lang.Long]("count", classOf[java.lang.Long]).toLong
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result
  }

  /**
   * Events are append only
   */
  override def countBucketsMayChange: Boolean = false

  override def timestampOfEvent(persistenceId: String, seqNr: Long): Future[Option[Instant]] = {
    r2dbcExecutor.selectOne("select timestampOfEvent")(
      connection =>
        connection
          .createStatement(selectTimestampOfEventSql)
          .bind(0, persistenceId)
          .bind(1, seqNr),
      row => row.getTimestamp("db_timestamp"))
  }

  override def loadEvent(
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[SerializedJournalRow]] =
    r2dbcExecutor.selectOne("select one event")(
      connection => {
        val selectSql = if (includePayload) selectOneEventSql else selectOneEventWithoutPayloadSql
        connection
          .createStatement(selectSql)
          .bind(0, persistenceId)
          .bind(1, seqNr)
      },
      row => {
        val payload =
          if (includePayload)
            Some(row.getPayload("event_payload"))
          else None
        SerializedJournalRow(
          slice = row.get[Integer]("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId,
          seqNr,
          dbTimestamp = row.getTimestamp("db_timestamp"),
          readDbTimestamp = row.getTimestamp("read_db_timestamp"),
          payload,
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = "", // not need in this query
          tags = row.getTags("tags"),
          metadata = readMetadata(row))
      })

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {

    val result = r2dbcExecutor.select(s"select eventsByPersistenceId [$persistenceId]")(
      connection => {
        val stmt = connection.createStatement(selectEventsSql)
        bindSelectEventsSql(stmt, persistenceId, fromSequenceNr, toSequenceNr, settings.querySettings.bufferSize)
      },
      row =>
        SerializedJournalRow(
          slice = row.get[Integer]("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId = row.get("persistence_id", classOf[String]),
          seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
          dbTimestamp = row.getTimestamp("db_timestamp"),
          readDbTimestamp = row.getTimestamp("read_db_timestamp"),
          payload = Some(row.getPayload("event_payload")),
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = row.get("writer", classOf[String]),
          tags = row.getTags("tags"),
          metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events for persistenceId [{}]", rows.size, persistenceId))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  protected def bindPersistenceIdsForEntityTypeAfterSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      after: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityType + likeStmtPostfix)
      .bind(1, after)
      .bind(2, limit)
  }

  protected def bindPersistenceIdsForEntityTypeSql(
      stmt: Statement,
      entityType: String,
      likeStmtPostfix: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityType + likeStmtPostfix)
      .bind(1, limit)
  }

  override def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(persistenceIdsForEntityTypeAfterSql)
            bindPersistenceIdsForEntityTypeAfterSql(stmt, entityType, likeStmtPostfix, after, limit)

          case None =>
            val stmt = connection.createStatement(persistenceIdsForEntityTypeSql)
            bindPersistenceIdsForEntityTypeSql(stmt, entityType, likeStmtPostfix, limit)

        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  protected def bindAllPersistenceIdsAfterSql(stmt: Statement, after: String, limit: Long): Statement = {
    stmt
      .bind(0, after)
      .bind(1, limit)
  }

  override def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection.createStatement(allPersistenceIdsAfterSql)
            bindAllPersistenceIdsAfterSql(stmt, after, limit)

          case None =>
            connection
              .createStatement(allPersistenceIdsSql)
              .bind(0, limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

}
