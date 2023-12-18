/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SerializedEventMetadata
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
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerQueryDao {
  private val log: Logger = LoggerFactory.getLogger(classOf[SqlServerQueryDao])
  def setFromDb[T](array: Array[T]): Set[T] = array match {
    case null    => Set.empty[T]
    case entries => entries.toSet
  }

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
 */
@InternalApi
private[r2dbc] class SqlServerQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends QueryDao {
  import SqlServerQueryDao.readMetadata

  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)
  import helper._

  private val FALSE = "0"

  protected def log: Logger = SqlServerQueryDao.log
  protected val journalTable = settings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec

  protected def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= @until" else ""

    def localNow = toDbTimestamp(nowInstant())

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < DATEADD(ms, -${behindCurrentTime.toMillis}, CAST('$localNow' as datetime2(6)))"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id "
      else
        "SELECT TOP(@limit) slice, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, tags, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    sql"""
      $selectColumns
      FROM $journalTable
      WHERE entity_type = @entityType
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= @from $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = $FALSE
      ORDER BY db_timestamp, seq_nr"""
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  private def selectBucketsSql(minSlice: Int, maxSlice: Int): String = {
    sql"""
      SELECT TOP(@limit) bucket, count(*) as count from
       (select DATEDIFF(s,'1970-01-01 00:00:00', db_timestamp)/10 as bucket
        FROM $journalTable
        WHERE entity_type = @entityType
        AND ${sliceCondition(minSlice, maxSlice)}
        AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
        AND deleted = $FALSE) as sub
      GROUP BY bucket ORDER BY bucket
      """
  }

  private val selectTimestampOfEventSql = sql"""
    SELECT db_timestamp FROM $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr = @seqNr AND deleted = $FALSE"""

  protected val selectOneEventSql = sql"""
    SELECT slice, entity_type, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr = @seqNr AND deleted = $FALSE"""

  protected val selectOneEventWithoutPayloadSql = sql"""
    SELECT slice, entity_type, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    FROM $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr = @seqNr AND deleted = $FALSE"""

  protected val selectEventsSql = sql"""
    SELECT TOP(@limit) slice, entity_type, persistence_id, seq_nr, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
    from $journalTable
    WHERE persistence_id = @persistenceId AND seq_nr >= @from AND seq_nr <= @to
    AND deleted = $FALSE
    ORDER BY seq_nr"""

  private val allPersistenceIdsSql =
    sql"SELECT DISTINCT TOP(@limit) persistence_id from $journalTable ORDER BY persistence_id"

  private val persistenceIdsForEntityTypeSql =
    sql"SELECT DISTINCT TOP(@limit) persistence_id from $journalTable WHERE persistence_id LIKE @entityTypeLike ORDER BY persistence_id"

  private val allPersistenceIdsAfterSql =
    sql"SELECT DISTINCT TOP(@limit) persistence_id from $journalTable WHERE persistence_id > @after ORDER BY persistence_id"

  private val persistenceIdsForEntityTypeAfterSql =
    sql"SELECT DISTINCT TOP(@limit) persistence_id from $journalTable WHERE persistence_id LIKE @entityTypeLike AND persistence_id > @after ORDER BY persistence_id"

  protected val r2dbcExecutor = new R2dbcExecutor(
    connectionFactory,
    log,
    settings.logDbCallsExceeding,
    settings.connectionFactorySettings.poolSettings.closeCallsExceeding)(ec, system)

  override def currentDbTimestamp(): Future[Instant] = Future.successful(nowInstant())

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
          .bind("@entityType", entityType)
          .bind("@from", toDbTimestamp(fromTimestamp))
        toTimestamp.foreach(t => stmt.bind("@until", toDbTimestamp(t)))
        stmt.bind("@limit", settings.querySettings.bufferSize)
      },
      row =>
        if (backtracking)
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
            readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
            payload = None, // lazy loaded for backtracking
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = "",
            writerUuid = "", // not need in this query
            tags = tagsFromDb(row),
            metadata = None)
        else
          SerializedJournalRow(
            slice = row.get[Integer]("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
            readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
            payload = Some(row.getPayload("event_payload")),
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = "", // not need in this query
            tags = tagsFromDb(row),
            metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debugN("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
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
      connection =>
        connection
          .createStatement(selectBucketsSql(minSlice, maxSlice))
          .bind("@entityType", entityType)
          .bind("@fromTimestamp", toDbTimestamp(fromTimestamp))
          .bind("@toTimestamp", toDbTimestamp(toTimestamp))
          .bind("@limit", limit),
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
          .bind("@persistenceId", persistenceId)
          .bind("@seqNr", seqNr),
      row => fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])))
  }

  override def loadEvent(
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean): Future[Option[SerializedJournalRow]] =
    r2dbcExecutor.selectOne(s"select one event ($persistenceId, $seqNr, $includePayload)")(
      connection => {
        val selectSql = if (includePayload) selectOneEventSql else selectOneEventWithoutPayloadSql
        connection
          .createStatement(selectSql)
          .bind("@persistenceId", persistenceId)
          .bind("@seqNr", seqNr)
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
          dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
          readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
          payload,
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = "", // not need in this query
          tags = tagsFromDb(row),
          metadata = readMetadata(row))
      })

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {

    val result = r2dbcExecutor.select(s"select eventsByPersistenceId [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectEventsSql)
          .bind("@persistenceId", persistenceId)
          .bind("@from", fromSequenceNr)
          .bind("@to", toSequenceNr)
          .bind("@limit", settings.querySettings.bufferSize),
      row =>
        SerializedJournalRow(
          slice = row.get[Integer]("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId = row.get("persistence_id", classOf[String]),
          seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
          dbTimestamp = fromDbTimestamp(row.get("db_timestamp", classOf[LocalDateTime])),
          readDbTimestamp = fromDbTimestamp(row.get("read_db_timestamp", classOf[LocalDateTime])),
          payload = Some(row.getPayload("event_payload")),
          serId = row.get[Integer]("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = row.get("writer", classOf[String]),
          tags = tagsFromDb(row),
          metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events for persistenceId [{}]", rows.size, persistenceId))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"

    val result = r2dbcExecutor.select(s"select persistenceIds by entity type")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(persistenceIdsForEntityTypeAfterSql)
              .bind("@entityTypeLike", entityType + likeStmtPostfix)
              .bind("@after", after)
              .bind("@limit", limit)
          case None =>
            connection
              .createStatement(persistenceIdsForEntityTypeSql)
              .bind("@entityTypeLike", entityType + likeStmtPostfix)
              .bind("@limit", limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(allPersistenceIdsAfterSql)
              .bind("@after", after)
              .bind("@limit", limit)
          case None =>
            connection
              .createStatement(allPersistenceIdsSql)
              .bind("@limit", limit)
        },
      row => row.get("persistence_id", classOf[String]))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

}
