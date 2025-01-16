/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.postgres

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.BucketDurationSeconds
import akka.persistence.r2dbc.internal.CorrelationId
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.JournalDao.SerializedJournalRow
import akka.persistence.r2dbc.internal.QueryDao
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.codec.TagsCodec.TagsCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichRow
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source

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
private[r2dbc] class PostgresQueryDao(executorProvider: R2dbcExecutorProvider) extends QueryDao {
  protected val settings: R2dbcSettings = executorProvider.settings
  protected val system: ActorSystem[_] = executorProvider.system
  implicit protected val ec: ExecutionContext = executorProvider.ec
  import settings.codecSettings.JournalImplicits._

  import PostgresJournalDao.readMetadata

  protected def log: Logger = PostgresQueryDao.log
  protected val persistenceExt: Persistence = Persistence(system)
  private val sqlCache = Sql.Cache(settings.numberOfDataPartitions > 1)

  protected def journalTable(slice: Int): String = settings.journalTableWithSchema(slice)

  protected def sqlFalse: String = "false"
  protected def sqlDbTimestamp = "CURRENT_TIMESTAMP"
  private val currentDbTimestampSql =
    "SELECT CURRENT_TIMESTAMP AS db_timestamp"

  protected def eventsBySlicesRangeSql(
      fromSeqNrParam: Boolean,
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    // not caching, too many combinations

    // If more events than the buffer size are all on the same timestamp, then the query will get stuck on that same
    // timestamp. Avoid this by also starting from the highest seen sequence number for that timestamp, using the fact
    // that events are ordered by db_timestamp, seq_nr. Note that sequence numbers are per persistence id, so a later
    // timestamp can have an earlier sequence number. Add a logical conditional only when db_timestamp = fromTimestamp
    // to also filter for seq_nr >= fromSeqNr. Expressed in a logically equivalent form, where A -> B === ~A v B.
    def fromSeqNrParamCondition =
      if (fromSeqNrParam) "AND (db_timestamp != ? OR seq_nr >= ?)" else ""

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

    val sliceFilter = {
      if (settings.querySettings.explicitSliceRangeCondition) sliceCondition(minSlice, maxSlice)
      else s"slice BETWEEN $minSlice AND $maxSlice"
    }

    sql"""
      $selectColumns
      FROM ${journalTable(minSlice)}
      WHERE entity_type = ?
      AND $sliceFilter
      AND db_timestamp >= ? $fromSeqNrParamCondition $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"

  protected def selectBucketsSql(minSlice: Int, maxSlice: Int): String =
    sqlCache.get(minSlice, s"selectBucketsSql-$minSlice-$maxSlice") {
      sql"""
      SELECT floor(extract(EPOCH from db_timestamp) / 10)::BIGINT AS bucket, count(*) AS count
      FROM ${journalTable(minSlice)}
      WHERE entity_type = ?
      AND ${sliceCondition(minSlice, maxSlice)}
      AND db_timestamp >= ? AND db_timestamp <= ?
      AND deleted = false
      GROUP BY bucket ORDER BY bucket LIMIT ?
      """
    }

  protected def selectTimestampOfEventSql(slice: Int): String =
    sqlCache.get(slice, "selectTimestampOfEventSql") {
      sql"""
      SELECT db_timestamp FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""
    }

  protected def selectLatestEventTimestampSql(slice: Int): String =
    sqlCache.get(slice, "selectLatestEventTimestampSql") {
      sql"""
      SELECT MAX(per_slice.latest_timestamp) AS latest_timestamp
      FROM (
        SELECT
          (SELECT MAX(db_timestamp)
           FROM ${journalTable(slice)}
           WHERE entity_type = ?
           AND slice = slice_range.slice
           AND deleted = $sqlFalse) AS latest_timestamp
        FROM (SELECT * FROM generate_series(?, ?)) AS slice_range(slice)
      ) per_slice
      WHERE per_slice.latest_timestamp IS NOT NULL;
      """
    }

  protected def selectOneEventSql(slice: Int): String =
    sqlCache.get(slice, "selectOneEventSql") {
      sql"""
      SELECT entity_type, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload, tags
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""
    }

  protected def selectOneEventWithoutPayloadSql(slice: Int): String =
    sqlCache.get(slice, "selectOneEventWithoutPayloadSql") {
      sql"""
      SELECT entity_type, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr = ? AND deleted = $sqlFalse"""
    }

  protected def selectLastEventSql(slice: Int): String =
    sqlCache.get(slice, "selectLastEventSql") {
      sql"""
      SELECT entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr <= ? AND deleted = $sqlFalse
      ORDER BY seq_nr DESC
      LIMIT 1"""
    }

  protected def selectLastEventIncludeDeletedSql(slice: Int): String =
    sqlCache.get(slice, "selectLastEventIncludeDeletedSql") {
      sql"""
      SELECT entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags, deleted
      FROM ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr <= ?
      ORDER BY seq_nr DESC
      LIMIT 1
      """
    }

  protected def selectEventsSql(slice: Int): String =
    sqlCache.get(slice, "selectEventsSql") {
      sql"""
      SELECT entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags
      from ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
      AND deleted = false
      ORDER BY seq_nr
      LIMIT ?"""
    }

  protected def selectEventsIncludeDeletedSql(slice: Int): String =
    sqlCache.get(slice, "selectEventsIncludeDeletedSql") {
      sql"""
      SELECT entity_type, seq_nr, db_timestamp, $sqlDbTimestamp AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload, tags, deleted
      from ${journalTable(slice)}
      WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
      ORDER BY seq_nr
      LIMIT ?"""
    }

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

  protected def allPersistenceIdsSql(minSlice: Int): String =
    sqlCache.get(minSlice, "allPersistenceIdsSql") {
      sql"SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} ORDER BY persistence_id LIMIT ?"
    }

  protected def persistenceIdsForEntityTypeSql(minSlice: Int): String =
    sqlCache.get(minSlice, "persistenceIdsForEntityTypeSql") {
      sql"SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"
    }

  protected def allPersistenceIdsAfterSql(minSlice: Int): String =
    sqlCache.get(minSlice, "allPersistenceIdsAfterSql") {
      sql"SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"
    }

  protected def persistenceIdsForEntityTypeAfterSql(minSlice: Int): String =
    sqlCache.get(minSlice, "persistenceIdsForEntityTypeAfterSql") {
      sql"SELECT DISTINCT(persistence_id) from ${journalTable(minSlice)} WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"
    }

  override def currentDbTimestamp(slice: Int): Future[Instant] = {
    val executor = executorProvider.executorFor(slice)
    executor
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
      fromSeqNr: Option[Long],
      toTimestamp: Option[Instant]): Statement = {
    stmt
      .bind(0, entityType)
      .bindTimestamp(1, fromTimestamp)
    val index1 = 2
    val index2 = fromSeqNr match {
      case Some(seqNr) =>
        stmt.bindTimestamp(index1, fromTimestamp)
        stmt.bind(index1 + 1, seqNr)
        index1 + 2
      case None => index1
    }
    toTimestamp match {
      case Some(until) =>
        stmt.bindTimestamp(index2, until)
        stmt.bind(index2 + 1, settings.querySettings.bufferSize)
      case None =>
        stmt.bind(index2, settings.querySettings.bufferSize)
    }
  }

  override def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      fromSeqNr: Option[Long],
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      correlationId: Option[String]): Source[SerializedJournalRow, NotUsed] = {

    if (!settings.isSliceRangeWithinSameDataPartition(minSlice, maxSlice))
      throw new IllegalArgumentException(
        s"Slice range [$minSlice-$maxSlice] spans over more than one " +
        s"of the [${settings.numberOfDataPartitions}] data partitions.")
    val executor = executorProvider.executorFor(minSlice)
    val result = executor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .createStatement(
            eventsBySlicesRangeSql(
              fromSeqNrParam = fromSeqNr.isDefined,
              toDbTimestampParam = toTimestamp.isDefined,
              behindCurrentTime,
              backtracking,
              minSlice,
              maxSlice))
        bindEventsBySlicesRangeSql(stmt, entityType, fromTimestamp, fromSeqNr, toTimestamp)
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

    if (log.isDebugEnabled) {
      result.foreach(rows =>
        log.debug(
          "Read [{}] events from slices [{} - {}]{}",
          rows.size,
          minSlice,
          maxSlice,
          CorrelationId.toLogText(correlationId)))
    }

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
      limit: Int,
      correlationId: Option[String]): Future[Seq[Bucket]] = {
    val executor = executorProvider.executorFor(minSlice)

    val now = InstantFactory.now() // not important to use database time
    val toTimestamp = {
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = executor.select(s"select bucket counts [$minSlice - $maxSlice]")(
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
      result.foreach(rows =>
        log.debug(
          "Read [{}] bucket counts from slices [{} - {}]{}",
          rows.size,
          minSlice,
          maxSlice,
          CorrelationId.toLogText(correlationId)))

    if (toTimestamp == now)
      result
    else
      result.map(appendEmptyBucketIfLastIsMissing(_, toTimestamp))
  }

  /**
   * Events are append only
   */
  override def countBucketsMayChange: Boolean = false

  override def timestampOfEvent(
      persistenceId: String,
      seqNr: Long,
      correlationId: Option[String]): Future[Option[Instant]] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor.selectOne("select timestampOfEvent")(
      connection =>
        connection
          .createStatement(selectTimestampOfEventSql(slice))
          .bind(0, persistenceId)
          .bind(1, seqNr),
      row => row.getTimestamp("db_timestamp"))
  }

  override def latestEventTimestamp(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      correlationId: Option[String]): Future[Option[Instant]] = {
    val executor = executorProvider.executorFor(minSlice)
    val result = executor
      .selectOne(s"select latest event timestamp for entity type [$entityType] slice range [$minSlice - $maxSlice]")(
        connection =>
          connection
            .createStatement(selectLatestEventTimestampSql(minSlice))
            .bind(0, entityType)
            .bind(1, minSlice)
            .bind(2, maxSlice),
        row => Option.when(row.get("latest_timestamp") ne null)(row.getTimestamp("latest_timestamp")))
      .map(_.flatten)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(timestamp =>
        log.debug(
          "Latest event timestamp for entity type [{}] slice range [{} - {}]: [{}]{}",
          entityType,
          minSlice,
          maxSlice,
          timestamp,
          CorrelationId.toLogText(correlationId)))

    result
  }

  override def loadEvent(
      persistenceId: String,
      seqNr: Long,
      includePayload: Boolean,
      correlationId: Option[String]): Future[Option[SerializedJournalRow]] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    executor.selectOne("select one event")(
      connection => {
        val selectSql = if (includePayload) selectOneEventSql(slice) else selectOneEventWithoutPayloadSql(slice)
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
          slice = slice,
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
  }

  override def loadLastEvent(
      persistenceId: String,
      toSeqNr: Long,
      includeDeleted: Boolean,
      correlationId: Option[String]): Future[Option[SerializedJournalRow]] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val selectSql = if (includeDeleted) selectLastEventIncludeDeletedSql(slice) else selectLastEventSql(slice)
    executor.selectOne("select last event")(
      connection => {
        connection
          .createStatement(selectSql)
          .bind(0, persistenceId)
          .bind(1, toSeqNr)
      },
      row => {
        if (includeDeleted && row.get[java.lang.Boolean]("deleted", classOf[java.lang.Boolean])) {
          // deleted row
          deletedJournalRow(slice, persistenceId, row)
        } else {
          SerializedJournalRow(
            slice = slice,
            entityType = row.get("entity_type", classOf[String]),
            persistenceId,
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = Some(row.getPayload("event_payload")),
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = row.get("writer", classOf[String]),
            tags = row.getTags("tags"),
            metadata = readMetadata(row))
        }
      })
  }

  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      includeDeleted: Boolean,
      correlationId: Option[String]): Source[SerializedJournalRow, NotUsed] = {
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)
    val executor = executorProvider.executorFor(slice)
    val result = executor.select(s"select eventsByPersistenceId [$persistenceId]")(
      connection => {
        val selectSql = if (includeDeleted) selectEventsIncludeDeletedSql(slice) else selectEventsSql(slice)
        val stmt = connection.createStatement(selectSql)
        bindSelectEventsSql(stmt, persistenceId, fromSequenceNr, toSequenceNr, settings.querySettings.bufferSize)
      },
      row =>
        if (includeDeleted && row.get[java.lang.Boolean]("deleted", classOf[java.lang.Boolean])) {
          // deleted row
          deletedJournalRow(slice, persistenceId, row)
        } else {
          SerializedJournalRow(
            slice = slice,
            entityType = row.get("entity_type", classOf[String]),
            persistenceId = persistenceId,
            seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.getTimestamp("db_timestamp"),
            readDbTimestamp = row.getTimestamp("read_db_timestamp"),
            payload = Some(row.getPayload("event_payload")),
            serId = row.get[Integer]("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = row.get("writer", classOf[String]),
            tags = row.getTags("tags"),
            metadata = readMetadata(row))
        })

    if (log.isDebugEnabled)
      result.foreach(rows =>
        log.debug(
          "Read [{}] events for persistenceId [{}]{}",
          rows.size,
          persistenceId,
          CorrelationId.toLogText(correlationId)))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  private def deletedJournalRow(slice: Int, persistenceId: String, row: Row): SerializedJournalRow = {
    SerializedJournalRow(
      slice = slice,
      entityType = row.get("entity_type", classOf[String]),
      persistenceId = persistenceId,
      seqNr = row.get[java.lang.Long]("seq_nr", classOf[java.lang.Long]),
      dbTimestamp = row.getTimestamp("db_timestamp"),
      readDbTimestamp = row.getTimestamp("read_db_timestamp"),
      payload = None,
      serId = 0,
      serManifest = "",
      writerUuid = "",
      tags = Set.empty,
      metadata = None)
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
    val actualLimit = if (limit > Int.MaxValue) Int.MaxValue else limit.toInt
    val likeStmtPostfix = PersistenceId.DefaultSeparator + "%"
    val results: immutable.IndexedSeq[Future[immutable.IndexedSeq[String]]] =
      // query each data partition
      settings.dataPartitionSliceRanges.map { sliceRange =>
        val executor = executorProvider.executorFor(sliceRange.min)
        executor.select(s"select persistenceIds by entity type")(
          connection =>
            afterId match {
              case Some(after) =>
                val stmt = connection.createStatement(persistenceIdsForEntityTypeAfterSql(sliceRange.min))
                bindPersistenceIdsForEntityTypeAfterSql(stmt, entityType, likeStmtPostfix, after, actualLimit)

              case None =>
                val stmt = connection.createStatement(persistenceIdsForEntityTypeSql(sliceRange.min))
                bindPersistenceIdsForEntityTypeSql(stmt, entityType, likeStmtPostfix, actualLimit)

            },
          row => row.get("persistence_id", classOf[String]))
      }

    // Theoretically it could blow up with too many rows (> Int.MaxValue) when fetching from more than
    // one data partition, but we have other places with a hard limit of a total number of persistenceIds less
    // than Int.MaxValue.
    val combined: Future[immutable.IndexedSeq[String]] =
      if (results.size == 1) results.head // no data partition databases
      else Future.sequence(results).map(_.flatten.sorted.take(actualLimit))

    if (log.isDebugEnabled)
      combined.foreach(rows => log.debug("Read [{}] persistence ids by entity type [{}]", rows.size, entityType))

    Source.futureSource(combined.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  protected def bindAllPersistenceIdsAfterSql(stmt: Statement, after: String, limit: Long): Statement = {
    stmt
      .bind(0, after)
      .bind(1, limit)
  }

  override def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val actualLimit = if (limit > Int.MaxValue) Int.MaxValue else limit.toInt
    val results: immutable.IndexedSeq[Future[immutable.IndexedSeq[String]]] =
      // query each data partition
      settings.dataPartitionSliceRanges.map { sliceRange =>
        val executor = executorProvider.executorFor(sliceRange.min)
        executor.select(s"select persistenceIds")(
          connection =>
            afterId match {
              case Some(after) =>
                val stmt = connection.createStatement(allPersistenceIdsAfterSql(sliceRange.min))
                bindAllPersistenceIdsAfterSql(stmt, after, actualLimit)

              case None =>
                connection
                  .createStatement(allPersistenceIdsSql(sliceRange.min))
                  .bind(0, actualLimit)
            },
          row => row.get("persistence_id", classOf[String]))
      }

    // Theoretically it could blow up with too many rows (> Int.MaxValue) when fetching from more than
    // one data partition, but we have other places with a hard limit of a total number of persistenceIds less
    // than Int.MaxValue.
    val combined: Future[immutable.IndexedSeq[String]] =
      if (results.size == 1) results.head // no data partitions
      else Future.sequence(results).map(_.flatten.sorted.take(actualLimit))

    if (log.isDebugEnabled)
      combined.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(combined.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

}
