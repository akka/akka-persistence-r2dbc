/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SliceUtils
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object QueryDao {
  val log: Logger = LoggerFactory.getLogger(classOf[QueryDao])
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class QueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends BySliceQuery.Dao[SerializedJournalRow] {
  import QueryDao.log
  import JournalDao.readMetadata

  private val journalTable = settings.journalTableWithSchema

  private val currentDbTimestampSql =
    "SELECT transaction_timestamp() AS db_timestamp"

  private def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): String = {
    var p = 0

    def nextParam(): String = {
      p += 1
      "$" + p
    }

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) s"AND db_timestamp <= ${nextParam()}" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < transaction_timestamp() - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    selectColumns +
    s"FROM $journalTable " +
    s"WHERE entity_type = ${nextParam()} " +
    s"AND slice BETWEEN ${nextParam()} AND ${nextParam()} " +
    s"AND db_timestamp >= ${nextParam()} $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition " +
    s"AND deleted = false " +
    s"ORDER BY db_timestamp, seq_nr " +
    s"LIMIT ${nextParam()}"
  }

  private val selectTimestampOfEventSql =
    s"SELECT db_timestamp FROM $journalTable " +
    "WHERE slice = $1 AND entity_type = $2 AND persistence_id = $3 AND seq_nr = $4 AND deleted = false"

  private val selectOneEventSql =
    s"SELECT db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload " +
    s"FROM $journalTable " +
    "WHERE slice = $1 AND entity_type = $2 AND persistence_id = $3 AND seq_nr = $4 AND deleted = false"

  private val selectEventsSql =
    s"SELECT slice, entity_type, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload " +
    s"from $journalTable " +
    "WHERE slice = $1 AND entity_type = $2 AND persistence_id = $3 AND seq_nr >= $4 AND seq_nr <= $5 " +
    "AND deleted = false " +
    "ORDER BY seq_nr " +
    "LIMIT $6"

  private val allPersistenceIdsSql =
    s"SELECT DISTINCT(persistence_id) from $journalTable ORDER BY persistence_id LIMIT $$1"

  private val allPersistenceIdsAfterSql =
    s"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id > $$1 ORDER BY persistence_id LIMIT $$2"

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.createStatement(currentDbTimestampSql),
        row => row.get("db_timestamp", classOf[Instant]))
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  def rowsBySlices(
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
            eventsBySlicesRangeSql(toDbTimestampParam = toTimestamp.isDefined, behindCurrentTime, backtracking))
          .bind(0, entityType)
          .bind(1, minSlice)
          .bind(2, maxSlice)
          .bind(3, fromTimestamp)
        toTimestamp match {
          case Some(until) =>
            stmt.bind(4, until)
            stmt.bind(5, settings.querySettings.bufferSize)
          case None =>
            stmt.bind(4, settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        if (backtracking)
          SerializedJournalRow(
            slice = row.get("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = None, // lazy loaded for backtracking
            serId = 0,
            serManifest = "",
            writerUuid = "", // not need in this query
            metadata = None)
        else
          SerializedJournalRow(
            slice = row.get("slice", classOf[Integer]),
            entityType,
            persistenceId = row.get("persistence_id", classOf[String]),
            seqNr = row.get("seq_nr", classOf[java.lang.Long]),
            dbTimestamp = row.get("db_timestamp", classOf[Instant]),
            readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
            payload = Some(row.get("event_payload", classOf[Array[Byte]])),
            serId = row.get("event_ser_id", classOf[Integer]),
            serManifest = row.get("event_ser_manifest", classOf[String]),
            writerUuid = "", // not need in this query
            metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def timestampOfEvent(entityType: String, persistenceId: String, slice: Int, seqNr: Long): Future[Option[Instant]] = {
    r2dbcExecutor.selectOne("select timestampOfEvent")(
      connection =>
        connection
          .createStatement(selectTimestampOfEventSql)
          .bind(0, slice)
          .bind(1, entityType)
          .bind(2, persistenceId)
          .bind(3, seqNr),
      row => row.get("db_timestamp", classOf[Instant]))
  }

  def loadEvent(
      entityType: String,
      persistenceId: String,
      slice: Int,
      seqNr: Long): Future[Option[SerializedJournalRow]] =
    r2dbcExecutor.selectOne("select one event")(
      connection =>
        connection
          .createStatement(selectOneEventSql)
          .bind(0, slice)
          .bind(1, entityType)
          .bind(2, persistenceId)
          .bind(3, seqNr),
      row =>
        SerializedJournalRow(
          slice,
          entityType,
          persistenceId,
          seqNr,
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
          payload = Some(row.get("event_payload", classOf[Array[Byte]])),
          serId = row.get("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = "", // not need in this query
          metadata = readMetadata(row)))

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {
    val entityType = SliceUtils.extractEntityTypeFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, settings.maxNumberOfSlices)

    val result = r2dbcExecutor.select(s"select eventsByPersistenceId [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectEventsSql)
          .bind(0, slice)
          .bind(1, entityType)
          .bind(2, persistenceId)
          .bind(3, fromSequenceNr)
          .bind(4, toSequenceNr)
          .bind(5, settings.querySettings.bufferSize),
      row =>
        SerializedJournalRow(
          slice = row.get("slice", classOf[Integer]),
          entityType = row.get("entity_type", classOf[String]),
          persistenceId = row.get("persistence_id", classOf[String]),
          seqNr = row.get("seq_nr", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = row.get("read_db_timestamp", classOf[Instant]),
          payload = Some(row.get("event_payload", classOf[Array[Byte]])),
          serId = row.get("event_ser_id", classOf[Integer]),
          serManifest = row.get("event_ser_manifest", classOf[String]),
          writerUuid = row.get("writer", classOf[String]),
          metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events for persistenceId [{}]", rows.size, persistenceId))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            connection
              .createStatement(allPersistenceIdsAfterSql)
              .bind(0, after)
              .bind(1, limit)
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
