/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.sql

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.{ PayloadCodec, TimestampCodec }
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichStatement => TimestampRichStatement }

import java.lang
import java.time.Instant
import scala.collection.immutable
import scala.concurrent.duration.{ Duration, FiniteDuration }

class PostgresDurableStateSql(settings: R2dbcSettings)(implicit
    statePayloadCodec: PayloadCodec,
    timestampCodec: TimestampCodec)
    extends BaseDurableStateSql {
  def bindUpdateStateSqlForDeleteState(
      stmt: Statement,
      revision: Long,
      persistenceId: String,
      previousRevision: Long): _root_.io.r2dbc.spi.Statement = {

    stmt
      .bind(0, revision)
      .bind(1, 0)
      .bind(2, "")
      .bindPayloadOption(3, None)

    if (settings.dbTimestampMonotonicIncreasing) {
      if (settings.durableStateAssertSingleWriter)
        stmt
          .bind(4, persistenceId)
          .bind(5, previousRevision)
      else
        stmt
          .bind(4, persistenceId)
    } else {
      stmt
        .bind(4, persistenceId)
        .bind(5, previousRevision)
        .bind(6, persistenceId)

      if (settings.durableStateAssertSingleWriter)
        stmt.bind(7, previousRevision)
      else
        stmt
    }

  }

  def bindDeleteStateForInsertState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): Statement = {
    stmt
      .bind(0, slice)
      .bind(1, entityType)
      .bind(2, persistenceId)
      .bind(3, revision)
      .bind(4, 0)
      .bind(5, "")
      .bindPayloadOption(6, None)
      .bindNull(7, classOf[Array[String]])
  }

  def binUpdateStateSqlForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      state: SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      previousRevision: Long): Statement = {
    stmt
      .bind(getAndIncIndex(), state.revision)
      .bind(getAndIncIndex(), state.serId)
      .bind(getAndIncIndex(), state.serManifest)
      .bindPayloadOption(getAndIncIndex(), state.payload)
    bindTags(stmt, getAndIncIndex(), state)
    bindAdditionalColumns(stmt, additionalBindings, getAndIncIndex)

    if (settings.dbTimestampMonotonicIncreasing) {
      if (settings.durableStateAssertSingleWriter)
        stmt
          .bind(getAndIncIndex(), state.persistenceId)
          .bind(getAndIncIndex(), previousRevision)
      else
        stmt
          .bind(getAndIncIndex(), state.persistenceId)
    } else {
      stmt
        .bind(getAndIncIndex(), state.persistenceId)
        .bind(getAndIncIndex(), previousRevision)
        .bind(getAndIncIndex(), state.persistenceId)

      if (settings.durableStateAssertSingleWriter)
        stmt.bind(getAndIncIndex(), previousRevision)
      else
        stmt
    }
  }

  // duplicated for now
  private def bindTags(stmt: Statement, i: Int, state: SerializedStateRow): Statement = {
    if (state.tags.isEmpty)
      stmt.bindNull(i, classOf[Array[String]])
    else
      stmt.bind(i, state.tags.toArray)
  }

  // duplicated for now
  def bindAdditionalColumns(
      stmt: Statement,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      getAndIncIndex: () => Int): Statement = {
    additionalBindings.foreach {
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindValue(v)) =>
        stmt.bind(getAndIncIndex(), v)
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
        stmt.bindNull(getAndIncIndex(), col.fieldClass)
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
    }
    stmt
  }

  def bindInsertStateForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      slice: Int,
      entityType: String,
      state: SerializedStateRow,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]) = {
    val st = stmt
      .bind(getAndIncIndex(), slice)
      .bind(getAndIncIndex(), entityType)
      .bind(getAndIncIndex(), state.persistenceId)
      .bind(getAndIncIndex(), state.revision)
      .bind(getAndIncIndex(), state.serId)
      .bind(getAndIncIndex(), state.serManifest)
      .bindPayloadOption(getAndIncIndex(), state.payload)
    bindTags(st, getAndIncIndex(), state)
    bindAdditionalColumns(st, additionalBindings, getAndIncIndex)
  }

  private def additionalUpdateParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    val timestamp =
      if (settings.dbTimestampMonotonicIncreasing)
        "CURRENT_TIMESTAMP"
      else
        "GREATEST(CURRENT_TIMESTAMP, " +
        s"(SELECT db_timestamp + '1 microsecond'::interval FROM $stateTable WHERE persistence_id = ? AND revision = ?))"

    val revisionCondition =
      if (settings.durableStateAssertSingleWriter) " AND revision = ?"
      else ""

    val tags = if (updateTags) ", tags = ?" else ""

    val additionalParams = additionalUpdateParameters(additionalBindings)
    sql"""
    UPDATE $stateTable
    SET revision = ?, state_ser_id = ?, state_ser_manifest = ?, state_payload = ?$tags$additionalParams, db_timestamp = $timestamp
    WHERE persistence_id = ?
    $revisionCondition"""
  }

  private def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(c, AdditionalColumn.BindNull) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  private def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(_, _: AdditionalColumn.BindValue[_]) |
            EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindNull) =>
          strB.append(", ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {

    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
      INSERT INTO $stateTable
      (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?$additionalParams, CURRENT_TIMESTAMP)"""

  }

  def bindForHardDeleteState(stmt: Statement, persistenceId: String): Statement = {
    stmt.bind(0, persistenceId)
  }

  def hardDeleteStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"DELETE from $stateTable WHERE persistence_id = ?"
  }

  override def selectStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = ?"""
  }

  override def bindForSelectStateSql(stmt: Statement, persistenceId: String): Statement = stmt.bind(0, persistenceId)

  override def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
     SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
     FROM $stateTable
     WHERE entity_type = ?
     AND ${sliceCondition(minSlice, maxSlice)}
     AND db_timestamp >= ? AND db_timestamp <= ?
     GROUP BY bucket ORDER BY bucket LIMIT ?
     """
  }

  override def bindSelectBucketsForCoundBuckets(
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

  override def persistenceIdsForEntityTypeAfterSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? AND persistence_id > ? ORDER BY persistence_id LIMIT ?"

  override def bindPersistenceIdsForEntityTypeAfter(
      stmt: Statement,
      entityTypePluslikeStmtPostfix: String,
      after: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityTypePluslikeStmtPostfix)
      .bind(1, after)
      .bind(2, limit)
  }

  override def persistenceIdsForEntityTypeSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id LIKE ? ORDER BY persistence_id LIMIT ?"

  override def bindPersistenceIdsForEntityType(
      stmt: Statement,
      entityTypePlusLikeStmtPostfix: String,
      limit: Long): Statement = {
    stmt
      .bind(0, entityTypePlusLikeStmtPostfix)
      .bind(1, limit)
  }

  override def bindForAllPersistenceIdsAfter(stmt: Statement, after: String, limit: Long): Statement =
    stmt
      .bind(0, after)
      .bind(1, limit)

  override def allPersistenceIdsAfterSql(table: String): String =
    sql"SELECT persistence_id from $table WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

  override def allPersistenceIdsSql(table: String): String =
    sql"SELECT persistence_id from $table ORDER BY persistence_id LIMIT ?"

  override def bindForAllPersistenceIdsSql(stmt: Statement, limit: Long): Statement =
    stmt.bind(0, limit)

  protected def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < CURRENT_TIMESTAMP - interval '${behindCurrentTime.toMillis} milliseconds'"
    else ""

  override def stateBySlicesRangeSql(
      entityType: String,
      maxDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    def maxDbTimestampParamCondition =
      if (maxDbTimestampParam) s"AND db_timestamp < ?" else ""

    val behindCurrentTimeIntervalCondition = behindCurrentTimeIntervalConditionFor(behindCurrentTime)

    val selectColumns =
      if (backtracking)
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id "
      else
        "SELECT persistence_id, revision, db_timestamp, CURRENT_TIMESTAMP AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
    $selectColumns
    FROM $stateTable
    WHERE entity_type = ?
    AND ${sliceCondition(minSlice, maxSlice)}
    AND db_timestamp >= ? $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
    ORDER BY db_timestamp, revision
    LIMIT ?"""
  }

  override def bindForStateBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration): Statement = {
    stmt
      .bind(0, entityType)
      .bind(1, fromTimestamp)
    toTimestamp match {
      case Some(until) =>
        stmt.bind(2, until)
        stmt.bind(3, settings.querySettings.bufferSize)
      case None =>
        stmt.bind(2, settings.querySettings.bufferSize)
    }
    stmt
  }

}
