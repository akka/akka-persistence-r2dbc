/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver.sql

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.{ PayloadCodec, TimestampCodec }
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.TimestampCodec.{ RichStatement => TimestampRichStatement }
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.internal.postgres.sql.BaseDurableStateSql
import akka.persistence.r2dbc.internal.sqlserver.SqlServerDialectHelper
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

import java.lang
import java.time.{ Instant, LocalDateTime }
import scala.collection.immutable
import scala.concurrent.duration.{ Duration, FiniteDuration }

class SqlServerDurableStateSql(settings: R2dbcSettings)(implicit
    statePayloadCodec: PayloadCodec,
    timestampCodec: TimestampCodec)
    extends BaseDurableStateSql {

  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)

  import helper._

  def bindUpdateStateSqlForDeleteState(
      stmt: Statement,
      revision: Long,
      persistenceId: String,
      previousRevision: Long): _root_.io.r2dbc.spi.Statement = {
    stmt
      .bind("@revision", revision)
      .bind("@stateSerId", 0)
      .bind("@stateSerManifest", "")
      .bindPayloadOption("@statePayload", None)
      .bind("@now", timestampCodec.now())
      .bind("@persistenceId", persistenceId)
      .bind("@previousRevision", previousRevision)
  }

  def bindDeleteStateForInsertState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): Statement = {

    stmt
      .bind("@slice", slice)
      .bind("@entityType", entityType)
      .bind("@persistenceId", persistenceId)
      .bind("@revision", revision)
      .bind("@stateSerId", 0)
      .bind("@stateSerManifest", "")
      .bindPayloadOption("@statePayload", None)
      .bindNull("@tags", classOf[String])
      .bind("@now", timestampCodec.now())

  }

  def binUpdateStateSqlForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      state: SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      previousRevision: Long): Statement = {
    stmt
      .bind("@revision", state.revision)
      .bind("@stateSerId", state.serId)
      .bind("@stateSerManifest", state.serManifest)
      .bindPayloadOption("@statePayload", state.payload)
      .bind("@now", timestampCodec.now())
      .bind("@persistenceId", state.persistenceId)
    bindTags(stmt, "@tags", state)
    bindAdditionalColumns(stmt, additionalBindings)

    if (settings.durableStateAssertSingleWriter) {
      stmt.bind("@previousRevision", previousRevision)
    }

    stmt

  }

  def bindTags(stmt: Statement, name: String, state: SerializedStateRow): Statement = {
    if (state.tags.isEmpty)
      stmt.bindNull(name, classOf[String])
    else
      stmt.bind(name, tagsToDb(state.tags))
  }

  def bindInsertStateForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      slice: Int,
      entityType: String,
      state: SerializedStateRow,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {

    stmt
      .bind("@slice", slice)
      .bind("@entityType", entityType)
      .bind("@persistenceId", state.persistenceId)
      .bind("@revision", state.revision)
      .bind("@stateSerId", state.serId)
      .bind("@stateSerManifest", state.serManifest)
      .bindPayloadOption("@statePayload", state.payload)
      .bind("@now", timestampCodec.now())
    bindTags(stmt, "@tags", state)
    bindAdditionalColumns(stmt, additionalBindings)
  }

  def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {

    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    val revisionCondition =
      if (settings.durableStateAssertSingleWriter) " AND revision = @previousRevision"
      else ""

    val tags = if (updateTags) ", tags = @tags" else ""

    val additionalParams = additionalUpdateParameters(additionalBindings)
    sql"""
    UPDATE $stateTable
    SET revision = @revision, state_ser_id = @stateSerId, state_ser_manifest = @stateSerManifest, state_payload = @statePayload $tags $additionalParams, db_timestamp = @now
    WHERE persistence_id = @persistenceId
    $revisionCondition"""

  }

  override def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
      INSERT INTO $stateTable
      (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
      VALUES (@slice, @entityType, @persistenceId, @revision, @stateSerId, @stateSerManifest, @statePayload, @tags$additionalParams, @now)"""
  }

  def bindForHardDeleteState(stmt: Statement, persistenceId: String): Statement = {
    stmt.bind("@persistenceId", persistenceId)
  }

  def hardDeleteStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"DELETE from $stateTable WHERE persistence_id = @persistenceId"
  }

  private def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, bindBalue) if bindBalue != AdditionalColumn.Skip =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, _) =>
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
        case EvaluatedAdditionalColumnBindings(col, bindValue) if bindValue != AdditionalColumn.Skip =>
          strB.append(s", @${col.columnName}")
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

  private def additionalUpdateParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, binValue) if binValue != AdditionalColumn.Skip =>
          strB.append(", ").append(col.columnName).append(s" = @${col.columnName}")
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

  def bindAdditionalColumns(
      stmt: Statement,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
    additionalBindings.foreach {
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindValue(v)) =>
        stmt.bind(s"@${col.columnName}", v)
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
        stmt.bindNull(s"@${col.columnName}", col.fieldClass)
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
    }
    stmt
  }

  override def bindForSelectStateSql(stmt: Statement, persistenceId: String): Statement =
    stmt.bind("@persistenceId", persistenceId)

  override def selectStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = @persistenceId"""
  }

  override def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    // group by column alias (bucket) needs a sub query
    val subQuery =
      s"""
         | select TOP(@limit) CAST(DATEDIFF(s,'1970-01-01 00:00:00',db_timestamp) AS BIGINT) / 10 AS bucket
         | FROM $stateTable
         | WHERE entity_type = @entityType
         | AND ${sliceCondition(minSlice, maxSlice)}
         | AND db_timestamp >= @fromTimestamp AND db_timestamp <= @toTimestamp
         |""".stripMargin
    sql"""
       SELECT bucket,  count(*) as count from ($subQuery) as sub
       GROUP BY bucket ORDER BY bucket
       """
  }

  override def bindSelectBucketsForCoundBuckets(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement = stmt
    .bind("@entityType", entityType)
    .bindTimestamp("@fromTimestamp", fromTimestamp)
    .bindTimestamp("@toTimestamp", toTimestamp)
    .bind("@limit", limit)

  override def persistenceIdsForEntityTypeAfterSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id LIKE @persistenceIdLike AND persistence_id > @persistenceId ORDER BY persistence_id"

  override def bindPersistenceIdsForEntityTypeAfter(
      stmt: Statement,
      entityTypePlusLikeStmtPostfix: String,
      afterPersistenceId: String,
      limit: Long): Statement = {
    stmt
      .bind("@persistenceIdLike", entityTypePlusLikeStmtPostfix)
      .bind("@persistenceId", afterPersistenceId)
      .bind("@limit", limit)
  }

  override def persistenceIdsForEntityTypeSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id LIKE @persistenceIdLike ORDER BY persistence_id"

  override def bindPersistenceIdsForEntityType(
      stmt: Statement,
      entityTypePlusLikeStmtPostfix: String,
      limit: Long): Statement =
    stmt
      .bind("@persistenceIdLike", entityTypePlusLikeStmtPostfix)
      .bind("@limit", limit)

  override def allPersistenceIdsAfterSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table WHERE persistence_id > @persistenceId ORDER BY persistence_id"

  override def bindForAllPersistenceIdsAfter(stmt: Statement, after: String, limit: Long): Statement = stmt
    .bind("@persistenceId", after)
    .bind("@limit", limit)

  override def bindForAllPersistenceIdsSql(stmt: Statement, limit: Long): Statement =
    stmt.bind("@limit", limit)

  override def allPersistenceIdsSql(table: String): String =
    sql"SELECT TOP(@limit) persistence_id from $table ORDER BY persistence_id"

  override def bindForStateBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration): Statement = {
    stmt
      .bind("@entityType", entityType)
      .bindTimestamp("@fromTimestamp", fromTimestamp)

    stmt.bind("@limit", settings.querySettings.bufferSize)

    if (behindCurrentTime > Duration.Zero) {
      stmt.bind("@now", timestampCodec.now())
    }

    toTimestamp.foreach(until => stmt.bindTimestamp("@until", until))

    stmt
  }

  private def behindCurrentTimeIntervalConditionFor(behindCurrentTime: FiniteDuration): String =
    if (behindCurrentTime > Duration.Zero)
      s"AND db_timestamp < DATEADD(ms, -${behindCurrentTime.toMillis}, @now)"
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
      if (maxDbTimestampParam) s"AND db_timestamp < @until" else ""

    val behindCurrentTimeIntervalCondition = behindCurrentTimeIntervalConditionFor(behindCurrentTime)

    val selectColumns =
      if (backtracking)
        "SELECT TOP(@limit) persistence_id, revision, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, state_ser_id "
      else
        "SELECT TOP(@limit) persistence_id, revision, db_timestamp, SYSUTCDATETIME() AS read_db_timestamp, state_ser_id, state_ser_manifest, state_payload "

    sql"""
    $selectColumns
    FROM $stateTable
    WHERE entity_type = @entityType
    AND ${sliceCondition(minSlice, maxSlice)}
    AND db_timestamp >= @fromTimestamp $maxDbTimestampParamCondition $behindCurrentTimeIntervalCondition
    ORDER BY db_timestamp, revision"""
  }

}
