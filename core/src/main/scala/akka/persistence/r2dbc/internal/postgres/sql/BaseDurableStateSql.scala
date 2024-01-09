/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.sql

import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import io.r2dbc.spi.Statement

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

trait BaseDurableStateSql {
  def bindForStateBySlicesRangeSql(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration): Statement

  def stateBySlicesRangeSql(
      entityType: String,
      maxDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean,
      minSlice: Int,
      maxSlice: Int): String

  def bindForAllPersistenceIdsSql(stmt: Statement, limit: Long): Statement

  def allPersistenceIdsSql(table: String): String

  def bindForAllPersistenceIdsAfter(stmt: Statement, after: String, limit: Long): Statement

  def allPersistenceIdsAfterSql(table: String): String

  def bindPersistenceIdsForEntityType(stmt: Statement, str: String, limit: Long): Statement

  def persistenceIdsForEntityTypeSql(table: String): String

  def bindPersistenceIdsForEntityTypeAfter(stmt: Statement, str: String, after: String, limit: Long): Statement

  def bindSelectBucketsForCoundBuckets(
      stmt: Statement,
      entityType: String,
      fromTimestamp: Instant,
      toTimestamp: Instant,
      limit: Int): Statement

  def persistenceIdsForEntityTypeAfterSql(table: String): String

  def selectBucketsSql(entityType: String, minSlice: Int, maxSlice: Int): String

  def bindForSelectStateSql(stmt: Statement, persistenceId: String): Statement

  def selectStateSql(entityType: String): String

  def bindUpdateStateSqlForDeleteState(
      stmt: Statement,
      revision: Long,
      persistenceId: String,
      previousRevision: Long): _root_.io.r2dbc.spi.Statement

  def bindDeleteStateForInsertState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): Statement

  def binUpdateStateSqlForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      state: SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      previousRevision: Long): Statement

  def bindInsertStateForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      slice: Int,
      entityType: String,
      state: SerializedStateRow,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement

  def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String

  def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String

  def bindForHardDeleteState(stmt: Statement, persistenceId: String): Statement

  def hardDeleteStateSql(entityType: String): String

  protected def sliceCondition(minSlice: Int, maxSlice: Int): String =
    s"slice in (${(minSlice to maxSlice).mkString(",")})"
}
