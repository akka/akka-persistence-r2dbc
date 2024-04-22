/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class DurableStateMigrationToolDao(
    executorProvider: R2dbcExecutorProvider,
    dialect: Dialect)(implicit ec: ExecutionContext)
    extends PostgresDurableStateDao(executorProvider, dialect) {

  def upsertDurableState(state: SerializedStateRow, value: Any): Future[Done] = {
    val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)
    val r2dbcExecutor = executorProvider.executorFor(slice)
    val entityType = PersistenceId.extractEntityType(state.persistenceId)
    readState(state.persistenceId).flatMap {
      case Some(existingState) => update(r2dbcExecutor, existingState, state, value, slice, entityType)
      case None                => insert(r2dbcExecutor, state, value, slice, entityType)
    }
  }

  private def insert(
      r2dbcExecutor: R2dbcExecutor,
      newRow: SerializedStateRow,
      newValue: Any,
      slice: Int,
      entityType: String) = {
    r2dbcExecutor
      .updateOne(s"insert state [${newRow.persistenceId}] for migration") { connection =>
        insertStatement(entityType, newRow, newValue, slice, connection)
      }
      .map(_ => Done)(ExecutionContexts.parasitic)
  }

  private def update(
      r2dbcExecutor: R2dbcExecutor,
      currentState: SerializedStateRow,
      newRow: SerializedStateRow,
      newValue: Any,
      slice: Int,
      entityType: String) = {
    r2dbcExecutor
      .updateOne(s"update state [${newRow.persistenceId}] for migration") { connection =>
        updateStatement(entityType, newRow, newValue, slice, connection, currentState.revision)
      }
      .map(_ => Done)(ExecutionContexts.parasitic)
  }
}
