/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class DurableStateMigrationToolDao(
    executorProvider: R2dbcExecutorProvider,
    dialect: Dialect)(implicit ec: ExecutionContext)
    extends PostgresDurableStateDao(executorProvider, dialect) {

  def upsertDurableState(state: SerializedStateRow, value: Any): Future[Done] = {

    def shouldInsert = (state: SerializedStateRow) =>
      readState(state.persistenceId).map {
        case Some(_) => false
        case None    => true
      }
    upsertState(state, value, None, shouldInsert).map(_ => Done)(ExecutionContexts.parasitic)
  }
}
