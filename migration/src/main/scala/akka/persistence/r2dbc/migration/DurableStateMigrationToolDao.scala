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

  /**
   * For migration, we want to INSERT INTO if there is now state yet, regardless of the revision. In production, we only
   * want to INSERT INTO if its the first revision.
   * @return
   */
  override protected def shouldInsert: SerializedStateRow => Future[Boolean] = (state: SerializedStateRow) =>
    readState(state.persistenceId).map {
      case Some(_) => false
      case None    => true
    }

}
