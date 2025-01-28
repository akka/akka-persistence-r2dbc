/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.ExecutionContext

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import io.r2dbc.spi.Statement

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class SqlServerMigrationToolDao(executorProvider: R2dbcExecutorProvider)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends MigrationToolDao(executorProvider) {

  import settings.codecSettings.JournalImplicits._

  override protected def createMigrationProgressTableSql(): String = {
    sql"""
          IF object_id('migration_progress') is null
            CREATE TABLE migration_progress(
              persistence_id NVARCHAR(255) NOT NULL,
              event_seq_nr BIGINT,
              snapshot_seq_nr BIGINT,
              state_revision  BIGINT,
              PRIMARY KEY(persistence_id)
            )"""
  }

  override def baseUpsertMigrationProgressSql(column: String): String = {
    sql"""
         UPDATE migration_progress SET
           $column = @bindColumn
         WHERE persistence_id = @persistenceId
         if @@ROWCOUNT = 0
              INSERT INTO migration_progress
              (persistence_id, $column)
              VALUES(@persistenceId, @bindColumn)

         """
  }

  // necessary, otherwise we would need to bind both columns multiple times
  override protected def bindBaseUpsertSql(stmt: Statement, persistenceId: String, columnValue: Long): Statement = {
    stmt
      .bind("@persistenceId", persistenceId)
      .bind("@bindColumn", columnValue)
  }

}
