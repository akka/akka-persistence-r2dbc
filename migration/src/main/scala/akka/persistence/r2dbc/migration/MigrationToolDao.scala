/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.migration

import java.lang
import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.IdentityAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter
import org.slf4j.LoggerFactory
import io.r2dbc.spi.Statement

import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object MigrationToolDao {
  private val log = LoggerFactory.getLogger(classOf[MigrationToolDao])

  final case class CurrentProgress(
      persistenceId: String,
      eventSeqNr: Long,
      snapshotSeqNr: Long,
      durableStateRevision: Long)
}

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class MigrationToolDao(executorProvider: R2dbcExecutorProvider)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import MigrationToolDao._

  protected val settings: R2dbcSettings = executorProvider.settings
  import settings.codecSettings.JournalImplicits._

  // progress always in data partition 0
  private val r2dbcExecutor = executorProvider.executorFor(slice = 0)

  protected def createMigrationProgressTableSql(): String = {
    sql"""
          CREATE TABLE IF NOT EXISTS migration_progress(
            persistence_id VARCHAR(255) NOT NULL,
            event_seq_nr BIGINT,
            snapshot_seq_nr BIGINT,
            state_revision  BIGINT,
            PRIMARY KEY(persistence_id)
          )"""
  }

  def createProgressTable(): Future[Done] = {
    r2dbcExecutor.executeDdl("create migration progress table") { connection =>
      connection.createStatement(createMigrationProgressTableSql())
    }
  }

  protected def baseUpsertMigrationProgressSql(column: String): String = {
    sql"""
            INSERT INTO migration_progress
            (persistence_id, $column)
            VALUES (?, ?)
            ON CONFLICT (persistence_id)
            DO UPDATE SET
            $column = excluded.$column"""
  }

  protected def bindBaseUpsertSql(stmt: Statement, persistenceId: String, seqNr: Long): Statement = {
    stmt
      .bind(0, persistenceId)
      .bind(1, seqNr)
  }

  def updateEventProgress(persistenceId: String, seqNr: Long): Future[Done] = {
    r2dbcExecutor
      .updateOne(s"upsert migration progress [$persistenceId]") { connection =>
        val stmt = connection.createStatement(baseUpsertMigrationProgressSql("event_seq_nr"))
        bindBaseUpsertSql(stmt, persistenceId, seqNr)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def updateSnapshotProgress(persistenceId: String, seqNr: Long): Future[Done] = {
    r2dbcExecutor
      .updateOne(s"upsert migration progress [$persistenceId]") { connection =>
        val stmt = connection.createStatement(baseUpsertMigrationProgressSql("snapshot_seq_nr"))
        bindBaseUpsertSql(stmt, persistenceId, seqNr)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def updateDurableStateProgress(persistenceId: String, revision: Long): Future[Done] = {
    r2dbcExecutor
      .updateOne(s"upsert migration progress [$persistenceId]") { connection =>
        val stmt = connection.createStatement(baseUpsertMigrationProgressSql("state_revision"))
        bindBaseUpsertSql(stmt, persistenceId, revision)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  def currentProgress(persistenceId: String): Future[Option[CurrentProgress]] = {
    r2dbcExecutor.selectOne(s"read migration progress [$persistenceId]")(
      _.createStatement(sql"SELECT * FROM migration_progress WHERE persistence_id = ?")
        .bind(0, persistenceId),
      row =>
        CurrentProgress(
          persistenceId,
          eventSeqNr = zeroIfNull(row.get("event_seq_nr", classOf[java.lang.Long])),
          snapshotSeqNr = zeroIfNull(row.get("snapshot_seq_nr", classOf[java.lang.Long])),
          durableStateRevision = zeroIfNull(row.get("state_revision", classOf[java.lang.Long]))))
  }

  private def zeroIfNull(n: java.lang.Long): Long =
    if (n eq null) 0L else n

}
