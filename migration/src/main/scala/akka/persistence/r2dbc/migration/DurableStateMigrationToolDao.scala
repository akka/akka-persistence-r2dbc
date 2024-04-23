/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.typed.PersistenceId

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] class DurableStateMigrationToolDao(
    persistenceExt: Persistence,
    executorProvider: R2dbcExecutorProvider,
    dialect: Dialect)(implicit ec: ExecutionContext)
    extends PostgresDurableStateDao(executorProvider, dialect) {

  import settings.codecSettings.DurableStateImplicits._

  def upsertDurableState(state: SerializedStateRow, value: Any): Future[Done] = {
    val slice = persistenceExt.sliceForPersistenceId(state.persistenceId)

    val r2dbcExecutor = executorProvider.executorFor(slice)
    val entityType = PersistenceId.extractEntityType(state.persistenceId)
    val stateTable = settings.getDurableStateTableWithSchema(entityType, slice)

    //val additionalBindings = getAdditionalBindings(state, value, entityType)
    //val additionalCols = additionalInsertColumns(additionalBindings)
    //val additionalParams = additionalInsertParameters(additionalBindings)

    val upsertSql =
      sql"""
         INSERT INTO $stateTable
         (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags, db_timestamp)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
         ON CONFLICT(persistence_id)
         DO UPDATE SET
           slice = EXCLUDED.slice,
           entity_type = EXCLUDED.entity_type,
           revision = EXCLUDED.revision,
           state_ser_id = EXCLUDED.state_ser_id,
           state_ser_manifest = EXCLUDED.state_ser_manifest,
           state_payload = EXCLUDED.state_payload,
           tags = EXCLUDED.tags,
           db_timestamp = EXCLUDED.db_timestamp
         """.stripMargin

    val result = r2dbcExecutor
      .updateOne(s"insert state [${state.persistenceId}] for migration") { connection =>
        val idx = Iterator.range(0, Int.MaxValue)
        val stmt = connection
          .createStatement(upsertSql)
          .bind(idx.next(), slice)
          .bind(idx.next(), entityType)
          .bind(idx.next(), state.persistenceId)
          .bind(idx.next(), state.revision)
          .bind(idx.next(), state.serId)
          .bind(idx.next(), state.serManifest)
          .bindPayloadOption(idx.next(), state.payload)
        bindTags(state, stmt, idx.next())
        bindTimestampNow(stmt, idx.next)
      }
    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

}
