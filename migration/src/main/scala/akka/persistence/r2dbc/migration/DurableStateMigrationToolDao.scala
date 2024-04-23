/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.migration

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.r2dbc.spi.Connection

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.DurableStateDao.EmptyDbTimestamp
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutorProvider
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.FutureDone
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
    val bindings = additionalBindings(entityType, state, value)

    val upsertSql =
      sql"""
         ${insertStateSql(slice, entityType, bindings)}
         ON CONFLICT(persistence_id)
         DO UPDATE SET
           slice = EXCLUDED.slice,
           entity_type = EXCLUDED.entity_type,
           revision = EXCLUDED.revision,
           state_ser_id = EXCLUDED.state_ser_id,
           state_ser_manifest = EXCLUDED.state_ser_manifest,
           state_payload = EXCLUDED.state_payload,
           tags = EXCLUDED.tags,
           test_column = EXCLUDED.test_column,
           db_timestamp = EXCLUDED.db_timestamp
         """.stripMargin

    def upsertStatement(connection: Connection) = {
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
      bindAdditionalColumns(idx.next, stmt, bindings)
      bindTimestampNow(stmt, idx.next)
    }
    val result = if (changeHandlers.contains(entityType)) {
      def change = new UpdatedDurableState[Any](
        state.persistenceId,
        state.revision,
        value,
        NoOffset,
        EmptyDbTimestamp.toEpochMilli)
      r2dbcExecutor.withConnection(s"insert [${state.persistenceId}] for migration with changeHandler") { connection =>
        for {
          updatedRows <- recoverDataIntegrityViolation(state, R2dbcExecutor.updateOneInTx(upsertStatement(connection)))
          _ <- changeHandlers.get(entityType).map(processChange(_, connection, change)).getOrElse(FutureDone)
        } yield updatedRows
      }
    } else {
      r2dbcExecutor.withConnection(s"upsert [${state.persistenceId}] for migration") { connection =>
        recoverDataIntegrityViolation(state, R2dbcExecutor.updateOneInTx(upsertStatement(connection)))
      }
    }
    result.map(_ => Done)(ExecutionContexts.parasitic)
  }

}
