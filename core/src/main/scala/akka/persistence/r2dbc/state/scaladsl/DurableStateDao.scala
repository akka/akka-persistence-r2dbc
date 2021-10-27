/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state.scaladsl

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.SliceUtils
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object DurableStateDao {
  val log: Logger = LoggerFactory.getLogger(classOf[DurableStateDao])
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedStateRow(
      persistenceId: String,
      revision: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      timestamp: Long,
      payload: Array[Byte],
      serId: Int,
      serManifest: String)
}

/**
 * INTERNAL API
 *
 * Class for encapsulating db interaction.
 */
@InternalApi
private[r2dbc] class DurableStateDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {
  import DurableStateDao._

  private val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(ec, system)

  private val stateTable = settings.durableStateTableWithSchema

  private val selectStateSql: String =
    s"SELECT * from $stateTable WHERE slice = $$1 AND entity_type_hint = $$2 AND persistence_id = $$3"

  private val insertStateSql: String =
    s"INSERT INTO $stateTable " +
    "(slice, entity_type_hint, persistence_id, revision, write_timestamp, state_ser_id, state_ser_manifest, state_payload, db_timestamp) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, transaction_timestamp())"

  private val updateStateSql: String =
    s"UPDATE $stateTable " +
    "SET revision = $1, write_timestamp = $2, state_ser_id = $3, state_ser_manifest = $4, state_payload = $5, db_timestamp = " +
    "GREATEST(transaction_timestamp(), " +
    s"(SELECT db_timestamp + '1 microsecond'::interval FROM $stateTable WHERE slice = $$6 AND entity_type_hint = $$7 AND persistence_id = $$8 AND revision = $$9)) " +
    "WHERE slice = $10 AND entity_type_hint = $11 AND persistence_id = $12 AND revision = $13"

  private val deleteStateSql: String =
    s"DELETE from $stateTable WHERE slice = $$1 AND entity_type_hint = $$2 AND persistence_id = $$3"

  def readState(persistenceId: String): Future[Option[SerializedStateRow]] = {
    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, settings.maxNumberOfSlices)

    r2dbcExecutor.selectOne(s"select [$persistenceId]")(
      connection =>
        connection
          .createStatement(selectStateSql)
          .bind(0, slice)
          .bind(1, entityTypeHint)
          .bind(2, persistenceId),
      row =>
        SerializedStateRow(
          persistenceId = persistenceId,
          revision = row.get("revision", classOf[java.lang.Long]),
          dbTimestamp = row.get("db_timestamp", classOf[Instant]),
          readDbTimestamp = Instant.EPOCH, // not needed here
          timestamp = row.get("write_timestamp", classOf[java.lang.Long]),
          payload = row.get("state_payload", classOf[Array[Byte]]),
          serId = row.get("state_ser_id", classOf[java.lang.Integer]),
          serManifest = row.get("state_ser_manifest", classOf[String])))
  }

  def writeState(state: SerializedStateRow): Future[Done] = {
    require(state.revision > 0)

    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(state.persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(state.persistenceId, settings.maxNumberOfSlices)

    val result = {
      if (state.revision == 1) {
        r2dbcExecutor
          .updateOne(s"insert [${state.persistenceId}]") { connection =>
            connection
              .createStatement(insertStateSql)
              .bind(0, slice)
              .bind(1, entityTypeHint)
              .bind(2, state.persistenceId)
              .bind(3, state.revision)
              .bind(4, state.timestamp)
              .bind(5, state.serId)
              .bind(6, state.serManifest)
              .bind(7, state.payload)
          }
          .recoverWith { case _: R2dbcDataIntegrityViolationException =>
            Future.failed(
              new IllegalStateException(
                s"Insert failed: durable state for persistence id [${state.persistenceId}] already exists"))
          }
      } else {
        val previousRevision = state.revision - 1

        r2dbcExecutor.updateOne(s"update [${state.persistenceId}]") { connection =>
          connection
            .createStatement(updateStateSql)
            .bind(0, state.revision)
            .bind(1, state.timestamp)
            .bind(2, state.serId)
            .bind(3, state.serManifest)
            .bind(4, state.payload)
            .bind(5, slice)
            .bind(6, entityTypeHint)
            .bind(7, state.persistenceId)
            .bind(8, previousRevision)
            .bind(9, slice)
            .bind(10, entityTypeHint)
            .bind(11, state.persistenceId)
            .bind(12, previousRevision)
        }
      }
    }

    result.map { updatedRows =>
      if (updatedRows != 1)
        throw new IllegalStateException(
          s"Update failed: durable state for persistence id [${state.persistenceId}] could not be updated to revision [${state.revision}]")
      else {
        log.debug("Updated durable state for persistenceId [{}] to revision [{}]", state.persistenceId, state.revision)
        Done
      }
    }
  }

  def deleteState(persistenceId: String): Future[Done] = {
    val entityTypeHint = SliceUtils.extractEntityTypeHintFromPersistenceId(persistenceId)
    val slice = SliceUtils.sliceForPersistenceId(persistenceId, settings.maxNumberOfSlices)

    val result =
      r2dbcExecutor.updateOne(s"delete [$persistenceId]") { connection =>
        connection
          .createStatement(deleteStateSql)
          .bind(0, slice)
          .bind(1, entityTypeHint)
          .bind(2, persistenceId)
      }

    if (log.isDebugEnabled())
      result.foreach(_ => log.debug("Deleted durable state for persistenceId [{}]", persistenceId))

    result.map(_ => Done)(ExecutionContext.parasitic)
  }

}
