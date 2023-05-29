/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.SnapshotSelectionCriteria
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.PayloadCodec
import akka.persistence.r2dbc.internal.PayloadCodec.RichRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.typed.PersistenceId
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
private[r2dbc] object SnapshotDao {

  final case class SerializedSnapshotRow(
      persistenceId: String,
      seqNr: Long,
      writeTimestamp: Long,
      snapshot: Array[Byte],
      serializerId: Int,
      serializerManifest: String,
      metadata: Option[SerializedSnapshotMetadata])

  final case class SerializedSnapshotMetadata(payload: Array[Byte], serializerId: Int, serializerManifest: String)

}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] trait SnapshotDao {
  import SnapshotDao._

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]]
  def store(serializedRow: SerializedSnapshotRow): Future[Unit]
  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]

}
