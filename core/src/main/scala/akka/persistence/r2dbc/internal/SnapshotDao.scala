/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant

import scala.concurrent.Future

import akka.annotation.InternalApi
import akka.persistence.SnapshotSelectionCriteria

/**
 * INTERNAL API
 */
private[r2dbc] object SnapshotDao {
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedSnapshotRow(
      slice: Int,
      entityType: String,
      persistenceId: String,
      seqNr: Long,
      dbTimestamp: Instant,
      writeTimestamp: Long,
      snapshot: Array[Byte],
      serializerId: Int,
      serializerManifest: String,
      metadata: Option[SerializedSnapshotMetadata])
      extends BySliceQuery.SerializedRow {
    override def readDbTimestamp: Instant = dbTimestamp
  }

  final case class SerializedSnapshotMetadata(payload: Array[Byte], serializerId: Int, serializerManifest: String)

}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] trait SnapshotDao extends BySliceQuery.Dao[SnapshotDao.SerializedSnapshotRow] {
  import SnapshotDao._

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]]
  def store(serializedRow: SerializedSnapshotRow): Future[Unit]
  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]

}
