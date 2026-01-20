/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
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
      tags: Set[String],
      metadata: Option[SerializedSnapshotMetadata])
      extends BySliceQuery.SerializedRow {
    override def readDbTimestamp: Instant = dbTimestamp
    override def source: String = EnvelopeOrigin.SourceQuery
  }

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
  def sequenceNumberOfSnapshot(persistenceId: String): Future[Option[Long]]

}
