/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant

import scala.concurrent.ExecutionContext
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

  /**
   * SKETCH / PROTOTYPE, see BatchingStartingFromSnapshotStage.
   *
   * Batched variant of `sequenceNumberOfSnapshot`, used by the start-from-snapshot filtering stage to resolve many
   * persistence ids with a single round-trip instead of one round-trip per persistence id. Default implementation falls
   * back to concurrent single-id lookups so existing dialects (H2, SQL Server) keep working without changes; dialects
   * that support an efficient `= ANY(...)`/`IN (...)` batched query should override this.
   *
   * Only returns entries for persistence ids that actually have a snapshot; ids without one are absent from the result
   * map.
   */
  def sequenceNumbersOfSnapshots(persistenceIds: Set[String])(implicit
      ec: ExecutionContext): Future[Map[String, Long]] =
    Future
      .traverse(persistenceIds)(pid => sequenceNumberOfSnapshot(pid).map(pid -> _))
      .map(_.collect { case (pid, Some(seqNr)) => pid -> seqNr }.toMap)

}
