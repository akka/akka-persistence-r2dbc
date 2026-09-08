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

  // Every implementation is backed by an R2dbcExecutorProvider with its own dedicated ec (e.g. H2's blocking-io
  // dispatcher), so this must come from the implementation rather than be left to whatever ec happens to be
  // implicit at each call site.
  protected implicit def ec: ExecutionContext

  def load(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SerializedSnapshotRow]]
  def store(serializedRow: SerializedSnapshotRow): Future[Unit]
  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
  def sequenceNumberOfSnapshot(persistenceId: String): Future[Option[Long]]

  /**
   * Batched variant of `sequenceNumberOfSnapshot`, used by the start-from-snapshot filtering stage to resolve many
   * persistence ids with a single round-trip instead of one round-trip per persistence id. Default implementation
   * delegates to [[sequenceNumbersOfSnapshotsConcurrently]]; dialects that support an efficient `= ANY(...)`/`IN (...)`
   * batched query should override this instead.
   *
   * Only returns entries for persistence ids that actually have a snapshot; ids without one are absent from the result
   * map.
   */
  def sequenceNumbersOfSnapshots(persistenceIds: Set[String]): Future[Map[String, Long]] =
    sequenceNumbersOfSnapshotsConcurrently(persistenceIds)

  /**
   * Fallback for dialects without an efficient batched lookup: resolves each persistence id concurrently with its own
   * round-trip. Exposed so a dialect that overrides `sequenceNumbersOfSnapshots` further down the class hierarchy (for
   * example a subclass of a dialect that does support batching) can opt back into this instead.
   */
  protected def sequenceNumbersOfSnapshotsConcurrently(persistenceIds: Set[String]): Future[Map[String, Long]] =
    Future
      .traverse(persistenceIds)(pid => sequenceNumberOfSnapshot(pid).map(pid -> _))
      .map(_.collect { case (pid, Some(seqNr)) => pid -> seqNr }.toMap)

}
