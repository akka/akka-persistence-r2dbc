/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.annotation.InternalApi

import java.time.Instant
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object JournalDao {

  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedJournalRow(
      slice: Int,
      entityType: String,
      persistenceId: String,
      seqNr: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Option[Array[Byte]],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      tags: Set[String],
      metadata: Option[SerializedEventMetadata])
      extends BySliceQuery.SerializedRow {
    override def isPayloadDefined: Boolean = payload.isDefined
  }

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] trait JournalDao {

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be the `db_timestamp` column and it is used in published events when that feature is
   * enabled.
   *
   * Note for implementing future database dialects: If a database dialect can't efficiently return the timestamp column
   * it can return `JournalDao.EmptyDbTimestamp` when the pub-sub feature is disabled. When enabled it would have to use
   * a select (in same transaction).
   */
  def writeEvents(events: Seq[JournalDao.SerializedJournalRow]): Future[Instant]
  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long]

  def readLowestSequenceNr(persistenceId: String): Future[Long]

  def deleteEventsTo(persistenceId: String, toSequenceNr: Long, resetSequenceNumber: Boolean): Future[Unit]

}
