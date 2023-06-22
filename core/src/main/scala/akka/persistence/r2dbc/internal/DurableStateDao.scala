/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.Done
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Source

import java.time.Instant
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi private[r2dbc] object DurableStateDao {

  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedStateRow(
      persistenceId: String,
      revision: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Option[Array[Byte]],
      serId: Int,
      serManifest: String,
      tags: Set[String])
      extends BySliceQuery.SerializedRow {
    override def seqNr: Long = revision
    override def isPayloadDefined: Boolean = payload.isDefined
  }

}

/**
 * INTERNAL API
 *
 * Class for encapsulating db interaction.
 */
@InternalApi
private[r2dbc] trait DurableStateDao extends BySliceQuery.Dao[DurableStateDao.SerializedStateRow] {
  import DurableStateDao._

  def readState(persistenceId: String): Future[Option[SerializedStateRow]]

  def upsertState(state: SerializedStateRow, value: Any): Future[Done]

  def deleteState(persistenceId: String, revision: Long): Future[Done]

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed]

  def persistenceIds(afterId: Option[String], limit: Long, table: String): Source[String, NotUsed]

  def persistenceIds(entityType: String, afterId: Option[String], limit: Long): Source[String, NotUsed]

}
