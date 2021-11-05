/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.scaladsl

import java.time.Instant

import scala.concurrent.Future

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 */
trait EventTimestampQuery {

  def timestampOf(entityType: String, persistenceId: String, slice: Int, sequenceNumber: Long): Future[Option[Instant]]

}
