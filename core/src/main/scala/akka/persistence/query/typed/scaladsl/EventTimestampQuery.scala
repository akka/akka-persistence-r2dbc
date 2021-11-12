/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.scaladsl

import java.time.Instant

import scala.concurrent.Future

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 */
trait EventTimestampQuery {

  def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]]

}
