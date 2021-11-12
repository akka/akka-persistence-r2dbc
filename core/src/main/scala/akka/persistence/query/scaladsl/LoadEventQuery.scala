/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.scaladsl

import scala.concurrent.Future

import akka.persistence.query.EventBySliceEnvelope

/**
 * [[EventsBySliceQuery]] that is using a timestamp based offset should also implement this query.
 */
trait LoadEventQuery {

  def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[Option[EventBySliceEnvelope[Event]]]
}
