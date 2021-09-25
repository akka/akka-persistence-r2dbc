/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.javadsl

import akka.persistence.query.javadsl._
import akka.persistence.r2dbc.query.scaladsl

object R2dbcReadJournal {
  val Identifier = "akka.persistence.spanner.query"
}

final class R2dbcReadJournal(delegate: scaladsl.R2dbcReadJournal) extends ReadJournal {

  // FIXME eventsBySlices
}
