/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
object CorrelationId {
  def toLogText(correlationid: Option[String]) = correlationid match {
    case Some(id) => s", correlation [$id]"
    case None     => ""
  }
}
