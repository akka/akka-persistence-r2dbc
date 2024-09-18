/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.annotation.InternalStableApi
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 */
@InternalStableApi private[akka] object EnvelopeOrigin {
  val SourceQuery = ""
  val SourceBacktracking = "BT"
  val SourcePubSub = "PS"
  val SourceSnapshot = "SN"
  val SourceHeartbeat = "HB"

  def fromQuery(env: EventEnvelope[_]): Boolean =
    env.source == SourceQuery

  def fromBacktracking(env: EventEnvelope[_]): Boolean =
    env.source == SourceBacktracking

  def fromBacktracking(change: UpdatedDurableState[_]): Boolean =
    change.value == null

  def fromPubSub(env: EventEnvelope[_]): Boolean =
    env.source == SourcePubSub

  def fromSnapshot(env: EventEnvelope[_]): Boolean =
    env.source == SourceSnapshot

  def fromHeartbeat(env: EventEnvelope[_]): Boolean =
    env.source == SourceHeartbeat

  def isFilteredEvent(env: Any): Boolean =
    env match {
      case e: EventEnvelope[_] => e.filtered
      case _                   => false
    }

}
