/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import akka.persistence.query.Offset

object TimestampOffset {
  val Zero: TimestampOffset = TimestampOffset(Instant.EPOCH, Map.empty)
}

/**
 * @param timestamp
 *   microsecond granularity database timestamp
 * @param seen
 *   List of sequence nrs for every persistence id seen at this timestamp
 */
final case class TimestampOffset(timestamp: Instant, seen: Map[String, Long]) extends Offset
