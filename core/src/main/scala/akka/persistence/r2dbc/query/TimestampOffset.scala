/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset

object TimestampOffset {
  val Zero: TimestampOffset = TimestampOffset(Instant.EPOCH, Instant.EPOCH, Map.empty)

  def apply(timestamp: Instant, seen: Map[String, Long]): TimestampOffset =
    TimestampOffset(timestamp, Instant.EPOCH, seen)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def toTimestampOffset(offset: Offset): TimestampOffset = {
    offset match {
      case t: TimestampOffset => t
      case NoOffset           => TimestampOffset.Zero
      case null               => throw new IllegalArgumentException("Offset must not be null")
      case other =>
        throw new IllegalArgumentException(
          s"Supported offset types are TimestampOffset and NoOffset, " +
          s"received ${other.getClass.getName}")
    }
  }
}

/**
 * @param timestamp
 *   time when the event was stored, microsecond granularity database timestamp
 * @param readTimestamp
 *   time when the event was read, microsecond granularity database timestamp
 * @param seen
 *   List of sequence nrs for every persistence id seen at this timestamp
 */
final case class TimestampOffset(timestamp: Instant, readTimestamp: Instant, seen: Map[String, Long]) extends Offset
