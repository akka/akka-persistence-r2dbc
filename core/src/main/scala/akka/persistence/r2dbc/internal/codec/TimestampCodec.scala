/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal.codec

import java.time.Instant

import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TimestampCodec {

  def encode(timestamp: Instant): Any
  def decode(row: Row, name: String): Instant
  def decode(row: Row, index: Int): Instant

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimestampCodec {

  class PostgresTimestampCodec extends TimestampCodec {
    override def decode(row: Row, name: String): Instant = row.get(name, classOf[Instant])
    override def decode(row: Row, index: Int): Instant = row.get(index, classOf[Instant])

    override def encode(timestamp: Instant): Any = timestamp
  }
  object PostgresTimestampCodec extends PostgresTimestampCodec

  object H2TimestampCodec extends PostgresTimestampCodec

  implicit class TimestampCodecRichStatement[T](val statement: Statement)(implicit codec: TimestampCodec)
      extends AnyRef {
    def bindTimestamp(name: String, timestamp: Instant): Statement = statement.bind(name, codec.encode(timestamp))
    def bindTimestamp(index: Int, timestamp: Instant): Statement = statement.bind(index, codec.encode(timestamp))
  }
  implicit class TimestampCodecRichRow[T](val row: Row)(implicit codec: TimestampCodec) extends AnyRef {
    def getTimestamp(index: String): Instant = codec.decode(row, index)
  }
}
