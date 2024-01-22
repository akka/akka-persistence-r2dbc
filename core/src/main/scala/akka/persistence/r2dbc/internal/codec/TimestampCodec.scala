/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.codec

import java.time.Instant
import java.time.LocalDateTime
import java.util.TimeZone

import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.InstantFactory

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TimestampCodec {

  def encode(timestamp: Instant): Any
  def decode(row: Row, name: String): Instant
  def decode(row: Row, index: Int): Instant

  def instantNow(): Instant = InstantFactory.now()

  def now[T](): T
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimestampCodec {

  class PostgresTimestampCodec extends TimestampCodec {
    override def decode(row: Row, name: String): Instant = row.get(name, classOf[Instant])
    override def decode(row: Row, index: Int): Instant = row.get(index, classOf[Instant])

    override def encode(timestamp: Instant): Any = timestamp

    override def now[T](): T = instantNow().asInstanceOf[T]
  }
  object PostgresTimestampCodec extends PostgresTimestampCodec

  case object SqlServerTimestampCodec extends TimestampCodec {

    // should this come from config?
    private val zone = TimeZone.getTimeZone("UTC").toZoneId

    private def toInstant(timestamp: LocalDateTime) =
      timestamp.atZone(zone).toInstant

    override def decode(row: Row, name: String): Instant = toInstant(row.get(name, classOf[LocalDateTime]))

    override def encode(timestamp: Instant): LocalDateTime = LocalDateTime.ofInstant(timestamp, zone)

    override def now[T](): T = LocalDateTime.ofInstant(instantNow(), zone).asInstanceOf[T]

    override def decode(row: Row, index: Int): Instant = toInstant(row.get(index, classOf[LocalDateTime]))
  }

  object H2TimestampCodec extends PostgresTimestampCodec

  implicit class TimestampCodecRichStatement[T](val statement: Statement)(implicit codec: TimestampCodec)
      extends AnyRef {
    def bindTimestamp(name: String, timestamp: Instant): Statement = statement.bind(name, codec.encode(timestamp))
    def bindTimestamp(index: Int, timestamp: Instant): Statement = statement.bind(index, codec.encode(timestamp))
  }
  implicit class TimestampCodecRichRow[T](val row: Row)(implicit codec: TimestampCodec) extends AnyRef {
    def getTimestamp(index: String = "db_timestamp"): Instant = codec.decode(row, index)
  }
}
