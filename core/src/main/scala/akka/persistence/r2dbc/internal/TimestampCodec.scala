/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.nio.charset.StandardCharsets.UTF_8
import akka.annotation.InternalApi
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import java.time.{ Instant, LocalDateTime }
import java.util.TimeZone

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TimestampCodec {

  def encode(timestamp: Instant): Any
  def decode(row: Row, name: String): Instant

  protected def instantNow() = InstantFactory.now()

  def now[T](): T
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimestampCodec {
  case object PostgresTimestampCodec extends TimestampCodec {
    override def decode(row: Row, name: String): Instant = row.get(name, classOf[Instant])

    override def encode(timestamp: Instant): Any = timestamp

    override def now[T](): T = instantNow().asInstanceOf[T]
  }

  case object SqlServerCodec extends TimestampCodec {

    // should this come from config?
    private val zone = TimeZone.getTimeZone("UTC").toZoneId

    override def decode(row: Row, name: String): Instant = {
      row
        .get(name, classOf[LocalDateTime])
        .atZone(zone)
        .toInstant
    }

    override def encode(timestamp: Instant): Any = LocalDateTime.ofInstant(timestamp, zone)

    override def now[T](): T = LocalDateTime.ofInstant(instantNow(), zone).asInstanceOf[T]
  }

  implicit class RichStatement[T](val statement: Statement)(implicit codec: TimestampCodec) extends AnyRef {
    def bindTimestamp(name: String, timestamp: Instant): Statement = statement.bind(name, codec.encode(timestamp))
    def bindTimestamp(index: Int, timestamp: Instant): Statement = statement.bind(index, codec.encode(timestamp))
  }
  implicit class RichRow[T](val row: Row)(implicit codec: TimestampCodec) extends AnyRef {
    def getTimestamp(rowName: String = "db_timestamp"): Instant = codec.decode(row, rowName)
  }
}
