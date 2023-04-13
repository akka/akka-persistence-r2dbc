/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.annotation.InternalApi
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait PayloadCodec {
  def payloadClass: Class[_]
  def encode(bytes: Array[Byte]): Any
  def decode(payload: Any): Array[Byte]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PayloadCodec {
  case object ByteArrayCodec extends PayloadCodec {
    override def payloadClass: Class[Array[Byte]] = classOf[Array[Byte]]
    override def encode(bytes: Array[Byte]): Array[Byte] = bytes
    override def decode(payload: Any): Array[Byte] = payload.asInstanceOf[Array[Byte]]
  }
  case object JsonCodec extends PayloadCodec {
    override def payloadClass: Class[Json] = classOf[Json]
    override def encode(bytes: Array[Byte]): Json = Json.of(bytes)
    override def decode(payload: Any): Array[Byte] =
      if (payload == null) null else payload.asInstanceOf[Json].asArray()
  }
  implicit class RichStatement(val statement: Statement)(implicit codec: PayloadCodec) extends AnyRef {
    def bindPayload(index: Int, payload: Array[Byte]): Statement = statement.bind(index, codec.encode(payload))
  }
  implicit class RichRow(val row: Row)(implicit codec: PayloadCodec) extends AnyRef {
    def getPayload(name: String): Array[Byte] = codec.decode(row.get(name, codec.payloadClass))
  }
}
