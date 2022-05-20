/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.nio.ByteBuffer
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

sealed trait PayloadCodec {
  def encode(bytes: Array[Byte]): AnyRef
  def decode(payload: AnyRef): Array[Byte]
}
object PayloadCodec {
  case object ByteArrayCodec extends PayloadCodec {
    override def encode(bytes: Array[Byte]): Array[Byte] = bytes
    override def decode(payload: AnyRef): Array[Byte] = payload.asInstanceOf[ByteBuffer].array()
  }
  case object JsonCodec extends PayloadCodec {
    override def encode(bytes: Array[Byte]): Json = Json.of(bytes)
    override def decode(payload: AnyRef): Array[Byte] = payload.asInstanceOf[Json].asArray()
  }
  implicit class RichStatement(val statement: Statement)(implicit codec: PayloadCodec) extends AnyRef {
    def bindPayload(index: Int, payload: Array[Byte]): Statement = statement.bind(index, codec.encode(payload))
  }
  implicit class RichRow(val row: Row)(implicit codec: PayloadCodec) extends AnyRef {
    def getPayload(name: String): Array[Byte] = codec.decode(row.get(name))
  }
}
