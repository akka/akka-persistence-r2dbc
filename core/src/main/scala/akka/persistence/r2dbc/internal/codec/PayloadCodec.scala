/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.nio.charset.StandardCharsets.UTF_8

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
  def nonePayload: Array[Byte]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object PayloadCodec {
  case object ByteArrayCodec extends PayloadCodec {
    override def payloadClass: Class[Array[Byte]] = classOf[Array[Byte]]
    override def encode(bytes: Array[Byte]): Array[Byte] = bytes
    override def decode(payload: Any): Array[Byte] = payload.asInstanceOf[Array[Byte]]
    override def nonePayload: Array[Byte] = Array.emptyByteArray
  }
  case object JsonCodec extends PayloadCodec {
    override def payloadClass: Class[Json] = classOf[Json]
    override def encode(bytes: Array[Byte]): Json = Json.of(bytes)
    override def decode(payload: Any): Array[Byte] =
      if (payload == null) null else payload.asInstanceOf[Json].asArray()
    override val nonePayload: Array[Byte] = "{}".getBytes(UTF_8)
  }
  implicit class RichStatement(val statement: Statement)(implicit codec: PayloadCodec) extends AnyRef {
    def bindPayload(index: Int, payload: Array[Byte]): Statement =
      statement.bind(index, codec.encode(payload))

    def bindPayload(name: String, payload: Array[Byte]): Statement =
      statement.bind(name, codec.encode(payload))

    def bindPayloadOption(index: Int, payloadOption: Option[Array[Byte]]): Statement =
      payloadOption match {
        case Some(payload) => bindPayload(index, payload)
        case None          => bindPayload(index, codec.nonePayload)
      }

    def bindPayloadOption(name: String, payloadOption: Option[Array[Byte]]): Statement =
      payloadOption match {
        case Some(payload) => bindPayload(name, payload)
        case None          => bindPayload(name, codec.nonePayload)
      }
  }
  implicit class RichRow(val row: Row)(implicit codec: PayloadCodec) extends AnyRef {
    def getPayload(name: String): Array[Byte] = codec.decode(row.get(name, codec.payloadClass))
  }
}
