/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import scala.util.control.NonFatal

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.query.TimestampOffset
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API: Serialized TimestampOffset is used by Akka Projections
 */
@InternalApi private[akka] class R2dbcSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {
  private val TimestampOffsetManifest = "a"

  // persistenceId and commitTimestamp must not contain this separator char
  private val separator = ';'

  override def manifest(o: AnyRef): String = o match {
    case _: TimestampOffset => TimestampOffsetManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case o: TimestampOffset => offsetToBinary(o)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  private def offsetToBinary(offset: TimestampOffset): Array[Byte] = {
    val str = new java.lang.StringBuilder
    str.append(offset.timestamp)
    if (offset.seen.size == 1) {
      // optimized for the normal case
      val pid = offset.seen.head._1
      val seqNr = offset.seen.head._2
      str.append(separator).append(pid).append(separator).append(seqNr)
    } else if (offset.seen.nonEmpty) {
      offset.seen.toList.sortBy(_._1).foreach { case (pid, seqNr) =>
        str.append(separator).append(pid).append(separator).append(seqNr)
      }
    }
    str.toString.getBytes(UTF_8)
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case TimestampOffsetManifest => offsetFromBinary(bytes)
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

  private def offsetFromBinary(bytes: Array[Byte]): TimestampOffset = {
    val str = new String(bytes, UTF_8)
    try {
      val parts = str.split(separator)
      val timestamp = Instant.parse(parts(0))
      if (parts.length == 3) {
        // optimized for the normal case
        TimestampOffset(timestamp, Map(parts(1) -> parts(2).toLong))
      } else if (parts.length == 1) {
        TimestampOffset(timestamp, Map.empty)
      } else {
        val seen = parts.toList
          .drop(1)
          .grouped(2)
          .map {
            case pid :: seqNr :: Nil => pid -> seqNr.toLong
            case _ =>
              throw new IllegalArgumentException(
                s"Invalid representation of Map(pid -> seqNr) [${parts.toList.drop(1).mkString(",")}]")
          }
          .toMap
        TimestampOffset(timestamp, seen)
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Unexpected serialized offset format [$str].", e)
    }
  }
}
