/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import scala.annotation.varargs
import scala.collection.immutable.IntMap

import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.r2dbc.internal.codec.IdentityAdapter
import akka.persistence.r2dbc.internal.codec.QueryAdapter

/**
 * INTERNAL API: Utility to format SQL strings. Replaces `?` with numbered `\$1`, `\$2` for bind parameters. Trims
 * whitespace, including line breaks.
 */
@InternalStableApi
object Sql {

  /**
   * Scala string interpolation with `sql` prefix. Replaces `?` with numbered `\$1`, `\$2` for bind parameters. Use `??`
   * to include a literal `?`. Trims whitespace, including line breaks. Standard string interpolation arguments `$` can
   * be used.
   */
  implicit class Interpolation(val sc: StringContext) extends AnyVal {
    def sql(args: Any*): String =
      fillInParameterNumbers(trimLineBreaks(sc.s(args: _*)))
  }

  /**
   * INTERNAL API: Scala string interpolation with `sql` prefix. Replaces `?` with numbered `\$1`, `\$2` for bind
   * parameters. Use `??` to include a literal `?`. Trims whitespace, including line breaks. Standard string
   * interpolation arguments `$` can be used.
   */
  @InternalApi private[akka] implicit class InterpolationWithAdapter(val sc: StringContext)(implicit
      adapter: QueryAdapter)
      extends AnyRef {
    def sql(args: Any*): String =
      adapter(fillInParameterNumbers(trimLineBreaks(sc.s(args: _*))))
  }

  /**
   * Java API: Replaces `?` with numbered `\$1`, `\$2` for bind parameters. Use `??` to include a literal `?`. Trims
   * whitespace, including line breaks. The arguments are used like in [[java.lang.String.format]].
   */
  @varargs
  def format(sql: String, args: AnyRef*): String =
    fillInParameterNumbers(trimLineBreaks(sql.format(args)))

  private def fillInParameterNumbers(sql: String): String = {
    if (sql.indexOf('?') == -1) {
      sql
    } else {
      val sb = new java.lang.StringBuilder(sql.length + 10)
      var n = 0
      var i = 0
      def isNext(d: Char): Boolean = (i < sql.length - 1) && (sql.charAt(i + 1) == d)
      while (i < sql.length) {
        val c = sql.charAt(i)
        if (c == '?' && isNext('?')) {
          sb.append('?')
          i += 1 // advance past extra '?'
        } else if (c == '?') {
          n += 1
          sb.append('$').append(n)
        } else {
          sb.append(c)
        }
        i += 1
      }
      sb.toString
    }
  }

  private def trimLineBreaks(sql: String): String = {
    if (sql.indexOf('\n') == -1) {
      sql.trim
    } else {
      sql.trim.split('\n').map(_.trim).mkString(" ")
    }
  }

  object Cache {
    def apply(dataPartitionsEnabled: Boolean): Cache =
      if (dataPartitionsEnabled) new CacheBySlice
      else new CacheIgnoringSlice
  }

  sealed trait Cache {
    def get(slice: Int, key: Any)(orCreate: => String): String
  }

  private final class CacheIgnoringSlice extends Cache {
    private var entries: Map[Any, String] = Map.empty

    def get(slice: Int, key: Any)(orCreate: => String): String = {
      entries.get(key) match {
        case Some(value) => value
        case None        =>
          // it's just a cache so no need for guarding concurrent updates
          val entry = orCreate
          entries = entries.updated(key, entry)
          entry
      }
    }
  }

  private final class CacheBySlice extends Cache {
    private var entriesPerSlice: IntMap[Map[Any, String]] = IntMap.empty

    def get(slice: Int, key: Any)(orCreate: => String): String = {

      def createEntry(entries: Map[Any, String]): String = {
        // it's just a cache so no need for guarding concurrent updates
        val entry = orCreate
        val newEntries = entries.updated(key, entry)
        entriesPerSlice = entriesPerSlice.updated(slice, newEntries)
        entry
      }

      entriesPerSlice.get(slice) match {
        case Some(entries) =>
          entries.get(key) match {
            case Some(value) => value
            case None        => createEntry(entries)
          }
        case None =>
          createEntry(Map.empty)
      }
    }
  }

}
