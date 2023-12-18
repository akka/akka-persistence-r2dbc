/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.InstantFactory
import com.typesafe.config.Config
import io.r2dbc.spi.Row

import java.time.Instant
import java.time.LocalDateTime
import java.util.TimeZone

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerDialectHelper {
  def apply(config: Config) = new SqlServerDialectHelper(config)
}

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class SqlServerDialectHelper(config: Config) {

  private val tagSeparator = config.getString("tag-separator")

  require(tagSeparator.length == 1, s"Tag separator '$tagSeparator' must be a single character.")

  def tagsToDb(tags: Set[String]): String = {
    if (tags.exists(_.contains(tagSeparator))) {
      throw new IllegalArgumentException(
        s"A tag in [$tags] contains the character '$tagSeparator' which is reserved. Please change `akka.persistence.r2dbc.sqlserver.tag-separator` to a character that is not contained by any of your tags.")
    }
    tags.mkString(tagSeparator)
  }

  def tagsFromDb(row: Row): Set[String] = row.get("tags", classOf[String]) match {
    case null    => Set.empty[String]
    case entries => entries.split(tagSeparator).toSet
  }

  private val zone = TimeZone.getTimeZone("UTC").toZoneId

  def nowInstant(): Instant = InstantFactory.now()

  def nowLocalDateTime(): LocalDateTime = LocalDateTime.ofInstant(nowInstant(), zone)

  def toDbTimestamp(timestamp: Instant): LocalDateTime =
    LocalDateTime.ofInstant(timestamp, zone)

  def fromDbTimestamp(time: LocalDateTime): Instant = time
    .atZone(zone)
    .toInstant

}
