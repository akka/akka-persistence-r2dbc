/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.codec

import com.typesafe.config.Config
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TagsCodec {
  def tagsClass: Class[_]
  def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement
  def bindTags(statement: Statement, name: String, tags: Set[String]): Statement
  def getTags(row: Row, column: String): Set[String]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TagsCodec {
  class PostgresTagsCodec extends TagsCodec {
    override def tagsClass: Class[Array[String]] = classOf[Array[String]]

    override def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement =
      statement.bind(index, tags.toArray)

    override def bindTags(statement: Statement, name: String, tags: Set[String]): Statement =
      statement.bind(name, tags.toArray)

    def getTags(row: Row, column: String): Set[String] = row.get(column, classOf[Array[String]]) match {
      case null      => Set.empty[String]
      case tagsArray => tagsArray.toSet
    }
  }
  case object PostgresTagsCodec extends PostgresTagsCodec

  class SqlServerTagsCodec(connectionFactoryConfig: Config) extends TagsCodec {

    private val tagSeparator: Char = {
      val tagStr = connectionFactoryConfig.getString("tag-separator")
      require(tagStr.length == 1, s"Tag separator '$tagSeparator' must be a single character.")
      tagStr.charAt(0)
    }

    override def tagsClass: Class[String] = classOf[String]
    override def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement =
      statement.bind(index, tags.mkString(","))

    override def bindTags(statement: Statement, name: String, tags: Set[String]): Statement =
      statement.bind(name, tags.mkString(","))

    override def getTags(row: Row, column: String): Set[String] = row.get(column, classOf[String]) match {
      case null    => Set.empty[String]
      case entries => entries.split(tagSeparator).toSet
    }
  }

  object H2TagsCodec extends PostgresTagsCodec {
    // needs to be picked up with Object event though it is an Array[String]
    // https://github.com/r2dbc/r2dbc-h2/issues/208
    override def getTags(row: Row, column: String): Set[String] = row.get(column, classOf[AnyRef]) match {
      case null              => Set.empty[String]
      case entries: Array[_] => entries.toSet.asInstanceOf[Set[String]]
    }
  }

  implicit class TagsCodecRichStatement(val statement: Statement)(implicit codec: TagsCodec) extends AnyRef {
    def bindTagsNull(index: String): Statement = statement.bindNull(index, codec.tagsClass)
    def bindTagsNull(index: Int): Statement = statement.bindNull(index, codec.tagsClass)
    def bindTags(index: Int, tags: Set[String]): Statement = codec.bindTags(statement, index, tags)
    def bindTags(name: String, tags: Set[String]): Statement = codec.bindTags(statement, name, tags)
  }

  implicit class TagsCodecRichRow(val row: Row)(implicit codec: TagsCodec) extends AnyRef {
    def getTags(column: String): Set[String] = codec.getTags(row, column)
  }
}
