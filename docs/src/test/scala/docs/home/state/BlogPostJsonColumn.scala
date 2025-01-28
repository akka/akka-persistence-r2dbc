/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.state

import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
// #additional-column-json
import io.r2dbc.postgresql.codec.Json

class BlogPostJsonColumn extends AdditionalColumn[BlogPost.State, Json] {

  override val columnName: String = "query_json"

  override def bind(upsert: AdditionalColumn.Upsert[BlogPost.State]): AdditionalColumn.Binding[Json] =
    upsert.value match {
      case s: BlogPost.DraftState =>
        // a json library would be used here
        val jsonString = s"""{"title": "${s.content.title}", "published": false}"""
        val json = Json.of(jsonString)
        AdditionalColumn.BindValue(json)
      case s: BlogPost.PublishedState =>
        // a json library would be used here
        val jsonString = s"""{"title": "${s.content.title}", "published": true}"""
        val json = Json.of(jsonString)
        AdditionalColumn.BindValue(json)
      case _ =>
        AdditionalColumn.Skip
    }
}
// #additional-column-json
