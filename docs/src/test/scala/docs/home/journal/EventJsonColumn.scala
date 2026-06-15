/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.home.journal

import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn
// #additional-column-json
import io.r2dbc.postgresql.codec.Json

class EventJsonColumn extends AdditionalColumn[BlogPost.Event, Json] {

  override val columnName: String = "event_json"

  override def bind(insert: AdditionalColumn.Insert[BlogPost.Event]): AdditionalColumn.Binding[Json] =
    insert.value match {
      case BlogPost.PostAdded(_, title, _) =>
        // a json library would be used here
        val jsonString = s"""{"title": "$title", "published": false}"""
        AdditionalColumn.BindValue(Json.of(jsonString))
      case _: BlogPost.Published =>
        val jsonString = """{"published": true}"""
        AdditionalColumn.BindValue(Json.of(jsonString))
      case _ =>
        AdditionalColumn.BindNull
    }
}
// #additional-column-json
