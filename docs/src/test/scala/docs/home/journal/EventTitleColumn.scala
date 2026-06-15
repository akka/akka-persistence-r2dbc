/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.home.journal

// #additional-column
import akka.persistence.r2dbc.journal.scaladsl.AdditionalColumn

// #additional-column

/* config:
// #additional-column-config
akka.persistence.r2dbc.journal {
  additional-columns {
    "BlogPost" = ["docs.home.journal.EventTitleColumn"]
  }
  custom-table {
    "BlogPost" = event_journal_blogpost
  }
}
// #additional-column-config
 */

// #additional-column
class EventTitleColumn extends AdditionalColumn[BlogPost.Event, String] {

  override val columnName: String = "title"

  override def bind(insert: AdditionalColumn.Insert[BlogPost.Event]): AdditionalColumn.Binding[String] =
    insert.value match {
      case BlogPost.PostAdded(_, title, _) => AdditionalColumn.BindValue(title)
      case _                               => AdditionalColumn.BindNull
    }
}
// #additional-column
