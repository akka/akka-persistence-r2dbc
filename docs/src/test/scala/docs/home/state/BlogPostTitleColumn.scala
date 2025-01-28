/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.state

// #additional-column
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn

// #additional-column

/* config:
// #additional-column-config
akka.persistence.r2dbc.state {
  additional-columns {
    "BlogPost" = ["docs.BlogPostTitleColumn"]
  }
  custom-table {
    "BlogPost" =  durable_state_blog_post
  }
}
// #additional-column-config
 */

// #additional-column
class BlogPostTitleColumn extends AdditionalColumn[BlogPost.State, String] {

  override val columnName: String = "title"

  override def bind(upsert: AdditionalColumn.Upsert[BlogPost.State]): AdditionalColumn.Binding[String] =
    upsert.value match {
      case BlogPost.BlankState =>
        AdditionalColumn.BindNull
      case s: BlogPost.DraftState =>
        AdditionalColumn.BindValue(s.content.title)
      case _: BlogPost.PublishedState =>
        AdditionalColumn.Skip
    }
}
// #additional-column
