/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.home.journal;

// #additional-column
import akka.persistence.r2dbc.journal.javadsl.AdditionalColumn;

public class EventTitleColumn extends AdditionalColumn<BlogPost.Event, String> {
  @Override
  public Class<String> fieldClass() {
    return String.class;
  }

  @Override
  public String columnName() {
    return "title";
  }

  @Override
  public Binding<String> bind(Insert<BlogPost.Event> insert) {
    BlogPost.Event event = insert.value();
    if (event instanceof BlogPost.PostAdded) {
      BlogPost.PostAdded added = (BlogPost.PostAdded) event;
      return AdditionalColumn.bindValue(added.title);
    } else {
      return AdditionalColumn.bindNull();
    }
  }
}
// #additional-column
