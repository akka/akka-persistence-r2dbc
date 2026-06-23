/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.home.journal;

import akka.persistence.r2dbc.journal.javadsl.AdditionalColumn;
// #additional-column-json
import io.r2dbc.postgresql.codec.Json;

public class EventJsonColumn extends AdditionalColumn<BlogPost.Event, Json> {
  @Override
  public Class<Json> fieldClass() {
    return Json.class;
  }

  @Override
  public String columnName() {
    return "event_json";
  }

  @Override
  public Binding<Json> bind(Insert<BlogPost.Event> insert) {
    BlogPost.Event event = insert.value();
    if (event instanceof BlogPost.PostAdded) {
      BlogPost.PostAdded added = (BlogPost.PostAdded) event;
      // a json library would be used here
      String jsonString = "{\"title\": \"" + added.title + "\", \"published\": false}";
      return AdditionalColumn.bindValue(Json.of(jsonString));
    } else if (event instanceof BlogPost.Published) {
      String jsonString = "{\"published\": true}";
      return AdditionalColumn.bindValue(Json.of(jsonString));
    } else {
      return AdditionalColumn.bindNull();
    }
  }
}
// #additional-column-json
