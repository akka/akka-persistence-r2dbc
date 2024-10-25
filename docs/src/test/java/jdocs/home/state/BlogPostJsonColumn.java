/*
 * Copyright (C) 2022 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home.state;

import akka.persistence.r2dbc.state.javadsl.AdditionalColumn;
// #additional-column-json
import io.r2dbc.postgresql.codec.Json;

public class BlogPostJsonColumn extends AdditionalColumn<BlogPost.State, Json> {
  @Override
  public Class<Json> fieldClass() {
    return Json.class;
  }

  @Override
  public String columnName() {
    return "query_json";
  }

  @Override
  public Binding<Json> bind(Upsert<BlogPost.State> upsert) {
    BlogPost.State state = upsert.value();
    if (state instanceof BlogPost.DraftState) {
      BlogPost.DraftState s = (BlogPost.DraftState) state;
      // a json library would be used here
      String jsonString = "{\"title\": \"" + s.content.title + "\", \"published\": false}";
      Json json = Json.of(jsonString);
      return AdditionalColumn.bindValue(json);
    } else if (state instanceof BlogPost.PublishedState) {
        BlogPost.PublishedState s = (BlogPost.PublishedState) state;
        // a json library would be used here
        String jsonString = "{\"title\": \"" + s.content.title + "\", \"published\": true}";
        Json json = Json.of(jsonString);
        return AdditionalColumn.bindValue(json);
    } else {
      return AdditionalColumn.skip();
    }
  }
}
// #additional-column-json
