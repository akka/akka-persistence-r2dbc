/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.home.state;

// #additional-column
import akka.persistence.r2dbc.state.javadsl.AdditionalColumn;

public class BlogPostTitleColumn extends AdditionalColumn<BlogPost.State, String> {
  @Override
  public Class<String> fieldClass() {
    return String.class;
  }

  @Override
  public String columnName() {
    return "title";
  }

  @Override
  public Binding<String> bind(Upsert<BlogPost.State> upsert) {
    BlogPost.State state = upsert.value();
    if (state.equals(BlogPost.BlankState.INSTANCE)) {
      return AdditionalColumn.bindNull();
    } else if (state instanceof BlogPost.DraftState) {
      BlogPost.DraftState draft = (BlogPost.DraftState) state;
      return AdditionalColumn.bindValue(draft.content.title);
    } else {
      return AdditionalColumn.skip();
    }
  }
}
// #additional-column
