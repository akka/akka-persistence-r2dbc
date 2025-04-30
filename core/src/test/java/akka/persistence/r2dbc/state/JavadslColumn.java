/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state;

import akka.persistence.r2dbc.state.javadsl.AdditionalColumn;

public class JavadslColumn extends AdditionalColumn<String, Integer> {
  @Override
  public Class<Integer> fieldClass() {
    return Integer.class;
  }

  @Override
  public String columnName() {
    return "col3";
  }

  @Override
  public Binding<Integer> bind(Upsert<String> upsert) {
    if (upsert.value().isEmpty()) return AdditionalColumn.bindNull();
    else if (upsert.value().equals("SKIP")) return AdditionalColumn.skip();
    else return new AdditionalColumn.BindValue(upsert.value().length());
  }
}
