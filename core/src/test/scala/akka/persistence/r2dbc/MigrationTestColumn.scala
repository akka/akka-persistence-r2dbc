/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn

class MigrationTestColumn extends AdditionalColumn[Any, String] {

  override val columnName: String = "test_column"

  override def bind(upsert: AdditionalColumn.Upsert[Any]): AdditionalColumn.Binding[String] = {
    if (upsert.value.asInstanceOf[String] == "s-column") {
      AdditionalColumn.BindValue("my value")
    } else AdditionalColumn.Skip
  }
}
