/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.h2

import akka.annotation.InternalApi
import io.r2dbc.spi.Row

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object H2Utils {

  def tagsFromDb(row: Row, columnName: String): Set[String] = {
    // needs to be picked up with Object event though it is an Array[String]
    row.get(columnName, classOf[AnyRef]) match {
      case null              => Set.empty[String]
      case entries: Array[_] => entries.toSet.asInstanceOf[Set[String]]
    }
  }

}
