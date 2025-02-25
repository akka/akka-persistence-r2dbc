/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.codec

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait QueryAdapter {
  def apply(query: String): String
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object IdentityAdapter extends QueryAdapter {
  override def apply(query: String): String = query
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SqlServerQueryAdapter extends QueryAdapter {
  /*
   * Convert a sqlserver query like
   *  `sql"select * from t where a=$1 and b=$2"`
   * into
   *  `select * from t where a=@p1 and b=@p2`
   * to make it compatible with the r2dbc sqlserver plugin.
   */
  override def apply(q: String): String = q.replace("$", "@p")
}
