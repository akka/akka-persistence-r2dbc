/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
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
