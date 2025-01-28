/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import akka.annotation.ApiMayChange

@ApiMayChange
final class ChangeHandlerException(message: String, cause: Throwable) extends RuntimeException(message, cause)
