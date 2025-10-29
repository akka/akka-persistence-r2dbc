/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.r2dbc.internal

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object InstantFactory {

  /**
   * Current time truncated to microseconds. The reason for using microseconds is that Postgres timestamps has the
   * resolution of microseconds but some OS/JDK (Linux/JDK17) has Instant resolution of nanoseconds.
   */
  def now(): Instant =
    Instant.now().truncatedTo(ChronoUnit.MICROS)

}
