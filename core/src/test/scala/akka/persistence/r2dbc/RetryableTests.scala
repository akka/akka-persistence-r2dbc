/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import org.scalatest.Outcome
import org.scalatest.Retries
import org.scalatest.TestSuite

trait RetryableTests extends TestSuite with Retries {
  override def withFixture(test: NoArgTest): Outcome = {
    if (isRetryable(test)) withRetryOnFailure { super.withFixture(test) }
    else super.withFixture(test)
  }
}
