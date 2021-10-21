/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.r2dbc

import java.util.concurrent.atomic.AtomicLong

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeHintCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeHintCounter

  def nextPid() = s"p-${pidCounter.incrementAndGet()}"
  // FIXME return PersistenceId instead
  def nextPid(entityTypeHint: String) = s"$entityTypeHint|p-${pidCounter.incrementAndGet()}"

  def nextEntityTypeHint() = s"TestEntity-${entityTypeHintCounter.incrementAndGet()}"

}
