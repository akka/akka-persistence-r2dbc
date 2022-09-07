/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc

import java.util.concurrent.atomic.AtomicLong

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeCounter

  def nextPid() = s"p-${pidCounter.incrementAndGet()}"
  // FIXME return PersistenceId instead
  def nextPid(entityType: String) = s"$entityType|p-${pidCounter.incrementAndGet()}"

  def nextEntityType() = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

}
