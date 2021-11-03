/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.snapshot

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.r2dbc.{ TestConfig, TestDbLifecycle }
import akka.persistence.snapshot.SnapshotStoreSpec

class R2dbcSnapshotStoreSpec extends SnapshotStoreSpec(TestConfig.config) with TestDbLifecycle {
  def typedSystem: ActorSystem[_] = system.toTyped

  protected override def supportsMetadata: CapabilityFlag = true
}
