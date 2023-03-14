/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import akka.persistence.r2dbc.TestDbLifecycle

object R2dbcJournalPerfSpec {
  val config = R2dbcJournalSpec.testConfig()
}

class R2dbcJournalPerfSpec extends JournalPerfSpec(R2dbcJournalPerfSpec.config) with TestDbLifecycle {
  override def eventsCount: Int = 200

  override def measurementIterations: Int = 2 // increase when testing for real

  override def awaitDurationMillis: Long = 60.seconds.toMillis

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()

  override def typedSystem: ActorSystem[_] = system.toTyped
}
