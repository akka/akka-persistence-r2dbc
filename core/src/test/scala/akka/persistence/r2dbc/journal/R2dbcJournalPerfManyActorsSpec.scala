/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import scala.concurrent.duration._

import akka.actor.Props
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalPerfSpec
import akka.persistence.journal.JournalPerfSpec.BenchActor
import akka.persistence.journal.JournalPerfSpec.Cmd
import akka.persistence.journal.JournalPerfSpec.ResetCounter
import akka.persistence.r2dbc.TestDbLifecycle
import akka.testkit.TestProbe

class R2dbcJournalPerfManyActorsSpec extends JournalPerfSpec(R2dbcJournalPerfSpec.config) with TestDbLifecycle {
  override def eventsCount: Int = 10

  override def measurementIterations: Int = 2 // increase when testing for real

  override def awaitDurationMillis: Long = 60.seconds.toMillis

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()

  override def typedSystem: ActorSystem[_] = system.toTyped

  def actorCount = 20 // increase when testing for real

  private val commands = Vector(1 to eventsCount: _*)

  "A PersistentActor's performance" must {

    if (r2dbcSettings.dialectName == "sqlserver") {
      pending
    }

    s"measure: persist()-ing $eventsCount events for $actorCount actors" in {
      val testProbe = TestProbe()
      val replyAfter = eventsCount
      def createBenchActor(actorNumber: Int) =
        system.actorOf(Props(classOf[BenchActor], s"$pid-$actorNumber", testProbe.ref, replyAfter))
      val actors = 1.to(actorCount).map(createBenchActor)

      measure(d => s"Persist()-ing $eventsCount * $actorCount took ${d.toMillis} ms") {
        for (cmd <- commands; actor <- actors) {
          actor ! Cmd("p", cmd)
        }
        for (_ <- actors) {
          testProbe.expectMsg(awaitDurationMillis.millis, commands.last)
        }
        for (actor <- actors) {
          actor ! ResetCounter
        }
      }
    }
  }
}
