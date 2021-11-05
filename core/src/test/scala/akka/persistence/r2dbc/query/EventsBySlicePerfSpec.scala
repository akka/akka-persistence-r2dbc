/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.NoOffset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpecLike

class EventsBySlicePerfSpec
    extends ScalaTestWithActorTestKit(TestConfig.backtrackingDisabledConfig.withFallback(TestConfig.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  s"EventsBySlices performance" should {

    "retrieve from several slices" in {
      // increase these properties for "real" testing
      val numberOfPersisters = 30
      val numberOfEvents = 5
      val writeConcurrency = 10
      val numberOfSliceRanges = 4
      val iterations = 3
      val totalNumberOfEvents = numberOfPersisters * numberOfEvents

      val entityType = nextEntityType()
      val probe = createTestProbe[Done]()
      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPid(entityType)).toVector
      var doneCount = 0
      val t0 = System.nanoTime()
      persistenceIds.zipWithIndex.foreach { case (pid, i) =>
        val ref = testKit.spawn(TestActors.Persister(pid))
        for (n <- 1 to numberOfEvents) {
          ref ! Persist(s"e-$n")
        }
        ref ! TestActors.Persister.Stop(probe.ref)
        if (i > writeConcurrency && i % writeConcurrency == 0) {
          // not too many at the same time
          probe.receiveMessages(writeConcurrency, 10.seconds)
          doneCount += writeConcurrency

          if (doneCount % 10 == 0)
            println(
              s"Persisting [${doneCount * numberOfEvents}] events from [$doneCount] persistent " +
              s"actors took [${(System.nanoTime() - t0) / 1000 / 1000}] ms")
        }
      }
      val remainingCount = persistenceIds.size - doneCount
      if (remainingCount > 0)
        probe.receiveMessages(remainingCount)

      println(
        s"Persisting all [$totalNumberOfEvents] events from [${persistenceIds.size}] persistent " +
        s"actors took [${(System.nanoTime() - t0) / 1000 / 1000}] ms")

      val ranges = query.sliceRanges(numberOfSliceRanges)

      (1 to iterations).foreach { iteration =>
        val t1 = System.nanoTime()
        val counts: Seq[Future[Int]] = ranges.map { range =>
          query
            .currentEventsBySlices(entityType, range.min, range.max, NoOffset)
            .runWith(Sink.fold(0) { case (acc, _) =>
              if (acc > 0 && acc % 100 == 0)
                println(s"#$iteration Reading [$acc] events from slices [${range.min}-${range.max}] " +
                s"took [${(System.nanoTime() - t1) / 1000 / 1000}] ms")
              acc + 1
            })
        }
        implicit val ec: ExecutionContext = testKit.system.executionContext
        val total = Await.result(Future.sequence(counts).map(_.sum), 30.seconds)
        total shouldBe totalNumberOfEvents
        println(
          s"#$iteration Reading all [$totalNumberOfEvents] events from [${ranges.size}] eventsBySlices " +
          s"took [${(System.nanoTime() - t1) / 1000 / 1000}] ms")
      }
    }

  }

}
