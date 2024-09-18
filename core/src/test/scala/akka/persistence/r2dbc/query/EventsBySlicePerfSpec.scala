/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.persistence.query.TimestampOffset
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySlicePerfSpec {
  private val config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.journal.publish-events = on
    akka.persistence.r2dbc.query {
      backtracking.enabled = on
      refresh-interval = 3s
      #buffer-size = 100
    }
    # to measure lag latency more accurately
    akka.persistence.r2dbc.use-app-timestamp = true
    """)
    .withFallback(TestConfig.config)

  private final case class PidSeqNr(pid: String, seqNr: Long)
}

class EventsBySlicePerfSpec
    extends ScalaTestWithActorTestKit(EventsBySlicePerfSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with LogCapturing
    with TestData {
  import EventsBySlicePerfSpec.PidSeqNr

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  s"EventsBySlices performance" should {

    if (settings.dialectName == "sqlserver") {
      pending
    }

    "retrieve from several slices" in {
      // increase these properties for "real" testing
      // also, remove LogCapturing and change logback log levels for "real" testing
      val numberOfPersisters = 30
      val numberOfEvents = 5
      val writeConcurrency = 10
      val numberOfSliceRanges = 4
      val iterations = 2
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
            .currentEventsBySlices[String](entityType, range.min, range.max, NoOffset)
            .runWith(Sink.fold(0) { case (acc, env) =>
              if (EnvelopeOrigin.fromQuery(env)) {
                if (acc > 0 && acc % 100 == 0)
                  println(s"#$iteration Reading [$acc] events from slices [${range.min}-${range.max}] " +
                  s"took [${(System.nanoTime() - t1) / 1000 / 1000}] ms")
                acc + 1
              } else {
                acc
              }
            })
        }
        implicit val ec: ExecutionContext = testKit.system.executionContext
        val total = Await.result(Future.sequence(counts).map(_.sum), 30.seconds)
        total shouldBe totalNumberOfEvents
        println(
          s"#$iteration Reading all [$totalNumberOfEvents] events from [${ranges.size}] slices with currentEventsBySlices " +
          s"took [${(System.nanoTime() - t1) / 1000 / 1000}] ms")
      }
    }

    "write and read concurrently" in {
      pendingIfMoreThanOneDataPartition()

      // increase these properties for "real" testing
      // also, remove LogCapturing and change logback log levels for "real" testing
      val numberOfEventsPerWriter = 20
      val writeConcurrency = 10
      val writeRps = 300
      val iterations = 2
      val totalNumberOfEvents = writeConcurrency * numberOfEventsPerWriter
      val verbosePrintLag = false

      implicit val ec: ExecutionContext = testKit.system.executionContext

      val entityType = nextEntityType()
      val persistenceIds = (1 to writeConcurrency).map(_ => nextPid(entityType)).toVector

      (1 to iterations).foreach { iteration =>
        val t0 = System.nanoTime()
        val writeProbe = createTestProbe[Done]()
        val persisters = persistenceIds.map(pid => testKit.spawn(TestActors.Persister(pid)))
        Source(1 to numberOfEventsPerWriter)
          .mapConcat(n => persisters.map(ref => ref -> n))
          .throttle(writeRps / 10, 100.millis)
          .map { case (ref, n) =>
            ref ! Persist(s"e-$n")
          }
          .runWith(Sink.ignore)
          .foreach { _ =>
            // stop them at the end
            persisters.foreach(_ ! TestActors.Persister.Stop(writeProbe.ref))
          }

        val done: Future[Done] =
          query
            .eventsBySlices[String](entityType, 0, persistenceExt.numberOfSlices - 1, NoOffset)
            .filterNot(EnvelopeOrigin.fromHeartbeat)
            .scan(Set.empty[PidSeqNr]) { case (acc, env) =>
              val newAcc = acc + PidSeqNr(env.persistenceId, env.sequenceNr)

              if (verbosePrintLag) {
                val duplicate = if (newAcc.size == acc.size) " (duplicate)" else ""
                val lagMillis = System.currentTimeMillis() - env.timestamp
                val delayed =
                  (EnvelopeOrigin.fromPubSub(env) && lagMillis > 50) ||
                  (EnvelopeOrigin.fromQuery(
                    env) && lagMillis > settings.querySettings.refreshInterval.toMillis + 300) ||
                  (EnvelopeOrigin.fromPubSub(
                    env) && lagMillis > settings.querySettings.backtrackingWindow.toMillis / 2 + 300)
                if (delayed)
                  println(
                    s"# received ${newAcc.size}$duplicate from ${env.source}: ${env.persistenceId} seqNr ${env.sequenceNr}, lag $lagMillis ms")
              }

              if (newAcc.size != acc.size && (newAcc.size % 100 == 0))
                println(s"#$iteration Reading [${newAcc.size}] events " +
                s"took [${(System.nanoTime() - t0) / 1000 / 1000}] ms")
              newAcc

            }
            .takeWhile(_.size < totalNumberOfEvents)
            .runWith(Sink.ignore)

        writeProbe.receiveMessages(persisters.size, (totalNumberOfEvents / writeRps).seconds + 10.seconds)
        println(
          s"#$iteration Persisting all [$totalNumberOfEvents] events from [${persistenceIds.size}] persistent " +
          s"actors took [${(System.nanoTime() - t0) / 1000 / 1000}] ms")

        Await.result(done, 30.seconds)
        println(
          s"#$iteration Reading all [$totalNumberOfEvents] events with eventsBySlices " +
          s"took [${(System.nanoTime() - t0) / 1000 / 1000}] ms")
      }
    }

  }

}
