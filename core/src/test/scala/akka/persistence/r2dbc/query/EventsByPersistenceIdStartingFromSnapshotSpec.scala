/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import java.time.Instant

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsByPersistenceIdStartingFromSnapshotSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc.query.start-from-snapshot.enabled = true
    """)
    .withFallback(TestConfig.config)
}

class EventsByPersistenceIdStartingFromSnapshotSpec
    extends ScalaTestWithActorTestKit(EventsByPersistenceIdStartingFromSnapshotSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventsByPersistenceIdStartingFromSnapshotSpec._

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private class Setup {
    val entityType = nextEntityType()
    val persistenceId = PersistenceId.ofUniqueId(nextPid(entityType))
    val slice = query.sliceForPersistenceId(persistenceId.id)
    val snapshotAckProbe = createTestProbe[Long]()
    val persister = spawn(TestActors.Persister.withSnapshotAck(persistenceId, Set.empty, snapshotAckProbe.ref))
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink[EventEnvelope[String]]()(system.classicSystem)
  }

  // Snapshots of the Persister is string concatenation of the events
  private def expectedSnapshotEvent(seqNr: Long): String =
    (1L to seqNr).map(i => s"e-$i").mkString("|") + "-snap"

  List[QueryType](Live, Current).foreach { queryType =>
    def doQuery(pid: String, from: Long, to: Long): Source[EventEnvelope[String], NotUsed] =
      queryType match {
        case Live =>
          query.eventsByPersistenceIdStartingFromSnapshot[String, String](pid, from, to, snap => snap)
        case Current =>
          query.currentEventsByPersistenceIdStartingFromSnapshot[String, String](pid, from, to, snap => snap)
      }

    def assertFinished(probe: TestSubscriber.Probe[_], liveShouldFinish: Boolean = false): Unit =
      queryType match {
        case Live if !liveShouldFinish =>
          probe.expectNoMessage()
          probe.cancel()
        case _ =>
          probe.expectComplete()
      }

    s"$queryType eventsByPersistenceIdStartingFromSnapshot" should {
      "populate timestamp offset" in new Setup {
        persister ! Persister.PersistWithAck("e-1-snap", probe.ref)
        snapshotAckProbe.expectMessage(1L)
        probe.expectMessage(Done)
        persister ! Persister.PersistWithAck("e-2", probe.ref)
        probe.expectMessage(Done)

        val result = doQuery(persistenceId.id, 0, Long.MaxValue)
          .runWith(sinkProbe)
          .request(10)

        val env1 = result.expectNext()
        toTimestampOffset(env1.offset).seen shouldBe Map(persistenceId.id -> 1)
        toTimestampOffset(env1.offset).timestamp should not be Instant.EPOCH

        val env2 = result.expectNext()
        toTimestampOffset(env2.offset).seen shouldBe Map(persistenceId.id -> 2)
        toTimestampOffset(env2.offset).timestamp should not be Instant.EPOCH

        assertFinished(result)
      }

      "return snapshot and all events" in new Setup {
        (1 to 20).map { i =>
          if (i == 17) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(17L)
          } else {
            persister ! PersistWithAck(s"e-$i", probe.ref)
          }

          probe.expectMessage(Done)
        }

        val result = doQuery(persistenceId.id, 0, Long.MaxValue)
          .runWith(sinkProbe)
          .request(21)

        result.expectNext().event shouldBe expectedSnapshotEvent(17)
        for (i <- 18 to 20) {
          result.expectNext().event shouldBe s"e-$i"
        }

        assertFinished(result)
      }

      "only return snapshot and events up to (inclusive) toSequenceNr" in new Setup {
        (1 to 10).map { i =>
          if (i == 3) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(3L)
          } else {
            persister ! PersistWithAck(s"e-$i", probe.ref)
          }

          probe.expectMessage(Done)
        }

        val result = doQuery(persistenceId.id, 0, 5L)
          .runWith(sinkProbe)
          .request(21)

        result.expectNext().event shouldBe expectedSnapshotEvent(3)
        for (i <- 4 to 5) {
          result.expectNext().event shouldBe s"e-$i"
        }

        assertFinished(result, liveShouldFinish = true)
      }

      "not return snapshot with seqNr greater than toSequenceNr" in new Setup {
        (1 to 10).map { i =>
          if (i == 6) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(6L)
          } else {
            persister ! PersistWithAck(s"e-$i", probe.ref)
          }

          probe.expectMessage(Done)
        }

        val result = doQuery(persistenceId.id, 0, 5L)
          .runWith(sinkProbe)
          .request(21)

        for (i <- 1 to 5) {
          result.expectNext().event shouldBe s"e-$i"
        }

        assertFinished(result, liveShouldFinish = true)
      }

      "not return snapshot with seqNr less than than fromSequenceNr" in new Setup {
        (1 to 10).map { i =>
          if (i == 2) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(2L)
          } else {
            persister ! PersistWithAck(s"e-$i", probe.ref)
          }

          probe.expectMessage(Done)
        }

        val result = doQuery(persistenceId.id, 3, Long.MaxValue)
          .runWith(sinkProbe)
          .request(21)

        for (i <- 3 to 10) {
          result.expectNext().event shouldBe s"e-$i"
        }

        assertFinished(result)
      }

      "allow querying for a single snapshot" in new Setup {
        (1 to 3).map { i =>
          if (i == 2) {
            persister ! PersistWithAck(s"e-$i-snap", probe.ref)
            snapshotAckProbe.expectMessage(2L)
          } else {
            persister ! PersistWithAck(s"e-$i", probe.ref)
          }

          probe.expectMessage(Done)
        }

        val result = doQuery(persistenceId.id, 2L, 2L)
          .runWith(sinkProbe)
          .request(21)

        result.expectNext().event shouldBe expectedSnapshotEvent(2)

        assertFinished(result, liveShouldFinish = true)
      }

      "includes tags in snapshot" in new Setup {
        val taggingPersister =
          spawn(TestActors.Persister.withSnapshotAck(persistenceId, tags = Set("tag-A"), snapshotAckProbe.ref))

        taggingPersister ! PersistWithAck(s"e-1", probe.ref)
        probe.expectMessage(Done)
        taggingPersister ! PersistWithAck(s"e-2-snap", probe.ref)
        snapshotAckProbe.expectMessage(2L)
        probe.expectMessage(Done)
        taggingPersister ! PersistWithAck(s"e-3", probe.ref)
        probe.expectMessage(Done)

        val result = doQuery(persistenceId.id, 0L, Long.MaxValue)
          .runWith(sinkProbe)
          .request(21)

        val snapshotEnvelope = result.expectNext()
        snapshotEnvelope.event shouldBe expectedSnapshotEvent(2)
        snapshotEnvelope.tags shouldBe Set("tag-A")

        val e3Envelope = result.expectNext()
        e3Envelope.event shouldBe "e-3"
        e3Envelope.tags shouldBe Set("tag-A")
      }
    }
  }

  "Live query" should {
    "pick up new events" in new Setup {
      for (i <- 1 to 20) {
        if (i == 17) {
          persister ! PersistWithAck(s"e-$i-snap", probe.ref)
          snapshotAckProbe.expectMessage(17L)
        } else
          persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }
      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query
          .eventsByPersistenceIdStartingFromSnapshot[String, String](persistenceId.id, 0L, Long.MaxValue, snap => snap)
          .runWith(sinkProbe)
          .request(21)

      result.expectNext().event shouldBe expectedSnapshotEvent(17)
      for (i <- 18 to 20) {
        result.expectNext().event shouldBe s"e-$i"
      }

      for (i <- 21 to 40) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        // make sure the query doesn't get an element in its buffer with nothing to take it
        // resulting in it not finishing the query and giving up the session
        result.request(1)
        probe.expectMessage(Done)
      }

      result.request(1)

      for (i <- 21 to 40) {
        result.expectNext().event shouldBe s"e-$i"
      }

      result.cancel()
    }
  }
}
