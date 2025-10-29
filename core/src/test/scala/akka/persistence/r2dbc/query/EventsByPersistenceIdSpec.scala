/*
 * Copyright (C) 2022 - 2025 Lightbend Inc. <https://akka.io>
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
import akka.persistence.query.{ EventEnvelope => ClassicEventEnvelope }
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike

object EventsByPersistenceIdSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType
}

class EventsByPersistenceIdSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventsByPersistenceIdSpec._

  override def typedSystem: ActorSystem[_] = system

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  List[QueryType](Live, Current).foreach { queryType =>
    def doQuery(pid: String, from: Long, to: Long): Source[ClassicEventEnvelope, NotUsed] =
      queryType match {
        case Live =>
          query.eventsByPersistenceId(pid, from, to)
        case Current =>
          query.currentEventsByPersistenceId(pid, from, to)
      }

    def assertFinished(probe: TestSubscriber.Probe[_], liveShouldFinish: Boolean = false): Unit =
      queryType match {
        case Live if !liveShouldFinish =>
          probe.expectNoMessage()
          probe.cancel()
        case _ =>
          probe.expectComplete()
      }

    s"$queryType eventsByPersistenceId" should {
      "populate timestamp offset" in {
        val pid = nextPid()
        val persister = testKit.spawn(Persister(pid))
        val probe = testKit.createTestProbe[Done]()
        persister ! Persister.PersistWithAck("e-1", probe.ref)
        probe.expectMessage(Done)

        val sub = doQuery(pid, 0, Long.MaxValue)
          .runWith(TestSink())
          .request(1)

        val env = sub.expectNext()
        toTimestampOffset(env.offset).seen shouldBe Map(pid -> 1)
        toTimestampOffset(env.offset).timestamp should not be Instant.EPOCH

        assertFinished(sub)
      }

      "return all events" in {
        val pid = nextPid()
        val persister = testKit.spawn(Persister(pid))
        val probe = testKit.createTestProbe[Done]()
        val events = (1 to 20).map { i =>
          val payload = s"e-$i"
          persister ! PersistWithAck(payload, probe.ref)
          probe.expectMessage(Done)
          payload
        }

        val sub = doQuery(pid, 0, Long.MaxValue)
          .map(_.event)
          .runWith(TestSink())

        sub
          .request(events.size + 1)
          .expectNextN(events.size)

        assertFinished(sub)
      }

      "only return sequence nrs requested" in {
        val pid = nextPid()
        val persister = testKit.spawn(Persister(pid))
        val probe = testKit.createTestProbe[Done]()
        val events = (1 to 20).map { i =>
          val payload = s"e-$i"
          persister ! PersistWithAck(payload, probe.ref)
          probe.expectMessage(Done)
          payload
        }

        val sub = doQuery(pid, 0, 5)
          .map(_.event)
          .runWith(TestSink())

        sub
          .request(events.size + 1)
          .expectNextN(events.take(5))

        assertFinished(sub, liveShouldFinish = true)
      }

      "allow querying for a single event" in {
        val pid = nextPid()
        val persister = testKit.spawn(Persister(pid))
        val probe = testKit.createTestProbe[Done]()

        (1 to 3).map { i =>
          val payload = s"e-$i"
          persister ! PersistWithAck(payload, probe.ref)
          probe.expectMessage(Done)
          payload
        }

        val sub = doQuery(pid, 2, 2)
          .map(_.event)
          .runWith(TestSink())

        val event = sub
          .request(2)
          .expectNext()
        event should ===("e-2")

        assertFinished(sub, liveShouldFinish = true)
      }

      "include metadata" in {
        val probe = testKit.createTestProbe[Done]()
        val entityType = nextEntityType()
        val entityId = "entity-1"
        val pid = TestActors.replicatedEventSourcedPersistenceId(entityType, entityId).id

        val persister = testKit.spawn(TestActors.replicatedEventSourcedPersister(entityType, entityId))
        persister ! Persister.PersistWithAck("e-1", probe.ref)
        probe.expectMessage(Done)
        persister ! Persister.PersistWithAck("e-2", probe.ref)
        probe.expectMessage(Done)

        val sub = doQuery(pid, 0, Long.MaxValue)
          .runWith(TestSink())
          .request(10)

        val env1 = sub.expectNext()
        env1.event shouldBe "e-1"
        val meta1 = env1.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
        meta1.originReplica.id shouldBe "dc-1"
        meta1.originSequenceNr shouldBe 1L

        val env2 = sub.expectNext()
        env2.event shouldBe "e-2"
        val meta2 = env2.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
        meta2.originReplica.id shouldBe "dc-1"
        meta2.originSequenceNr shouldBe 2L

        assertFinished(sub)
      }
    }
  }

  "Typed versions of query" should {
    "include tags" in {
      val probe = testKit.createTestProbe[Done]()
      val entityType = nextEntityType()
      val entityId = "entity-1"
      val pid = PersistenceId(entityType, entityId)

      val persister = testKit.spawn(TestActors.Persister(pid, tags = Set("tag")))
      persister ! Persister.PersistWithAck("e-1", probe.ref)
      probe.expectMessage(Done)

      val events = query
        .currentEventsByPersistenceIdTyped[String](pid.id, 0L, Long.MaxValue)
        .runWith(Sink.seq)
        .futureValue

      events should have size 1
      events.head.tags should ===(Set("tag"))

      val event = query
        .eventsByPersistenceIdTyped[String](pid.id, 0L, Long.MaxValue)
        .runWith(Sink.head)
        .futureValue
      event.tags should ===(Set("tag"))
    }
  }

  "Live query" should {
    "pick up new events" in {
      val pid = nextPid()
      val persister = testKit.spawn(Persister(pid))
      val probe = testKit.createTestProbe[Done]()
      val sub = query
        .eventsByPersistenceId(pid, 0, Long.MaxValue)
        .map(_.event)
        .runWith(TestSink())
      val events = (1 to 20).map { i =>
        val payload = s"e-$i"
        persister ! PersistWithAck(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }

      sub.request(21)
      sub.expectNextN(events)

      val events2 = (21 to 40).map { i =>
        val payload = s"e-$i"
        // make the live query can deliver an element it picks up so it can end its query and give up the sesion
        sub.request(1)
        persister ! PersistWithAck(payload, probe.ref)
        probe.expectMessage(Done)
        payload
      }
      sub.request(1)
      sub.expectNextN(events2)

      sub.expectNoMessage()
      sub.cancel()
    }
  }
}
