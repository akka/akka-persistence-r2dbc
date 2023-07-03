/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestActors.Persister.PersistAll
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestActors.Persister.Ping
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySliceSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType

  def config: Config =
    TestConfig.backtrackingDisabledConfig
      .withFallback(ConfigFactory.parseString(s"""
    # This test is not using backtracking, so increase behind-current-time to
    # reduce risk of missing events
    akka.persistence.r2dbc.query.behind-current-time = 500 millis
    akka.persistence.r2dbc-small-buffer = $${akka.persistence.r2dbc}

    akka.persistence.r2dbc.journal.publish-events = off

    # this is used by the "read in chunks" test
    akka.persistence.r2dbc-small-buffer.query {
      buffer-size = 4
      # for this extreme scenario it will add delay between each query for the live case
      refresh-interval = 20 millis
    }
    """))
      .withFallback(TestConfig.config)
      .resolve()
}

class EventsBySliceSpec
    extends ScalaTestWithActorTestKit(EventsBySliceSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventsBySliceSpec._

  override def typedSystem: ActorSystem[_] = system
  private val settings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private class Setup {
    val entityType = nextEntityType()
    val persistenceId = nextPid(entityType)
    val slice = query.sliceForPersistenceId(persistenceId)
    val persister = spawn(TestActors.Persister(persistenceId))
    val probe = createTestProbe[Done]()
    val sinkProbe = TestSink.probe[EventEnvelope[String]](system.classicSystem)
  }

  List[QueryType](Current, Live).foreach { queryType =>
    def doQuery(
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        offset: Offset,
        queryImpl: R2dbcReadJournal = query): Source[EventEnvelope[String], NotUsed] =
      queryType match {
        case Live =>
          queryImpl.eventsBySlices[String](entityType, minSlice, maxSlice, offset)
        case Current =>
          queryImpl.currentEventsBySlices[String](entityType, minSlice, maxSlice, offset)
      }

    def assertFinished(probe: TestSubscriber.Probe[EventEnvelope[String]]): Unit =
      queryType match {
        case Live =>
          probe.expectNoMessage()
          probe.cancel()
        case Current =>
          probe.expectComplete()
      }

    s"$queryType eventsBySlices" should {
      "return all events for NoOffset" in new Setup {
        for (i <- 1 to 20) {
          persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)
        for (i <- 1 to 20) {
          result.expectNext().event shouldBe s"e-$i"
        }
        assertFinished(result)
      }

      "only return events after an offset" in new Setup {
        for (i <- 1 to 20) {
          persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }

        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)

        result.expectNextN(9)

        val offset = result.expectNext().offset
        result.cancel()

        val withOffset =
          doQuery(entityType, slice, slice, offset)
            .runWith(TestSink.probe[EventEnvelope[String]](system.classicSystem))
        withOffset.request(12)
        for (i <- 11 to 20) {
          withOffset.expectNext().event shouldBe s"e-$i"
        }
        assertFinished(withOffset)
      }

      "read in chunks" in new Setup {
        val queryWithSmallBuffer = PersistenceQuery(testKit.system)
          .readJournalFor[R2dbcReadJournal]("akka.persistence.r2dbc-small-buffer.query")
        for (i <- 1 to 10; n <- 1 to 10 by 2) {
          persister ! PersistAll(List(s"e-$i-$n", s"e-$i-${n + 1}"))
        }
        persister ! Ping(probe.ref)
        probe.expectMessage(Done)
        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset, queryWithSmallBuffer)
            .runWith(sinkProbe)
            .request(101)
        for (i <- 1 to 10; n <- 1 to 10) {
          result.expectNext().event shouldBe s"e-$i-$n"
        }
        assertFinished(result)
      }

      "include metadata" in {
        val probe = testKit.createTestProbe[Done]()
        val entityType = nextEntityType()
        val entityId = "entity-1"
        val persistenceId = PersistenceId(entityType, entityId)
        val slice = query.sliceForPersistenceId(persistenceId.id)

        val persister = testKit.spawn(TestActors.replicatedEventSourcedPersister(entityType, entityId))
        persister ! Persister.PersistWithAck("e-1", probe.ref)
        probe.expectMessage(Done)
        persister ! Persister.PersistWithAck("e-2", probe.ref)
        probe.expectMessage(Done)

        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(TestSink())
            .request(21)

        val env1 = result.expectNext()
        env1.event shouldBe "e-1"
        val meta1 = env1.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
        meta1.originReplica.id shouldBe "dc-1"
        meta1.originSequenceNr shouldBe 1L

        val env2 = result.expectNext()
        env2.event shouldBe "e-2"
        val meta2 = env2.eventMetadata.get.asInstanceOf[ReplicatedEventMetadata]
        meta2.originReplica.id shouldBe "dc-1"
        meta2.originSequenceNr shouldBe 2L

        assertFinished(result)
      }

      "support EventTimestampQuery" in new Setup {
        for (i <- 1 to 3) {
          persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }

        query.isInstanceOf[EventTimestampQuery] shouldBe true
        query.timestampOf(persistenceId, 2L).futureValue.isDefined shouldBe true
        query.timestampOf(persistenceId, 1L).futureValue.isDefined shouldBe true
        query.timestampOf(persistenceId, 4L).futureValue.isDefined shouldBe false
      }

      "support LoadEventQuery" in new Setup {
        for (i <- 1 to 3) {
          persister ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }

        query.isInstanceOf[LoadEventQuery] shouldBe true
        query.loadEnvelope[String](persistenceId, 2L).futureValue.event shouldBe "e-2"
        query.loadEnvelope[String](persistenceId, 1L).futureValue.event shouldBe "e-1"
        intercept[NoSuchElementException] {
          Await.result(query.loadEnvelope[String](persistenceId, 4L), patience.timeout)
        }
      }

      "includes tags" in new Setup {
        val taggingPersister: ActorRef[Persister.Command] =
          spawn(TestActors.Persister(PersistenceId.ofUniqueId(persistenceId), tags = Set("tag-A")))
        for (i <- 1 to 3) {
          taggingPersister ! PersistWithAck(s"f-$i", probe.ref)
          probe.expectMessage(Done)
        }

        val result: TestSubscriber.Probe[EventEnvelope[String]] =
          doQuery(entityType, slice, slice, NoOffset)
            .runWith(TestSink())

        result.request(3)
        val envelopes = result.expectNextN(3)
        envelopes.map(_.tags) should ===(Seq(Set("tag-A"), Set("tag-A"), Set("tag-A")))

        query.loadEnvelope[String](persistenceId, 1L).futureValue.tags shouldBe Set("tag-A")
      }

    }
  }

  // tests just relevant for current query
  "Current eventsBySlices" should {
    "filter events with the same timestamp based on seen sequence nrs" in new Setup {
      persister ! PersistWithAck(s"e-1", probe.ref)
      probe.expectMessage(Done)
      val singleEvent: EventEnvelope[String] =
        query.currentEventsBySlices[String](entityType, slice, slice, NoOffset).runWith(Sink.head).futureValue
      val offset = singleEvent.offset.asInstanceOf[TimestampOffset]
      offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)
      query
        .currentEventsBySlices[String](entityType, slice, slice, offset)
        .take(1)
        .runWith(Sink.headOption)
        .futureValue shouldEqual None
    }

    "not filter events with the same timestamp based on sequence nrs" in new Setup {
      persister ! PersistWithAck(s"e-1", probe.ref)
      probe.expectMessage(Done)
      val singleEvent: EventEnvelope[String] =
        query.currentEventsBySlices[String](entityType, slice, slice, NoOffset).runWith(Sink.head).futureValue
      val offset = singleEvent.offset.asInstanceOf[TimestampOffset]
      offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)

      val offsetWithoutSeen = TimestampOffset(offset.timestamp, Map.empty)
      val singleEvent2 = query
        .currentEventsBySlices[String](entityType, slice, slice, offsetWithoutSeen)
        .runWith(Sink.headOption)
        .futureValue
      singleEvent2.get.event shouldBe "e-1"
    }

    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 20
      val numberOfEvents = 3
      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPid(entityType)).toVector
      val persisters = persistenceIds.map { pid =>
        val ref = testKit.spawn(TestActors.Persister(pid))
        for (i <- 1 to numberOfEvents) {
          ref ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
      }

      persistenceExt.numberOfSlices should be(1024)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 255)
      ranges(1) should be(256 to 511)
      ranges(2) should be(512 to 767)
      ranges(3) should be(768 to 1023)

      val allEnvelopes =
        (0 until 4).flatMap { rangeIndex =>
          val result =
            query
              .currentEventsBySlices[String](entityType, ranges(rangeIndex).min, ranges(rangeIndex).max, NoOffset)
              .runWith(Sink.seq)
              .futureValue
          result.foreach { env =>
            ranges(rangeIndex) should contain(query.sliceForPersistenceId(env.persistenceId))
          }
          result
        }
      allEnvelopes.size should be(numberOfPersisters * numberOfEvents)
    }
  }

  // tests just relevant for live query
  "Live eventsBySlices" should {
    "find new events" in new Setup {
      for (i <- 1 to 20) {
        persister ! PersistWithAck(s"e-$i", probe.ref)
        probe.expectMessage(Done)
      }
      val result: TestSubscriber.Probe[EventEnvelope[String]] =
        query.eventsBySlices[String](entityType, slice, slice, NoOffset).runWith(sinkProbe).request(21)
      for (i <- 1 to 20) {
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

    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 20
      val numberOfEvents = 3

      persistenceExt.numberOfSlices should be(1024)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 255)
      ranges(1) should be(256 to 511)
      ranges(2) should be(512 to 767)
      ranges(3) should be(768 to 1023)

      val queries: Seq[Source[EventEnvelope[String], NotUsed]] =
        (0 until 4).map { rangeIndex =>
          query
            .eventsBySlices[String](entityType, ranges(rangeIndex).min, ranges(rangeIndex).max, NoOffset)
            .map { env =>
              ranges(rangeIndex) should contain(query.sliceForPersistenceId(env.persistenceId))
              env
            }
        }
      val allEnvelopes =
        queries(0)
          .merge(queries(1))
          .merge(queries(2))
          .merge(queries(3))
          .take(numberOfPersisters * numberOfEvents)
          .runWith(Sink.seq[EventEnvelope[String]])

      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPid(entityType)).toVector
      val persisters = persistenceIds.map { pid =>
        val ref = testKit.spawn(TestActors.Persister(pid))
        for (i <- 1 to numberOfEvents) {
          ref ! Persist(s"e-$i")
        }
        ref
      }

      persisters.foreach { ref =>
        ref ! Ping(probe.ref)
        probe.expectMessage(10.seconds, Done)
      }

      allEnvelopes.futureValue.size should be(numberOfPersisters * numberOfEvents)
    }
  }

}
