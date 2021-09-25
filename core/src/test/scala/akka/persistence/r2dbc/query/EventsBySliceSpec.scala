/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query

import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.query.EventEnvelope
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.TestActors
import akka.persistence.r2dbc.TestActors.Persister.Persist
import akka.persistence.r2dbc.TestActors.Persister.PersistWithAck
import akka.persistence.r2dbc.TestConfig
import akka.persistence.r2dbc.TestData
import akka.persistence.r2dbc.TestDbLifecycle
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.wordspec.AnyWordSpecLike

object EventsBySliceSpec {
  sealed trait QueryType
  case object Live extends QueryType
  case object Current extends QueryType
}

class EventsBySliceSpec
    extends ScalaTestWithActorTestKit(TestConfig.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventsBySliceSpec._

  override def typedSystem: ActorSystem[_] = system
  private val settings = new R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  import settings.maxNumberOfSlices

  private val query = PersistenceQuery(testKit.system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private class Setup {
    val entityTypeHint = nextEntityTypeHint
    val persistenceId = nextPid(entityTypeHint)
    val slice = query.sliceForPersistenceId(persistenceId)
    val persister = spawn(TestActors.Persister(persistenceId))
    val probe = createTestProbe[Done]
    val sinkProbe = TestSink.probe[EventEnvelope](system.classicSystem)
  }

  List[QueryType](Current, Live).foreach { queryType =>
    def doQuery(entityTypeHint: String, minSlice: Int, maxSlice: Int, offset: Offset): Source[EventEnvelope, NotUsed] =
      queryType match {
        case Live =>
          query.eventsBySlices(entityTypeHint, minSlice, maxSlice, offset)
        case Current =>
          query.currentEventsBySlices(entityTypeHint, minSlice, maxSlice, offset)
      }

    def assertFinished(probe: TestSubscriber.Probe[EventEnvelope]): Unit =
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
          probe.expectMessage(10.seconds, Done)
        }
        val result: TestSubscriber.Probe[EventEnvelope] =
          doQuery(entityTypeHint, slice, slice, NoOffset)
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

        val result: TestSubscriber.Probe[EventEnvelope] =
          doQuery(entityTypeHint, slice, slice, NoOffset)
            .runWith(sinkProbe)
            .request(21)

        result.expectNextN(9)

        val offset = result.expectNext().offset
        result.cancel()

        val withOffset =
          doQuery(entityTypeHint, slice, slice, offset)
            .runWith(TestSink.probe[EventEnvelope](system.classicSystem))
        withOffset.request(12)
        for (i <- 11 to 20) {
          withOffset.expectNext().event shouldBe s"e-$i"
        }
        assertFinished(withOffset)
      }

    }
  }

  // tests just relevant for current query
  "Current eventsBySlices" should {
    "filter events with the same timestamp based on seen sequence nrs" in new Setup {
      persister ! PersistWithAck(s"e-1", probe.ref)
      probe.expectMessage(Done)
      val singleEvent: EventEnvelope =
        query.currentEventsBySlices(entityTypeHint, slice, slice, NoOffset).runWith(Sink.head).futureValue
      val offset = singleEvent.offset.asInstanceOf[TimestampOffset]
      offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)
      query
        .currentEventsBySlices(entityTypeHint, slice, slice, offset)
        .take(1)
        .runWith(Sink.headOption)
        .futureValue shouldEqual None
    }

    "not filter events with the same timestamp based on sequence nrs" in new Setup {
      persister ! PersistWithAck(s"e-1", probe.ref)
      probe.expectMessage(Done)
      val singleEvent: EventEnvelope =
        query.currentEventsBySlices(entityTypeHint, slice, slice, NoOffset).runWith(Sink.head).futureValue
      val offset = singleEvent.offset.asInstanceOf[TimestampOffset]
      offset.seen shouldEqual Map(singleEvent.persistenceId -> singleEvent.sequenceNr)

      val offsetWithoutSeen = TimestampOffset(offset.timestamp, Map.empty)
      query
        .currentEventsBySlices(entityTypeHint, slice, slice, offsetWithoutSeen)
        .runWith(Sink.headOption)
        .futureValue shouldEqual Some(singleEvent)
    }

    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 40
      val numberOfEvents = 3
      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPid(entityTypeHint)).toVector
      val persisters = persistenceIds.map { pid =>
        val ref = testKit.spawn(TestActors.Persister(pid))
        for (i <- 1 to numberOfEvents) {
          ref ! PersistWithAck(s"e-$i", probe.ref)
          probe.expectMessage(Done)
        }
      }

      maxNumberOfSlices should be(128)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 31)
      ranges(1) should be(32 to 63)
      ranges(2) should be(64 to 95)
      ranges(3) should be(96 to 127)

      val allEnvelopes =
        (0 until 4).flatMap { rangeIndex =>
          val result =
            query
              .currentEventsBySlices(entityTypeHint, ranges(rangeIndex).min, ranges(rangeIndex).max, NoOffset)
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
      val result: TestSubscriber.Probe[EventEnvelope] =
        query.eventsBySlices(entityTypeHint, slice, slice, NoOffset).runWith(sinkProbe).request(21)
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
    }

    "retrieve from several slices" in new Setup {
      val numberOfPersisters = 40
      val numberOfEvents = 3

      maxNumberOfSlices should be(128)
      val ranges = query.sliceRanges(4)
      ranges(0) should be(0 to 31)
      ranges(1) should be(32 to 63)
      ranges(2) should be(64 to 95)
      ranges(3) should be(96 to 127)

      val queries: Seq[Source[EventEnvelope, NotUsed]] =
        (0 until 4).map { rangeIndex =>
          query
            .eventsBySlices(entityTypeHint, ranges(rangeIndex).min, ranges(rangeIndex).max, NoOffset)
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
          .runWith(Sink.seq[EventEnvelope])

      val persistenceIds = (1 to numberOfPersisters).map(_ => nextPid(entityTypeHint)).toVector
      val persisters = persistenceIds.map { pid =>
        val ref = testKit.spawn(TestActors.Persister(pid))
        for (i <- 1 to numberOfEvents) {
          ref ! Persist(s"e-$i")
        }
      }

      allEnvelopes.futureValue.size should be(numberOfPersisters * numberOfEvents)
    }
  }

}
